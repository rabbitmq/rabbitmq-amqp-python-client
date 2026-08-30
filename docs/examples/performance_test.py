"""Performance test: throughput and round-trip latency of one publisher/consumer pair (step_050).

Declares one queue of a caller-chosen type, publishes a caller-chosen number of
messages to it as fast as the link allows, consumes them back on the same
connection, and reports the two headline numbers a caller tuning broker/client
performance cares about — **throughput** (messages sent/sec and consumed/sec,
tracked separately) and **latency** (publish-to-consume round-trip time) —
printed periodically while the run is in progress and once more as a final
summary::

    python3 docs/examples/performance_test.py --messages 2000 --latency-window-size 200
    python3 docs/examples/performance_test.py --messages 100000 --queue-type quorum
    python3 docs/examples/performance_test.py --help

This program adds nothing to the client's public surface: it is built entirely
out of :class:`~src.Connection`,
:class:`~src.Management`,
:class:`~src.Publisher` and
:class:`~src.Consumer`. It owns exactly one connection,
one publisher and one consumer: it measures what one such pair can sustain, not
a cluster-wide ceiling (§10).

Unlike ``console_application.py``, whose job is correctness, this program's only
job is producing trustworthy numbers, so it deliberately keeps its failure mode
simple: **no auto-reconnection is wired in** (§4.1, §10). No
``on_unexpected_close`` callback is registered and ``RecoveryConfiguration`` is
switched off, so a mid-run disconnect is not recovered from — it surfaces only
indirectly, as ``--consume-timeout`` elapsing with ``messages_consumed`` short of
``messages_sent``.

Exit codes:
    0: every message was confirmed and consumption caught up.
    1: at least one message was not confirmed (rejected, released or failed).
    2: the options were rejected before anything was connected.
    3: connecting, declaring the queue, or attaching a link failed.
    4: the drain wait timed out before consumption caught up.
    130: interrupted with Ctrl-C.
"""

from __future__ import annotations

import argparse
import logging
import ssl
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

# Make the client importable when this example is run straight from a checkout,
# with neither the package installed nor PYTHONPATH set.
_SOURCE_ROOT = Path(__file__).resolve().parents[2]
if _SOURCE_ROOT.is_dir() and str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from src import (  # noqa: E402 - after the sys.path bootstrap above
    AMQPError,
    ApplicationProperties,
    Connection,
    ConnectionParameters,
    Consumer,
    Context,
    Long,
    Message,
    Outcome,
    OutcomeState,
    Publisher,
    QueueSpecification,
    RecoveryConfiguration,
)

# --- exit codes (§8) ----------------------------------------------------

EXIT_OK = 0
EXIT_MESSAGES_NOT_CONFIRMED = 1
EXIT_INVALID_OPTIONS = 2
EXIT_SETUP_FAILED = 3
EXIT_MESSAGES_NOT_CONSUMED = 4
EXIT_INTERRUPTED = 130

# --- option defaults (§2) -----------------------------------------------

DEFAULT_MESSAGE_COUNT = 1_000_000
DEFAULT_MESSAGE_SIZE_BYTES = 16

#: Overridden from ``ConsumerBuilder``'s own default of 100, which is sized for
#: correctness examples rather than for keeping the broker able to sustain this
#: program's throughput without link-credit stalls (§2).
DEFAULT_INITIAL_CREDITS = 1000

DEFAULT_CONSUME_TIMEOUT_SECONDS = 30.0
DEFAULT_PUBLISH_TIMEOUT_SECONDS = 5.0
DEFAULT_STATS_INTERVAL_SECONDS = 1.0
DEFAULT_LATENCY_WINDOW_SIZE = 10_000

QUEUE_TYPE_CLASSIC = "classic"
QUEUE_TYPE_QUORUM = "quorum"
QUEUE_TYPE_STREAM = "stream"

#: The three values ``--queue-type`` accepts, in the order ``--help`` lists them.
QUEUE_TYPES = (QUEUE_TYPE_CLASSIC, QUEUE_TYPE_QUORUM, QUEUE_TYPE_STREAM)

#: Prefix of the queue name generated when ``--queue`` is not given.
GENERATED_QUEUE_PREFIX = "perf-test-"

# --- message payload (§3) -----------------------------------------------

#: ``ApplicationProperties`` key carrying the monotonic send time, in nanoseconds.
SEND_TIMESTAMP_PROPERTY = "x-send-timestamp"

#: Byte the filler body is padded with; the pattern itself is not contractual.
FILLER_BYTE = b"x"

NANOSECONDS_PER_MILLISECOND = 1_000_000

# --- timing -------------------------------------------------------------

#: How long the drain wait blocks per poll before re-reading the counters.
DRAIN_POLL_INTERVAL_SECONDS = 0.05

#: How long :meth:`StatsPrinter.stop` waits for the printer thread to end.
STATS_JOIN_TIMEOUT_SECONDS = 5.0

# --- output (§7) --------------------------------------------------------

#: Column the summary's values start at, so every label is padded to it.
LABEL_WIDTH = 31

logger = logging.getLogger("performance-test")


class OptionsError(ValueError):
    """An option, or a combination of them, was rejected before connecting.

    Raised by :func:`parse_args` for everything argparse itself cannot check —
    notably ``--messages 0`` and ``--latency-window-size 0`` (§8 rows 1-2).
    :func:`main` catches it, logs it at error level and exits
    :data:`EXIT_INVALID_OPTIONS` without ever opening a connection, so nothing
    about the broker can influence that path.
    """


@dataclass(frozen=True)
class Options:
    """Everything this run was configured with (§2).

    Attributes:
        messages: How many messages to publish; never ``0``.
        queue_type: One of :data:`QUEUE_TYPES`.
        queue: Name of the queue to declare, publish to and consume from.
        keep_queue: Whether teardown leaves the queue in place.
        message_size: Bytes of filler body every message carries; ``0`` is valid.
        initial_credits: ``ConsumerBuilder.initial_credits`` for this run.
        consume_timeout: Seconds the drain wait blocks for at most.
        publish_timeout: Per-call timeout passed to every ``publish()``.
        stats_interval: Seconds between periodic stats blocks.
        latency_window_size: Messages the rolling latency window resets every.
        host: Broker host.
        port: Broker port, or ``None`` to let ``ConnectionParameters`` default it
            from ``tls``.
        user: SASL PLAIN username.
        password: SASL PLAIN password.
        virtual_host: RabbitMQ virtual host.
        tls: Whether to wrap the socket in TLS with a default SSL context.
    """

    messages: int = DEFAULT_MESSAGE_COUNT
    queue_type: str = QUEUE_TYPE_CLASSIC
    queue: str = ""
    keep_queue: bool = False
    message_size: int = DEFAULT_MESSAGE_SIZE_BYTES
    initial_credits: int = DEFAULT_INITIAL_CREDITS
    consume_timeout: float = DEFAULT_CONSUME_TIMEOUT_SECONDS
    publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS
    stats_interval: float = DEFAULT_STATS_INTERVAL_SECONDS
    latency_window_size: int = DEFAULT_LATENCY_WINDOW_SIZE
    host: str = "localhost"
    port: int | None = None
    user: str = "guest"
    password: str = "guest"  # noqa: S105 - the documented local-broker default
    virtual_host: str = "/"
    tls: bool = False


def generate_queue_name() -> str:
    """Return the queue name used when ``--queue`` is not given."""
    return f"{GENERATED_QUEUE_PREFIX}{uuid.uuid4().hex[:12]}"


def build_parser() -> argparse.ArgumentParser:
    """Return the parser defining every flag from §2.

    Returns:
        A parser whose defaults match :class:`Options`', except for ``--queue``,
        which is generated per run by :func:`parse_args`.
    """
    parser = argparse.ArgumentParser(
        prog="performance_test.py",
        description="Measure the throughput and round-trip latency of one publisher/consumer pair.",
    )
    parser.add_argument(
        "-n",
        "--messages",
        type=int,
        default=DEFAULT_MESSAGE_COUNT,
        help="how many messages to publish (default: %(default)s); 0 is rejected",
    )
    parser.add_argument(
        "--queue-type",
        choices=QUEUE_TYPES,
        default=QUEUE_TYPE_CLASSIC,
        help="which queue sub-builder declares the queue (default: %(default)s)",
    )
    parser.add_argument("--queue", default="", help="queue name (default: a generated perf-test-* name)")
    parser.add_argument("--keep-queue", action="store_true", help="do not delete the queue during teardown")
    parser.add_argument(
        "--message-size",
        type=int,
        default=DEFAULT_MESSAGE_SIZE_BYTES,
        help="bytes of filler body per message (default: %(default)s); 0 is valid",
    )
    parser.add_argument(
        "--initial-credits",
        type=int,
        default=DEFAULT_INITIAL_CREDITS,
        help="link credit the consumer keeps outstanding (default: %(default)s)",
    )
    parser.add_argument(
        "--consume-timeout",
        type=float,
        default=DEFAULT_CONSUME_TIMEOUT_SECONDS,
        help="seconds to wait for consumption to catch up (default: %(default)s)",
    )
    parser.add_argument(
        "--publish-timeout",
        type=float,
        default=DEFAULT_PUBLISH_TIMEOUT_SECONDS,
        help="per-call publish timeout, in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--stats-interval",
        type=float,
        default=DEFAULT_STATS_INTERVAL_SECONDS,
        help="seconds between periodic stats blocks (default: %(default)s)",
    )
    parser.add_argument(
        "--latency-window-size",
        type=int,
        default=DEFAULT_LATENCY_WINDOW_SIZE,
        help="messages the rolling latency window resets every (default: %(default)s); 0 is rejected",
    )
    parser.add_argument("--host", default="localhost", help="broker host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=None, help="broker port (default: 5672, or 5671 with --tls)")
    parser.add_argument("--user", default="guest", help="SASL PLAIN username (default: %(default)s)")
    parser.add_argument("--password", default="guest", help="SASL PLAIN password (default: %(default)s)")
    parser.add_argument("--vhost", default="/", help="virtual host (default: %(default)s)")
    parser.add_argument("--tls", action="store_true", help="wrap the connection in TLS")
    return parser


def parse_args(argv: list[str] | None = None) -> Options:
    """Parse ``argv`` into :class:`Options`, rejecting what §8 rejects.

    Args:
        argv: Argument list without the program name; ``sys.argv[1:]`` when
            ``None``.

    Returns:
        The options this run uses, with ``--queue`` filled in with a generated
        name when it was not given.

    Raises:
        OptionsError: If ``--messages`` or ``--latency-window-size`` is not > 0,
            or any size/credit/timing value is invalid. Raised *before* anything
            connects, which is what §8's first two rows require.
        SystemExit: Propagated from argparse for an unknown flag or an
            unparseable value.
    """
    namespace = build_parser().parse_args(argv)
    if namespace.messages <= 0:
        raise OptionsError(f"--messages must be > 0, got {namespace.messages}: there would be nothing to measure")
    if namespace.latency_window_size <= 0:
        raise OptionsError(
            f"--latency-window-size must be > 0, got {namespace.latency_window_size}: "
            "a zero-size window can never complete"
        )
    if namespace.message_size < 0:
        raise OptionsError(f"--message-size must be >= 0, got {namespace.message_size}")
    if namespace.initial_credits <= 0:
        raise OptionsError(f"--initial-credits must be > 0, got {namespace.initial_credits}")
    if namespace.consume_timeout < 0:
        raise OptionsError(f"--consume-timeout must be >= 0, got {namespace.consume_timeout}")
    if namespace.publish_timeout <= 0:
        raise OptionsError(f"--publish-timeout must be > 0, got {namespace.publish_timeout}")
    if namespace.stats_interval <= 0:
        raise OptionsError(f"--stats-interval must be > 0, got {namespace.stats_interval}")
    return Options(
        messages=namespace.messages,
        queue_type=namespace.queue_type,
        queue=namespace.queue or generate_queue_name(),
        keep_queue=namespace.keep_queue,
        message_size=namespace.message_size,
        initial_credits=namespace.initial_credits,
        consume_timeout=namespace.consume_timeout,
        publish_timeout=namespace.publish_timeout,
        stats_interval=namespace.stats_interval,
        latency_window_size=namespace.latency_window_size,
        host=namespace.host,
        port=namespace.port,
        user=namespace.user,
        password=namespace.password,
        virtual_host=namespace.vhost,
        tls=namespace.tls,
    )


# --- counters (§5) ------------------------------------------------------


@dataclass(frozen=True)
class CounterSnapshot:
    """One consistent read of every counter (§5).

    Taken under :class:`Counters`' lock, so the numbers always add up even though
    the publish loop and the delivery loop keep incrementing concurrently. Unlike
    ``console_application.py``, this program folds ``rejected``, ``released`` and
    a raised ``publish()`` into one bucket: its purpose is a throughput/latency
    figure, not a per-outcome diagnostic (§4.5 point 3).

    Attributes:
        messages_sent: Publish attempts made.
        messages_confirmed: Attempts the broker settled ``accepted``.
        messages_not_confirmed: Attempts that ended as anything else.
        messages_consumed: Deliveries handed to the message handler.
    """

    messages_sent: int = 0
    messages_confirmed: int = 0
    messages_not_confirmed: int = 0
    messages_consumed: int = 0


class Counters:
    """The run's counters, shared by every thread that touches them (§5).

    All process-local and reset to zero here: this is a one-shot run, not a
    service with persisted metrics (§1 point 3). Every increment and every read
    goes through one lock, so :meth:`snapshot` never observes a half-updated set.

    Example:
        >>> counters = Counters()
        >>> counters.record_sent()
        >>> counters.record_confirmed()
        >>> counters.snapshot().messages_confirmed
        1
    """

    def __init__(self) -> None:
        """Create a set of counters, all at zero."""
        self._lock = threading.Lock()
        self._snapshot = CounterSnapshot()

    def snapshot(self) -> CounterSnapshot:
        """Return a consistent read of every counter."""
        with self._lock:
            return self._snapshot

    def record_sent(self) -> None:
        """Count one publish attempt, immediately before it is issued (§4.5 point 1)."""
        self._bump(messages_sent=1)

    def record_confirmed(self) -> None:
        """Count one ``accepted`` outcome (§4.5 point 3)."""
        self._bump(messages_confirmed=1)

    def record_not_confirmed(self) -> None:
        """Count one ``rejected``/``released`` outcome, or one raised ``publish()`` (§4.5 points 3-4)."""
        self._bump(messages_not_confirmed=1)

    def record_consumed(self) -> None:
        """Count one delivery reaching the message handler (§4.3 point 3)."""
        self._bump(messages_consumed=1)

    def _bump(self, **deltas: int) -> None:
        """Add ``deltas`` to the current snapshot, replacing it under the lock."""
        with self._lock:
            current = self._snapshot
            self._snapshot = CounterSnapshot(
                messages_sent=current.messages_sent + deltas.get("messages_sent", 0),
                messages_confirmed=current.messages_confirmed + deltas.get("messages_confirmed", 0),
                messages_not_confirmed=current.messages_not_confirmed + deltas.get("messages_not_confirmed", 0),
                messages_consumed=current.messages_consumed + deltas.get("messages_consumed", 0),
            )


# --- throughput (§5) ----------------------------------------------------


def instantaneous_rate(count_now: int, count_previous: int, elapsed: float) -> float:
    """Return the live, per-second rate over one ``--stats-interval`` (§5.1/§5.2).

    The same function serves both throughput families: only which counter is fed
    to it differs.

    Args:
        count_now: The counter as this tick reads it.
        count_previous: The counter as the previous tick read it.
        elapsed: Seconds between the two readings.

    Returns:
        Messages per second over that interval, or ``0.0`` when no measurable
        time passed — a non-positive elapsed time can only come from two readings
        of the clock too close together to divide by.
    """
    if elapsed <= 0:
        return 0.0
    return (count_now - count_previous) / elapsed


def overall_rate(count: int, elapsed: float) -> float:
    """Return the whole-run, per-second rate for the final summary (§5.1/§5.2).

    Both overall figures reduce to this division; they differ only in the instant
    that ends their elapsed time — the publish loop returning for the sent figure,
    the drain wait returning for the consumed one (§5.2).

    Args:
        count: The counter as the final summary reads it.
        elapsed: Seconds from the run's single ``start_time`` to that figure's own
            end instant.

    Returns:
        Messages per second, or ``0.0`` when no measurable time passed.
    """
    if elapsed <= 0:
        return 0.0
    return count / elapsed


# --- latency (§6) -------------------------------------------------------


@dataclass(frozen=True)
class LatencySnapshot:
    """One consistent read of every latency accumulator (§6).

    Attributes:
        window_sum_ns: Sum of the current rolling window's samples.
        window_count: Samples in the current rolling window.
        last_window_average_ns: Average of the last window that completed, kept
            across resets; ``None`` until one has (§6.1 point 3).
        overall_sum_ns: Sum of every sample this run computed, never reset.
        overall_count: Every sample this run computed, never reset.
        completed_windows: How many times the rolling window has reset.
    """

    window_sum_ns: int = 0
    window_count: int = 0
    last_window_average_ns: float | None = None
    overall_sum_ns: int = 0
    overall_count: int = 0
    completed_windows: int = 0

    @property
    def current_average_ns(self) -> float | None:
        """What the periodic printer reports (§6.1's closing formula).

        The live window when it holds anything, and the last completed window's
        average otherwise — which is what keeps a tick landing right after a reset
        from printing a misleading ``0ms``. ``None`` only before the very first
        sample of a run.
        """
        if self.window_count > 0:
            return self.window_sum_ns / self.window_count
        return self.last_window_average_ns

    @property
    def overall_average_ns(self) -> float | None:
        """What the final summary reports (§6.2); ``None`` when nothing was sampled."""
        if self.overall_count == 0:
            return None
        return self.overall_sum_ns / self.overall_count


class LatencyAccumulator:
    """The rolling and overall latency accumulators, updated together (§6).

    One lock guards all of them, so the printer thread can never observe a state
    where the window's sum has been reset but its count has not (§6.1 point 2).
    A run whose ``--messages`` is below ``window_size`` never completes a window
    at all — the printer just keeps reading the live, never-reset sum and count,
    which §6.1's closing paragraph calls expected rather than a bug.

    Example:
        >>> accumulator = LatencyAccumulator(window_size=2)
        >>> accumulator.record(1_000_000)
        >>> accumulator.current_average_ns()
        1000000.0
        >>> accumulator.record(3_000_000)  # the window completes and resets
        >>> accumulator.current_average_ns()
        2000000.0
    """

    def __init__(self, window_size: int) -> None:
        """Create an accumulator whose window resets every ``window_size`` samples.

        Args:
            window_size: The **X** of §6.1, ``--latency-window-size``.

        Raises:
            ValueError: If ``window_size`` is not > 0 — such a window could never
                reach §6.1 point 2's reset condition. :func:`parse_args` already
                rejects it before this is reached.
        """
        if window_size <= 0:
            raise ValueError(f"the latency window size must be > 0, got {window_size}")
        self._window_size = window_size
        self._lock = threading.Lock()
        self._snapshot = LatencySnapshot()

    @property
    def window_size(self) -> int:
        """Samples this accumulator resets its rolling window every."""
        return self._window_size

    def record(self, latency_ns: int) -> None:
        """Add one sample to both the rolling window and the overall total (§6.1 point 1).

        Completing the window computes and stores its average, then zeroes the
        window's sum and count in the same critical section (§6.1 point 2). The
        overall accumulators are never reset (§6.2).

        Args:
            latency_ns: Round-trip time of one delivery, in nanoseconds.
        """
        with self._lock:
            current = self._snapshot
            window_sum = current.window_sum_ns + latency_ns
            window_count = current.window_count + 1
            last_average = current.last_window_average_ns
            completed = current.completed_windows
            if window_count >= self._window_size:
                last_average = window_sum / window_count
                window_sum = 0
                window_count = 0
                completed += 1
            self._snapshot = LatencySnapshot(
                window_sum_ns=window_sum,
                window_count=window_count,
                last_window_average_ns=last_average,
                overall_sum_ns=current.overall_sum_ns + latency_ns,
                overall_count=current.overall_count + 1,
                completed_windows=completed,
            )

    def snapshot(self) -> LatencySnapshot:
        """Return a consistent read of every accumulator."""
        with self._lock:
            return self._snapshot

    def current_average_ns(self) -> float | None:
        """Return the rolling-window average the periodic printer reports (§6.1)."""
        return self.snapshot().current_average_ns

    def overall_average_ns(self) -> float | None:
        """Return the whole-run average the final summary reports (§6.2)."""
        return self.snapshot().overall_average_ns


# --- message payload (§3) -----------------------------------------------


def build_payload(size: int) -> bytes:
    """Return the filler body every published message carries (§3 point 2).

    Args:
        size: ``--message-size`` in bytes; ``0`` yields an empty body.

    Returns:
        ``size`` bytes of filler. The pattern is an implementation detail — this
        program never inspects a received message's body, only its
        ``x-send-timestamp`` property.
    """
    return FILLER_BYTE * size


def build_message(payload: bytes, send_timestamp_ns: int) -> Message:
    """Build one message to publish, stamped with its send time (§3 point 1).

    Args:
        payload: The filler body from :func:`build_payload`.
        send_timestamp_ns: A monotonic-clock reading in nanoseconds, taken
            immediately before ``publish()`` is called.

    Returns:
        The message, carrying the timestamp as a typed ``ApplicationProperties``
        entry — pinned to a signed 64-bit ``long`` rather than left to width
        inference, so the property's wire type does not depend on how long the
        machine happens to have been up.
    """
    return Message(
        payload,
        application_properties=ApplicationProperties({SEND_TIMESTAMP_PROPERTY: Long(send_timestamp_ns)}),
    )


def read_send_timestamp(message: Message) -> int | None:
    """Read a delivery's ``x-send-timestamp`` back, tolerating its absence (§3 point 4).

    Args:
        message: The received message.

    Returns:
        The timestamp in nanoseconds, or ``None`` when the property is missing or
        does not decode as an integer — the caller then counts the delivery but
        skips latency accounting for it. ``bool`` is refused explicitly: it is an
        ``int`` in Python but never a timestamp.
    """
    properties = message.application_properties
    if properties is None:
        return None
    value = properties.value.get(SEND_TIMESTAMP_PROPERTY)
    if value is None or isinstance(value, bool) or not isinstance(value, int):
        return None
    return int(value)


class WarnOnce:
    """Logs one warning for a whole run, however often it is asked to.

    §3 point 4 wants a missing/undecodable ``x-send-timestamp`` logged "once" —
    which against a queue left behind by ``--keep-queue`` could otherwise mean
    one line per delivery, drowning the very stats this program exists to print.

    Example:
        >>> warning = WarnOnce("no timestamp on delivery %s")
        >>> warning.warn(1)
        True
        >>> warning.warn(2)
        False
    """

    def __init__(self, template: str) -> None:
        """Create a one-shot warning.

        Args:
            template: ``logging``-style format string for the single line.
        """
        self._template = template
        self._lock = threading.Lock()
        self._logged = False

    @property
    def has_logged(self) -> bool:
        """Whether the one line has already been logged."""
        with self._lock:
            return self._logged

    def warn(self, *args: object) -> bool:
        """Log the line if it has not been logged yet.

        Args:
            *args: Arguments for the template.

        Returns:
            Whether this call was the one that logged.
        """
        with self._lock:
            if self._logged:
                return False
            self._logged = True
        logger.warning(self._template, *args)
        return True


# --- output (§7) --------------------------------------------------------


def format_count(value: int) -> str:
    """Render one counter, with thousands separators."""
    return f"{value:,}"


def format_rate(messages_per_second: float) -> str:
    """Render one throughput figure, with thousands separators (§5)."""
    return f"{messages_per_second:,.1f}"


def format_latency_ms(average_ns: float | None) -> str:
    """Render one latency average in milliseconds, or ``n/a`` (§6.2).

    Args:
        average_ns: The average in nanoseconds, or ``None`` when nothing has been
            sampled — dividing by a zero count is what this avoids.

    Returns:
        The average in milliseconds to three decimals, or ``"n/a"``.
    """
    if average_ns is None:
        return "n/a"
    return f"{average_ns / NANOSECONDS_PER_MILLISECOND:.3f}"


def format_summary(
    snapshot: CounterSnapshot,
    sent_per_second: float,
    consumed_per_second: float,
    average_latency_ns: float | None,
) -> str:
    """Render §7's block, for a periodic tick or for the final summary.

    Both roles share one renderer; what differs is only which figures the caller
    feeds it — live rates and the rolling-window latency while the run is in
    progress, overall rates and the overall latency at the end (§7).

    Args:
        snapshot: The counters to render.
        sent_per_second: §5.1's live or overall sent throughput.
        consumed_per_second: §5.2's live or overall consumed throughput.
        average_latency_ns: §6.1's rolling-window or §6.2's overall average, or
            ``None`` for ``n/a``.

    Returns:
        The seven lines §7 defines, without a trailing newline. Labels, order and
        completeness are fixed by the spec; the padding is not.
    """
    rows = (
        ("Messages sent:", format_count(snapshot.messages_sent)),
        ("Messages confirmed:", format_count(snapshot.messages_confirmed)),
        ("Messages not confirmed:", format_count(snapshot.messages_not_confirmed)),
        ("Messages consumed:", format_count(snapshot.messages_consumed)),
        ("Messages sent/sec:", format_rate(sent_per_second)),
        ("Messages consumed/sec:", format_rate(consumed_per_second)),
        ("Avg latency (ms):", format_latency_ms(average_latency_ns)),
    )
    return "\n".join(f"{label:<{LABEL_WIDTH}}{value}" for label, value in rows)


@dataclass(frozen=True)
class Tick:
    """What one periodic tick read, so the next one can diff against it (§5).

    Attributes:
        at: The monotonic instant of the reading.
        messages_sent: ``messages_sent`` as that tick read it.
        messages_consumed: ``messages_consumed`` as that tick read it.
    """

    at: float
    messages_sent: int
    messages_consumed: int


class StatsPrinter:
    """Prints §7's block every ``--stats-interval`` on its own thread (§4.6).

    Reads the counters and accumulators and never mutates any of them, and a slow
    stdout delays a tick rather than the run: nothing the publish loop or the
    delivery handler does waits on this thread. :meth:`stop` joins it, so the
    periodic prints can never interleave with the final summary.

    The very first tick has no previous tick to diff against, so it is skipped
    outright — both throughput figures together, since there is exactly one first
    tick for the pair (§5's closing paragraph).

    Example:
        >>> printer = StatsPrinter(Counters(), LatencyAccumulator(10), interval=1.0)
        >>> printer.start()
        >>> printer.stop()
    """

    def __init__(
        self,
        counters: Counters,
        latency: LatencyAccumulator,
        interval: float,
        stream: TextIO | None = None,
    ) -> None:
        """Create a printer; :meth:`start` puts it to work.

        Args:
            counters: The counters to report on.
            latency: The latency accumulators to report on.
            interval: Seconds between ticks.
            stream: Where to print; ``sys.stdout`` when omitted, which is also
                where the final summary goes.
        """
        self._counters = counters
        self._latency = latency
        self._interval = interval
        self._stream = stream if stream is not None else sys.stdout
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at = 0.0
        self._previous: Tick | None = None

    @property
    def is_running(self) -> bool:
        """Whether a printer thread is currently started."""
        return self._thread is not None

    def start(self) -> None:
        """Start ticking; a second call while running does nothing."""
        if self._thread is not None:
            return
        self._started_at = time.monotonic()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="perf-test-stats", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop ticking and wait for the thread to have finished printing.

        Idempotent, and the only thing standing between the last periodic block
        and the final summary (§4.6 point 2).
        """
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is None or thread is threading.current_thread():
            return
        thread.join(STATS_JOIN_TIMEOUT_SECONDS)
        if thread.is_alive():
            logger.warning("the stats printer did not stop within %.1fs", STATS_JOIN_TIMEOUT_SECONDS)

    def _run(self) -> None:
        """Print a tick every interval until :meth:`stop` is called."""
        while not self._stop.wait(self._interval):
            self.print_tick()

    def print_tick(self) -> bool:
        """Print one periodic block, prefixed with an elapsed-time marker.

        The prefix is not part of §7's block; it is only there so a caller reading
        the terminal can tell a live snapshot from the final summary. An
        unwritable stream is logged rather than allowed to kill the thread.

        Returns:
            Whether anything was printed. The first tick prints nothing: it only
            records the baseline the second tick diffs against (§5).
        """
        now = time.monotonic()
        snapshot = self._counters.snapshot()
        previous, self._previous = (
            self._previous,
            Tick(
                at=now,
                messages_sent=snapshot.messages_sent,
                messages_consumed=snapshot.messages_consumed,
            ),
        )
        if previous is None:
            return False
        elapsed_since_previous = now - previous.at
        block = format_summary(
            snapshot,
            instantaneous_rate(snapshot.messages_sent, previous.messages_sent, elapsed_since_previous),
            instantaneous_rate(snapshot.messages_consumed, previous.messages_consumed, elapsed_since_previous),
            self._latency.current_average_ns(),
        )
        try:
            print(f"--- {now - self._started_at:.1f}s elapsed ---\n{block}", file=self._stream, flush=True)
        except OSError as error:  # a broken pipe must not stop the run it reports on
            logger.warning("could not print the periodic stats: %s", error)
        return True


# --- run (§4) -----------------------------------------------------------


def classify_outcome(outcome: Outcome, counters: Counters) -> None:
    """Count one publish outcome in one of §4.5 point 3's two buckets.

    ``rejected`` and ``released`` are folded together here, unlike
    ``console_application.py``'s three-way breakdown — but each is still logged
    at warning level, so a caller chasing one does not need a counter for it.

    Args:
        outcome: What the broker reported for one message.
        counters: The counters to increment.
    """
    if outcome.state is OutcomeState.ACCEPTED:
        counters.record_confirmed()
        return
    if outcome.state in (OutcomeState.REJECTED, OutcomeState.RELEASED):
        logger.warning("a message was not confirmed: the broker settled it %s", outcome.state.value)
    else:  # pragma: no cover - OutcomeState has exactly the three members above
        logger.warning("counting an unmodelled outcome state %r as not confirmed", outcome.state)
    counters.record_not_confirmed()


def record_delivery(
    message: Message,
    counters: Counters,
    latency: LatencyAccumulator,
    missing_timestamp_warning: WarnOnce,
) -> None:
    """Account for one delivery: its latency when it has one, its count always (§4.3).

    A message whose ``x-send-timestamp`` is missing or undecodable still counts
    toward ``messages_consumed`` and is still accepted by the caller; only latency
    accounting skips it, and only one warning is logged for the whole run (§3
    point 4).

    Args:
        message: The received message.
        counters: The counters to increment.
        latency: The accumulators to add the sample to.
        missing_timestamp_warning: The run's one-shot warning for the skip path.
    """
    send_timestamp_ns = read_send_timestamp(message)
    if send_timestamp_ns is None:
        missing_timestamp_warning.warn(SEND_TIMESTAMP_PROPERTY)
    else:
        latency.record(time.monotonic_ns() - send_timestamp_ns)
    counters.record_consumed()


def map_queue_type(options: Options, specification: QueueSpecification) -> QueueSpecification:
    """Apply §2.1's queue-type mapping to ``specification``.

    Each sub-builder sets ``x-queue-type`` on the specification itself and
    ``queue()`` hands the same object back, so the returned builder is always
    ``specification`` — no further queue argument is set.

    Args:
        options: The options carrying ``queue_type``.
        specification: The builder from ``management.queue(name)``.

    Returns:
        The same builder, ready to :meth:`~src.QueueSpecification.declare`.

    Raises:
        OptionsError: If ``queue_type`` is not one of :data:`QUEUE_TYPES`.
    """
    if options.queue_type == QUEUE_TYPE_CLASSIC:
        return specification.classic().queue()
    if options.queue_type == QUEUE_TYPE_QUORUM:
        return specification.quorum().queue()
    if options.queue_type == QUEUE_TYPE_STREAM:
        return specification.stream().queue()
    raise OptionsError(f"unknown queue type {options.queue_type!r}, expected one of {', '.join(QUEUE_TYPES)}")


def build_connection_parameters(options: Options) -> ConnectionParameters:
    """Build the parameters §4.1 constructs the connection from.

    No ``on_unexpected_close`` callback is wired, and recovery is switched off
    rather than left at ``RecoveryConfiguration``'s own ``activated=True``: §4.1
    and §10 both make "no auto-reconnection" this program's contract, so an
    unexpected mid-run disconnect must stall the drain wait until
    ``--consume-timeout`` gives up instead of quietly resuming a benchmark whose
    numbers would no longer describe one continuous run.

    Args:
        options: The connection-related options, forwarded verbatim — this
            program adds no connection-level default of its own.

    Returns:
        The parameters this run's connection is built from.
    """
    return ConnectionParameters(
        host=options.host,
        port=options.port,
        virtual_host=options.virtual_host,
        user=options.user,
        password=options.password,
        tls=ssl.create_default_context() if options.tls else None,
        recovery_configuration=RecoveryConfiguration(activated=False),
    )


def build_consumer(
    connection: Connection,
    options: Options,
    counters: Counters,
    latency: LatencyAccumulator,
    missing_timestamp_warning: WarnOnce,
) -> Consumer:
    """Attach the consumer that times and counts every delivery (§4.3).

    Built before the publisher, so nothing this run publishes can arrive before
    something is already listening. ``initial_credits`` comes from
    ``--initial-credits``; ``settle_strategy`` is left at ``ConsumerBuilder``'s
    own default of ``EXPLICIT_SETTLE``, since a presettled run's latency would
    exclude the disposition round-trip this program means to measure (§10).

    Args:
        connection: The connection to attach on.
        options: The options carrying the queue name and the credit to grant.
        counters: The counters the delivery handler increments.
        latency: The accumulators the delivery handler feeds.
        missing_timestamp_warning: The run's one-shot warning for §3 point 4.

    Returns:
        The consumer, already receiving.

    Raises:
        AMQPError: If the broker refuses the queue, or does not answer the
            ``attach``.
    """

    def on_delivery(context: Context, message: Message) -> None:
        """Time and count the delivery, then accept it — this program never discards."""
        record_delivery(message, counters, latency, missing_timestamp_warning)
        try:
            context.accept()
        except AMQPError as error:  # the delivery still happened, so it still counts
            logger.warning("could not accept delivery %d: %s", context.delivery_id, error)

    return (
        connection.consumer_builder()
        .queue(options.queue)
        .initial_credits(options.initial_credits)
        .message_handler(on_delivery)
        .build()
    )


def publish_all(publisher: Publisher, options: Options, counters: Counters) -> None:
    """Publish ``options.messages`` messages, classifying every outcome (§4.5).

    Sequential: one message is settled before the next is sent. That is the
    slowest arrangement and the easiest to reason about, and §4.5 leaves the
    choice open — only the counting/stamping contract is fixed. Every attempt is
    made even after a failure: a broken publish is counted as not confirmed,
    logged at warning level with its index, and the loop continues (§4.5 point 4).
    ``AMQPError`` is caught rather than only ``AMQPTimeoutError``/
    ``PublisherError``, because a connection that died mid-run raises other
    subclasses — ``ProtocolError``, say — and those must not abort the whole loop
    either.

    Args:
        publisher: The bound publisher every message goes through.
        options: The options carrying the count, the body size and the per-call
            timeout.
        counters: The counters to update.
    """
    payload = build_payload(options.message_size)
    for index in range(options.messages):
        counters.record_sent()
        # The clock is read here, immediately before the call, so the latency this
        # stamp yields covers the publish itself and not the loop's own overhead.
        message = build_message(payload, time.monotonic_ns())
        try:
            result = publisher.publish(message, timeout=options.publish_timeout)
        except AMQPError as error:  # AMQPTimeoutError/PublisherError and anything else the link raises
            counters.record_not_confirmed()
            logger.warning("publishing message %d failed: %s", index, error)
            continue
        classify_outcome(result.outcome, counters)


def wait_for_consumption(
    counters: Counters,
    timeout: float,
    poll_interval: float = DRAIN_POLL_INTERVAL_SECONDS,
) -> bool:
    """Block until consumption catches up with the attempts made (§4.7).

    Args:
        counters: The counters to watch.
        timeout: Upper bound, in seconds, on how long to wait.
        poll_interval: Seconds between reads of the counters.

    Returns:
        Whether ``messages_consumed`` reached ``messages_sent``. ``False`` means
        ``--consume-timeout`` elapsed first — which, with no auto-reconnection
        wired in (§4.1), is also how a mid-run disconnect surfaces.
    """
    deadline = time.monotonic() + timeout
    idle = threading.Event()
    while True:
        snapshot = counters.snapshot()
        if snapshot.messages_consumed >= snapshot.messages_sent:
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning(
                "gave up after %.1fs waiting for consumption to catch up: %d of %d consumed",
                timeout,
                snapshot.messages_consumed,
                snapshot.messages_sent,
            )
            return False
        idle.wait(min(poll_interval, remaining))


def decide_exit_code(snapshot: CounterSnapshot, caught_up: bool) -> int:
    """Turn the final counters into this program's exit code (§8).

    Takes a snapshot rather than the live counters so the code describes exactly
    the numbers the summary printed: a delivery still arriving between the two
    reads must not decide the run against what a caller can see (§4.8).

    Args:
        snapshot: The counters as the final summary printed them.
        caught_up: What :func:`wait_for_consumption` returned.

    Returns:
        :data:`EXIT_OK` only when every attempt was confirmed and consumption
        caught up; a non-zero code otherwise, with the summary still printed by
        the caller either way.
    """
    if snapshot.messages_not_confirmed > 0:
        return EXIT_MESSAGES_NOT_CONFIRMED
    if not caught_up or snapshot.messages_consumed != snapshot.messages_sent:
        return EXIT_MESSAGES_NOT_CONSUMED
    return EXIT_OK


def tear_down(
    connection: Connection,
    consumer: Consumer | None,
    publisher: Publisher | None,
    options: Options,
) -> None:
    """Close everything this run opened, in §4.9's order.

    Every step runs even when an earlier one failed, and every failure is logged
    rather than raised: teardown never changes the exit code §8 already decided.

    Args:
        connection: The run's connection.
        consumer: The consumer, or ``None`` if it was never built.
        publisher: The publisher, or ``None`` if it was never built.
        options: The options carrying ``queue`` and ``keep_queue``.
    """
    if consumer is not None:
        _best_effort("closing the consumer", consumer.close)
    if publisher is not None:
        _best_effort("closing the publisher", publisher.close)
    if options.keep_queue:
        logger.info("leaving the queue %r in place, as --keep-queue asked", options.queue)
    else:
        _best_effort(
            f"deleting the queue {options.queue!r}",
            lambda: connection.management().queue(options.queue).delete(),
        )
    _best_effort("closing the connection", connection.close)


def _best_effort(description: str, step: Callable[[], object]) -> None:
    """Run one teardown step, logging whatever it raises.

    Args:
        description: What the step was doing, for the log line.
        step: The step itself.
    """
    try:
        step()
    except Exception as error:  # noqa: BLE001 - teardown continues whatever one step does
        logger.warning("ignoring an error while %s: %s", description, error)


@dataclass(frozen=True)
class RunResult:
    """Everything one run produced, beyond what it printed.

    :func:`main` only needs :attr:`exit_code`; the rest is what makes an
    end-to-end test able to assert on the accumulators (how many rolling windows
    completed, say) without scraping stdout.

    Attributes:
        exit_code: The code §8 decided for this run.
        counters: The counters as the final summary printed them.
        latency: The latency accumulators as the final summary printed them.
        sent_per_second: §5.1's overall sent throughput.
        consumed_per_second: §5.2's overall consumed throughput.
    """

    exit_code: int
    counters: CounterSnapshot = CounterSnapshot()
    latency: LatencySnapshot = LatencySnapshot()
    sent_per_second: float = 0.0
    consumed_per_second: float = 0.0


def run(options: Options) -> RunResult:
    """Run one whole measured publish/consume cycle (§4).

    Args:
        options: The already-validated options.

    Returns:
        The run's result, whose ``exit_code`` is one of the codes this module
        defines, per §8. A setup failure returns before anything is measured, so
        its counters and accumulators are all zero and no summary is printed.
    """
    counters = Counters()
    latency = LatencyAccumulator(options.latency_window_size)
    missing_timestamp_warning = WarnOnce(
        "at least one delivery carried no usable %r property, so it counts toward the consumed total "
        "but not toward latency; this is only logged once per run"
    )
    try:
        connection = Connection(build_connection_parameters(options))
    except (AMQPError, OSError) as error:
        logger.error("could not connect to %s:%s: %s", options.host, options.port or "default", error)
        return RunResult(exit_code=EXIT_SETUP_FAILED)
    consumer: Consumer | None = None
    publisher: Publisher | None = None
    try:
        try:
            info = map_queue_type(options, connection.management().queue(options.queue)).declare()
            logger.info("declared the %s queue %r", info.queue_type.value, info.name)
        except (AMQPError, OptionsError) as error:
            logger.error("could not declare the %s queue %r: %s", options.queue_type, options.queue, error)
            return RunResult(exit_code=EXIT_SETUP_FAILED)
        try:
            consumer = build_consumer(connection, options, counters, latency, missing_timestamp_warning)
            publisher = connection.publisher_builder().queue(options.queue).build()
        except AMQPError as error:
            logger.error("could not attach a link to the queue %r: %s", options.queue, error)
            return RunResult(exit_code=EXIT_SETUP_FAILED)
        printer = StatsPrinter(counters, latency, options.stats_interval)
        # One clock start for the whole run, shared by both throughput families
        # (§5.2), taken at the same instant the printer starts.
        start_time = time.monotonic()
        printer.start()
        try:
            logger.info(
                "publishing %d message(s) of %d byte(s) to %r", options.messages, options.message_size, options.queue
            )
            publish_all(publisher, options, counters)
            publish_end = time.monotonic()
            caught_up = wait_for_consumption(counters, options.consume_timeout)
            drain_end = time.monotonic()
        finally:
            # Fully stopped before the final summary prints, so the two can never
            # interleave on stdout (§4.6 point 2).
            printer.stop()
        snapshot = counters.snapshot()
        latency_snapshot = latency.snapshot()
        # The sent figure excludes the drain wait and the consumed one includes
        # it: they measure different things (§5.1/§5.2).
        sent_per_second = overall_rate(snapshot.messages_sent, publish_end - start_time)
        consumed_per_second = overall_rate(snapshot.messages_consumed, drain_end - start_time)
        print(format_summary(snapshot, sent_per_second, consumed_per_second, latency_snapshot.overall_average_ns))
        return RunResult(
            exit_code=decide_exit_code(snapshot, caught_up),
            counters=snapshot,
            latency=latency_snapshot,
            sent_per_second=sent_per_second,
            consumed_per_second=consumed_per_second,
        )
    finally:
        tear_down(connection, consumer, publisher, options)


def main(argv: list[str] | None = None) -> int:
    """Parse the options, run the cycle, and return the process exit code.

    Args:
        argv: Argument list without the program name; ``sys.argv[1:]`` when
            ``None``.

    Returns:
        The exit code, per §8. Invalid options return
        :data:`EXIT_INVALID_OPTIONS` without connecting to anything and without
        printing a summary.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    try:
        options = parse_args(argv)
    except OptionsError as error:
        logger.error("%s", error)
        return EXIT_INVALID_OPTIONS
    try:
        return run(options).exit_code
    except KeyboardInterrupt:
        logger.warning("interrupted")
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
