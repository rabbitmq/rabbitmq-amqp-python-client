"""Console application: the whole client exercised end to end (step_045).

Declares a queue of a caller-chosen type, publishes a caller-chosen number of
messages to it, consumes them back, logs it if the connection dies mid-run, and
prints how it all went — first periodically while the run is in progress, then
once as a final summary. The process exit code says whether every message was
both confirmed and consumed, so this doubles as a scriptable smoke test::

    python3 docs/examples/console_application.py --messages 500 --consume-timeout 15
    python3 docs/examples/console_application.py --messages 500 --queue-type quorum
    python3 docs/examples/console_application.py --help

This program adds nothing to the client's public surface: it is built entirely
out of :class:`~src.Connection`,
:class:`~src.Management`,
:class:`~src.Publisher`,
:class:`~src.Consumer` and
:class:`~src.RecoveryConfiguration`. It owns exactly one
connection: it is a correctness/smoke-test tool, not a load generator.

Exit codes:
    0: every message confirmed and consumed, no unexpected closure.
    1: at least one message was not confirmed (rejected, released or failed), or
       the publish loop did not make every attempt.
    2: the options were rejected before anything was connected.
    3: connecting, declaring the queue, or attaching a link failed.
    4: the drain wait timed out before consumption caught up.
    5: the connection closed unexpectedly during the run.
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
from typing import TYPE_CHECKING, TextIO

# Make the client importable when this example is run straight from a checkout,
# with neither the package installed nor PYTHONPATH set.
_SOURCE_ROOT = Path(__file__).resolve().parents[2]
if _SOURCE_ROOT.is_dir() and str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from src import (  # noqa: E402 - after the sys.path bootstrap above
    AMQPError,
    Connection,
    ConnectionParameters,
    Consumer,
    Context,
    Message,
    Outcome,
    OutcomeState,
    Publisher,
    QueueSpecification,
    RecoveryConfiguration,
)

if TYPE_CHECKING:
    from src.connection import ConnectionState

# --- exit codes (§7) ----------------------------------------------------

EXIT_OK = 0
EXIT_MESSAGES_NOT_CONFIRMED = 1
EXIT_INVALID_OPTIONS = 2
EXIT_SETUP_FAILED = 3
EXIT_MESSAGES_NOT_CONSUMED = 4
EXIT_UNEXPECTED_CLOSE = 5
EXIT_INTERRUPTED = 130

# --- option defaults (§2) -----------------------------------------------

DEFAULT_MESSAGE_COUNT = 1_000_000
DEFAULT_CONSUME_TIMEOUT_SECONDS = 30.0
DEFAULT_PUBLISH_TIMEOUT_SECONDS = 5.0
DEFAULT_STATS_INTERVAL_SECONDS = 1.0

QUEUE_TYPE_CLASSIC = "classic"
QUEUE_TYPE_QUORUM = "quorum"
QUEUE_TYPE_STREAM = "stream"

#: The three values ``--queue-type`` accepts, in the order ``--help`` lists them.
QUEUE_TYPES = (QUEUE_TYPE_CLASSIC, QUEUE_TYPE_QUORUM, QUEUE_TYPE_STREAM)

#: Prefix of the queue name generated when ``--queue`` is not given.
GENERATED_QUEUE_PREFIX = "console-app-"

# --- timing -------------------------------------------------------------

#: How long the drain wait blocks per poll before re-reading the counters.
DRAIN_POLL_INTERVAL_SECONDS = 0.05

#: How long :meth:`StatsPrinter.stop` waits for the printer thread to end.
STATS_JOIN_TIMEOUT_SECONDS = 5.0

# --- output (§6) --------------------------------------------------------

#: Column the summary's values start at, so every label is padded to it.
LABEL_WIDTH = 31

logger = logging.getLogger("console-application")


class OptionsError(ValueError):
    """An option, or a combination of them, was rejected before connecting.

    Raised by :func:`parse_args` for everything argparse itself cannot check —
    notably ``--messages 0`` (§7 row 1). :func:`main` catches it, logs it at
    error level and exits :data:`EXIT_INVALID_OPTIONS` without ever opening a
    connection, so nothing about the broker can influence that path.
    """


@dataclass(frozen=True)
class Options:
    """Everything this run was configured with (§2).

    Attributes:
        messages: How many messages to publish; never ``0``.
        queue_type: One of :data:`QUEUE_TYPES`.
        queue: Name of the queue to declare, publish to and consume from.
        keep_queue: Whether teardown leaves the queue in place.
        consume_timeout: Seconds the drain wait blocks for at most.
        publish_timeout: Per-call timeout passed to every ``publish()``.
        stats_interval: Seconds between periodic stats blocks.
        host: Broker host.
        port: Broker port, or ``None`` to let ``ConnectionParameters`` default it
            from ``tls``.
        user: SASL PLAIN username.
        password: SASL PLAIN password.
        virtual_host: RabbitMQ virtual host.
        tls: Whether to wrap the socket in TLS with a default SSL context.
        recovery: ``RecoveryConfiguration.activated``.
        recovery_topology: ``RecoveryConfiguration.topology``.
    """

    messages: int = DEFAULT_MESSAGE_COUNT
    queue_type: str = QUEUE_TYPE_CLASSIC
    queue: str = ""
    keep_queue: bool = False
    consume_timeout: float = DEFAULT_CONSUME_TIMEOUT_SECONDS
    publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS
    stats_interval: float = DEFAULT_STATS_INTERVAL_SECONDS
    host: str = "localhost"
    port: int | None = None
    user: str = "guest"
    password: str = "guest"  # noqa: S105 - the documented local-broker default
    virtual_host: str = "/"
    tls: bool = False
    recovery: bool = True
    recovery_topology: bool = False


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
        prog="console_application.py",
        description="Publish and consume messages against one RabbitMQ queue, then report what happened.",
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
    parser.add_argument("--queue", default="", help="queue name (default: a generated console-app-* name)")
    parser.add_argument("--keep-queue", action="store_true", help="do not delete the queue during teardown")
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
    parser.add_argument("--host", default="localhost", help="broker host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=None, help="broker port (default: 5672, or 5671 with --tls)")
    parser.add_argument("--user", default="guest", help="SASL PLAIN username (default: %(default)s)")
    parser.add_argument("--password", default="guest", help="SASL PLAIN password (default: %(default)s)")
    parser.add_argument("--vhost", default="/", help="virtual host (default: %(default)s)")
    parser.add_argument("--tls", action="store_true", help="wrap the connection in TLS")
    parser.add_argument(
        "--recovery",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="auto-reconnect after an unexpected disconnect (default: enabled)",
    )
    parser.add_argument(
        "--recovery-topology",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="redeclare recorded topology after a reconnect (default: disabled)",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> Options:
    """Parse ``argv`` into :class:`Options`, rejecting what §7 rejects.

    Args:
        argv: Argument list without the program name; ``sys.argv[1:]`` when
            ``None``.

    Returns:
        The options this run uses, with ``--queue`` filled in with a generated
        name when it was not given.

    Raises:
        OptionsError: If ``--messages`` is not > 0, or any timeout/interval is
            invalid. Raised *before* anything connects, which is what §7's first
            row requires.
        SystemExit: Propagated from argparse for an unknown flag or an
            unparseable value.
    """
    namespace = build_parser().parse_args(argv)
    if namespace.messages <= 0:
        raise OptionsError(f"--messages must be > 0, got {namespace.messages}: there would be nothing to measure")
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
        consume_timeout=namespace.consume_timeout,
        publish_timeout=namespace.publish_timeout,
        stats_interval=namespace.stats_interval,
        host=namespace.host,
        port=namespace.port,
        user=namespace.user,
        password=namespace.password,
        virtual_host=namespace.vhost,
        tls=namespace.tls,
        recovery=namespace.recovery,
        recovery_topology=namespace.recovery_topology,
    )


@dataclass(frozen=True)
class CounterSnapshot:
    """One consistent read of every counter (§5).

    Taken under :class:`Counters`' lock, so the derived totals below always add
    up even though the publish loop, the delivery loop and the unexpected-close
    callback all keep incrementing concurrently.

    Attributes:
        messages_sent: Publish attempts made.
        messages_confirmed: Attempts the broker settled ``accepted``.
        messages_rejected: Attempts the broker settled ``rejected``.
        messages_released: Attempts the broker settled ``released``.
        messages_failed: Attempts whose ``publish()`` call raised.
        messages_consumed: Deliveries handed to the message handler.
        unexpected_close_count: ``0`` or ``1``, never more (§4 point 3).
    """

    messages_sent: int = 0
    messages_confirmed: int = 0
    messages_rejected: int = 0
    messages_released: int = 0
    messages_failed: int = 0
    messages_consumed: int = 0
    unexpected_close_count: int = 0

    @property
    def messages_not_confirmed(self) -> int:
        """Attempts that ended as anything other than ``accepted``."""
        return self.messages_rejected + self.messages_released + self.messages_failed

    @property
    def messages_classified(self) -> int:
        """Attempts whose fate is known; below :attr:`messages_sent` mid-run."""
        return self.messages_confirmed + self.messages_not_confirmed


class Counters:
    """The run's counters, shared by every thread that touches them (§5).

    All process-local and reset to zero here: this is a one-shot run, not a
    service with persisted metrics. Every increment and every read goes through
    one lock, so :meth:`snapshot` never observes a half-updated set.

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
        """Count one publish attempt, immediately before it is issued (§3.5 point 1)."""
        self._bump(messages_sent=1)

    def record_confirmed(self) -> None:
        """Count one ``accepted`` outcome."""
        self._bump(messages_confirmed=1)

    def record_rejected(self) -> None:
        """Count one ``rejected`` outcome."""
        self._bump(messages_rejected=1)

    def record_released(self) -> None:
        """Count one ``released`` outcome."""
        self._bump(messages_released=1)

    def record_failed(self) -> None:
        """Count one publish attempt whose call raised instead of returning."""
        self._bump(messages_failed=1)

    def record_consumed(self) -> None:
        """Count one delivery reaching the message handler (§3.3 point 1)."""
        self._bump(messages_consumed=1)

    def record_unexpected_close(self) -> None:
        """Count the one unexpected closure this run can ever see (§4 point 2)."""
        self._bump(unexpected_close_count=1)

    def _bump(self, **deltas: int) -> None:
        """Add ``deltas`` to the current snapshot, replacing it under the lock."""
        with self._lock:
            current = self._snapshot
            self._snapshot = CounterSnapshot(
                messages_sent=current.messages_sent + deltas.get("messages_sent", 0),
                messages_confirmed=current.messages_confirmed + deltas.get("messages_confirmed", 0),
                messages_rejected=current.messages_rejected + deltas.get("messages_rejected", 0),
                messages_released=current.messages_released + deltas.get("messages_released", 0),
                messages_failed=current.messages_failed + deltas.get("messages_failed", 0),
                messages_consumed=current.messages_consumed + deltas.get("messages_consumed", 0),
                unexpected_close_count=current.unexpected_close_count + deltas.get("unexpected_close_count", 0),
            )


def format_snapshot(snapshot: CounterSnapshot) -> str:
    """Render §6's block for one snapshot.

    Args:
        snapshot: The counters to render.

    Returns:
        The eight lines §6 defines, without a trailing newline. Labels, order and
        completeness are fixed by the spec; the padding is not.
    """
    rows = (
        ("Messages sent:", snapshot.messages_sent),
        ("Messages confirmed:", snapshot.messages_confirmed),
        ("Messages not confirmed:", snapshot.messages_not_confirmed),
        ("  rejected:", snapshot.messages_rejected),
        ("  released:", snapshot.messages_released),
        ("  failed:", snapshot.messages_failed),
        ("Messages consumed:", snapshot.messages_consumed),
        ("Unexpected closures:", snapshot.unexpected_close_count),
    )
    return "\n".join(f"{label:<{LABEL_WIDTH}}{value}" for label, value in rows)


def format_summary(counters: Counters) -> str:
    """Render §6's block for ``counters`` as they stand right now.

    Args:
        counters: The run's counters.

    Returns:
        The rendered block, without a trailing newline.
    """
    return format_snapshot(counters.snapshot())


def classify_outcome(outcome: Outcome, counters: Counters) -> None:
    """Count one publish outcome in exactly one bucket (§3.5 point 3).

    Never raises: an outcome state this client does not model cannot reach here,
    because ``outcome_from_delivery_state`` has already rejected it, so the
    fallthrough below only logs and counts the attempt as a failure.

    Args:
        outcome: What the broker reported for one message.
        counters: The counters to increment.
    """
    if outcome.state is OutcomeState.ACCEPTED:
        counters.record_confirmed()
    elif outcome.state is OutcomeState.REJECTED:
        counters.record_rejected()
    elif outcome.state is OutcomeState.RELEASED:
        counters.record_released()
    else:  # pragma: no cover - OutcomeState has exactly the three members above
        logger.warning("counting an unmodelled outcome state %r as a failure", outcome.state)
        counters.record_failed()


def decide_exit_code(options: Options, counters: Counters) -> int:
    """Turn the final counters into this program's exit code (§7).

    The checks run in the order §7's table implies: an unexpected closure decides
    the run on its own, whatever progress it had already made.

    Args:
        options: The options the run used, for the expected attempt count.
        counters: The counters as they stood when the drain wait returned.

    Returns:
        :data:`EXIT_OK` only when every attempt was made, every one of them was
        confirmed, consumption caught up, and the connection never dropped.
    """
    snapshot = counters.snapshot()
    if snapshot.unexpected_close_count > 0:
        return EXIT_UNEXPECTED_CLOSE
    if snapshot.messages_sent != options.messages:
        return EXIT_MESSAGES_NOT_CONFIRMED
    if snapshot.messages_confirmed != snapshot.messages_sent:
        return EXIT_MESSAGES_NOT_CONFIRMED
    if snapshot.messages_consumed != snapshot.messages_sent:
        return EXIT_MESSAGES_NOT_CONSUMED
    return EXIT_OK


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


def build_connection_parameters(
    options: Options,
    on_unexpected_close: Callable[[BaseException | None], None],
) -> ConnectionParameters:
    """Build the parameters §3.1 point 2 constructs the connection from.

    Args:
        options: The connection-related options, forwarded verbatim — this
            program adds no connection-level default of its own.
        on_unexpected_close: §4's callback.

    Returns:
        The parameters, carrying a ``RecoveryConfiguration`` built from
        ``--recovery``/``--recovery-topology``.
    """
    return ConnectionParameters(
        host=options.host,
        port=options.port,
        virtual_host=options.virtual_host,
        user=options.user,
        password=options.password,
        tls=ssl.create_default_context() if options.tls else None,
        on_unexpected_close=on_unexpected_close,
        recovery_configuration=RecoveryConfiguration(
            activated=options.recovery,
            topology=options.recovery_topology,
        ),
    )


class UnexpectedCloseReporter:
    """§4's ``logUnexpectedClose``, as a callable with the state it needs.

    Wired onto ``ConnectionParameters.on_unexpected_close`` before the connection
    exists, so :attr:`connection` is set immediately afterwards and read only
    when the callback actually fires. With ``--recovery`` on (the default) that
    only happens once every reconnection attempt has already failed, at which
    point the connection is dead for good — which is why :attr:`aborted` is set
    too, cutting both the publish loop and the drain wait short instead of
    waiting out ``--consume-timeout`` against a connection that can never
    deliver anything again.

    Attributes:
        connection: The connection being watched, for its ``state``; ``None``
            until it has been constructed.
        aborted: Set when the callback fires, and never cleared.
    """

    def __init__(self, counters: Counters) -> None:
        """Create a reporter feeding ``counters``.

        Args:
            counters: The counters to record the closure in.
        """
        self._counters = counters
        self.connection: Connection | None = None
        self.aborted = threading.Event()

    def __call__(self, error: BaseException | None) -> None:
        """Log the closure once, at error level, and record it (§4 points 1-2).

        Args:
            error: What killed the connection, or ``None`` when the peer closed
                cleanly without an ``error``.
        """
        snapshot = self._counters.snapshot()
        logger.error(
            "the connection closed unexpectedly (state=%s, sent=%d, confirmed=%d, consumed=%d): %s",
            self._state_name(),
            snapshot.messages_sent,
            snapshot.messages_confirmed,
            snapshot.messages_consumed,
            error,
        )
        self._counters.record_unexpected_close()
        self.aborted.set()

    def _state_name(self) -> str:
        """Describe the watched connection's state, tolerating its absence."""
        connection = self.connection
        if connection is None:
            return "unknown"
        state: ConnectionState = connection.state
        return state.value


class StatsPrinter:
    """Prints §6's block every ``--stats-interval`` on its own thread (§3.9).

    Reads the counters and never mutates them, and a slow stdout delays a tick
    rather than the run: nothing the publish or delivery loop does waits on this
    thread. :meth:`stop` joins it, so the periodic prints can never interleave
    with the final summary.

    Example:
        >>> printer = StatsPrinter(Counters(), interval=1.0)
        >>> printer.start()
        >>> printer.stop()
    """

    def __init__(self, counters: Counters, interval: float, stream: TextIO | None = None) -> None:
        """Create a printer; :meth:`start` puts it to work.

        Args:
            counters: The counters to report on.
            interval: Seconds between ticks.
            stream: Where to print; ``sys.stdout`` when omitted, which is also
                where the final summary goes.
        """
        self._counters = counters
        self._interval = interval
        self._stream = stream if stream is not None else sys.stdout
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at = 0.0

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
        self._thread = threading.Thread(target=self._run, name="console-app-stats", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop ticking and wait for the thread to have finished printing.

        Idempotent, and the only thing standing between the last periodic block
        and the final summary (§3.9 point 3).
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

    def print_tick(self) -> None:
        """Print one periodic block, prefixed with an elapsed-time marker.

        The prefix is not part of §6's block; it is only there so a caller
        reading the terminal can tell a live snapshot from the final summary. An
        unwritable stream is logged rather than allowed to kill the thread.
        """
        elapsed = time.monotonic() - self._started_at
        block = format_summary(self._counters)
        try:
            print(f"--- {elapsed:.1f}s elapsed ---\n{block}", file=self._stream, flush=True)
        except OSError as error:  # a broken pipe must not stop the run it reports on
            logger.warning("could not print the periodic stats: %s", error)


def build_message(index: int) -> Message:
    """Build the message published at ``index``.

    Args:
        index: Zero-based position in the publish loop.

    Returns:
        A message whose body carries that index, so a caller reading broker-side
        logs or tracing can correlate one delivery back to one attempt.
    """
    return Message(f"console-app-message-{index}")


def build_consumer(connection: Connection, options: Options, counters: Counters) -> Consumer:
    """Attach the consumer that counts every delivery (§3.3).

    Built before the publisher, so nothing this run publishes can arrive before
    something is already listening. ``initial_credits`` and ``settle_strategy``
    are left at ``ConsumerBuilder``'s own defaults (``EXPLICIT_SETTLE``).

    Args:
        connection: The connection to attach on.
        options: The options carrying the queue name.
        counters: The counters the delivery handler increments.

    Returns:
        The consumer, already receiving.

    Raises:
        AMQPError: If the broker refuses the queue, or does not answer the
            ``attach``.
    """

    def on_delivery(context: Context, message: Message) -> None:
        """Count the delivery, then accept it — this program never discards."""
        counters.record_consumed()
        try:
            context.accept()
        except AMQPError as error:  # the delivery still happened, so it still counts
            logger.warning("could not accept delivery %d: %s", context.delivery_id, error)

    return connection.consumer_builder().queue(options.queue).message_handler(on_delivery).build()


def publish_all(publisher: Publisher, options: Options, counters: Counters, aborted: threading.Event) -> None:
    """Publish ``options.messages`` messages, classifying every outcome (§3.5).

    Sequential: one message is settled before the next is sent. That is the
    slowest arrangement and the easiest to reason about, and §3.5 leaves the
    choice open — only the counting contract is fixed. A failed publish is logged
    and the loop continues; only an unexpected closure ends it early, because
    from that point on every remaining attempt would just wait out
    ``--publish-timeout`` against a dead connection (§4 point 3).

    Args:
        publisher: The bound publisher every message goes through.
        options: The options carrying the count and the per-call timeout.
        counters: The counters to update.
        aborted: Set by §4's callback when the connection died for good.
    """
    for index in range(options.messages):
        if aborted.is_set():
            logger.warning(
                "abandoning the publish loop after %d of %d attempts: the connection is gone",
                index,
                options.messages,
            )
            return
        counters.record_sent()
        try:
            result = publisher.publish(build_message(index), timeout=options.publish_timeout)
        except AMQPError as error:  # AMQPTimeoutError/PublisherError and anything else the link raises
            counters.record_failed()
            logger.warning("publishing message %d failed: %s", index, error)
            time.sleep(1)
            continue
        classify_outcome(result.outcome, counters)


def wait_for_consumption(
    counters: Counters,
    timeout: float,
    aborted: threading.Event,
    poll_interval: float = DRAIN_POLL_INTERVAL_SECONDS,
) -> bool:
    """Block until consumption catches up with the attempts made (§3.6).

    Args:
        counters: The counters to watch.
        timeout: Upper bound, in seconds, on how long to wait.
        aborted: Set by §4's callback; makes this return immediately rather than
            wait out ``timeout`` against a dead connection.
        poll_interval: Seconds between reads of the counters.

    Returns:
        Whether ``messages_consumed`` reached ``messages_sent``. ``False`` means
        the timeout elapsed or the connection died — neither is, by itself, a
        statement about how many messages were confirmed.
    """
    deadline = time.monotonic() + timeout
    while True:
        snapshot = counters.snapshot()
        if snapshot.messages_consumed >= snapshot.messages_sent:
            return True
        if aborted.is_set():
            logger.warning("the connection died, so the drain wait stops without consumption catching up")
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning(
                "gave up after %.1fs waiting for consumption to catch up: %d of %d consumed",
                timeout,
                snapshot.messages_consumed,
                snapshot.messages_sent,
            )
            return False
        # Waiting on the abort event rather than sleeping wakes this loop the
        # moment §4's callback fires.
        aborted.wait(min(poll_interval, remaining))


def tear_down(
    connection: Connection,
    consumer: Consumer | None,
    publisher: Publisher | None,
    options: Options,
) -> None:
    """Close everything this run opened, in §3.8's order.

    Every step runs even when an earlier one failed, and every failure is logged
    rather than raised: teardown is never this program's pass/fail signal (§7's
    last row).

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


def run(options: Options) -> int:
    """Run one whole publish/consume cycle and return its exit code (§3).

    Args:
        options: The already-validated options.

    Returns:
        One of the exit codes this module defines, per §7.
    """
    counters = Counters()
    reporter = UnexpectedCloseReporter(counters)
    try:
        connection = Connection(build_connection_parameters(options, reporter))
    except (AMQPError, OSError) as error:
        logger.error("could not connect to %s:%s: %s", options.host, options.port or "default", error)
        return EXIT_SETUP_FAILED
    reporter.connection = connection
    consumer: Consumer | None = None
    publisher: Publisher | None = None
    try:
        try:
            info = map_queue_type(options, connection.management().queue(options.queue)).declare()
            logger.info("declared the %s queue %r", info.queue_type.value, info.name)
        except (AMQPError, OptionsError) as error:
            logger.error("could not declare the %s queue %r: %s", options.queue_type, options.queue, error)
            return EXIT_SETUP_FAILED
        try:
            consumer = build_consumer(connection, options, counters)
            publisher = connection.publisher_builder().queue(options.queue).build()
        except AMQPError as error:
            logger.error("could not attach a link to the queue %r: %s", options.queue, error)
            return EXIT_SETUP_FAILED
        printer = StatsPrinter(counters, options.stats_interval)
        printer.start()
        try:
            logger.info("publishing %d message(s) to %r", options.messages, options.queue)
            publish_all(publisher, options, counters, reporter.aborted)
            wait_for_consumption(counters, options.consume_timeout, reporter.aborted)
        finally:
            # Fully stopped before the final summary prints, so the two can
            # never interleave on stdout (§3.9 point 3).
            printer.stop()
        print(format_summary(counters))
        return decide_exit_code(options, counters)
    finally:
        tear_down(connection, consumer, publisher, options)


def main(argv: list[str] | None = None) -> int:
    """Parse the options, run the cycle, and return the process exit code.

    Args:
        argv: Argument list without the program name; ``sys.argv[1:]`` when
            ``None``.

    Returns:
        The exit code, per §7. Invalid options return
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
        return run(options)
    except KeyboardInterrupt:
        logger.warning("interrupted")
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
