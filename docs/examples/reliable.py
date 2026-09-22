"""Reliable messaging: publishing and consuming through a broker outage.

Modeled on rabbitmq-amqp-go-client's own reliable example
(https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/reliable):
one connection, a quorum queue, a publisher and a consumer both built once and
left running while messages flow, and a background disconnect/reconnect can
happen at any point without either of them being rebuilt. Every publish
outcome is classified (accepted/released/rejected/failed) and every delivery
is validated and counted, with periodic stats logged the whole time it runs.

Like its Go counterpart, every setting here is an environment variable —
there are no command-line flags — which is what lets
docs/examples/docker/reliable/Dockerfile run this script as a container
configured entirely through ``docker run -e``::

    QUEUE_NAME=my-queue MESSAGES_TO_SEND=1000 \\
        PYTHONPATH=. .venv/bin/python docs/examples/reliable.py

    docker build -f docs/examples/docker/reliable/Dockerfile -t reliable-example .
    docker run --rm -e AMQP_HOST=rabbitmq -e MESSAGES_TO_SEND=1000 reliable-example

See docs/examples/docker/reliable/README.md for the full variable table.

Unlike ``console_application.py``, this is not a scriptable smoke test with a
meaningful exit code — like the Go example, it is a long-running demo you
either watch until it finishes publishing (``IS_SILENT=false``, the default)
or leave running as a container (``IS_SILENT=true``) until it is stopped.

This program adds nothing to the client's public surface: it is built
entirely out of :class:`~rabbitmq_amqp_python_client.Connection`,
:class:`~rabbitmq_amqp_python_client.Publisher`,
:class:`~rabbitmq_amqp_python_client.Consumer` and
:class:`~rabbitmq_amqp_python_client.RecoveryConfiguration`.
"""

from __future__ import annotations

import contextlib
import logging
import os
import ssl
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

# Make the client importable when this example is run straight from a checkout,
# with neither the package installed nor PYTHONPATH set. Guarded by a length
# check because a containerized copy of this script (see
# docs/examples/docker/reliable/) lives too shallow in the image's filesystem
# for a third parent to exist.
_RESOLVED_PARENTS = Path(__file__).resolve().parents
_SOURCE_ROOT = _RESOLVED_PARENTS[2] if len(_RESOLVED_PARENTS) > 2 else None
if _SOURCE_ROOT is not None and _SOURCE_ROOT.is_dir() and str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from rabbitmq_amqp_python_client import (  # noqa: E402 - after the sys.path bootstrap above
    AMQPError,
    ApplicationProperties,
    Connection,
    ConnectionParameters,
    ConnectionState,
    Consumer,
    Context,
    Message,
    Outcome,
    OutcomeState,
    Publisher,
    RecoveryConfiguration,
)

TAG = "[python-amqp1.0]"

EXIT_OK = 0
EXIT_INVALID_CONFIGURATION = 2
EXIT_SETUP_FAILED = 3
EXIT_INTERRUPTED = 130

DEFAULT_QUEUE_NAME = "reliable-amqp10-python-queue"
DEFAULT_MESSAGES_TO_SEND = 500_000

STATS_INTERVAL_SECONDS = 5.0
STATE_POLL_INTERVAL_SECONDS = 0.2
PUBLISH_TIMEOUT_SECONDS = 5.0
DELAY_MESSAGE_SECONDS = 0.2
JOIN_TIMEOUT_SECONDS = 5.0

_ENV_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_ENV_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

logger = logging.getLogger("reliable-example")


class ConfigurationError(ValueError):
    """An environment variable was rejected before anything connected."""


def _env_str(name: str, default: str) -> str:
    """Return the string environment variable ``name``, or ``default`` if unset."""
    return os.environ.get(name, default)


def _env_optional_int(name: str) -> int | None:
    """Return ``name`` parsed as an int, or ``None`` if unset.

    Raises:
        ConfigurationError: If ``name`` is set but is not a valid integer.
    """
    value = os.environ.get(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        raise ConfigurationError(f"{name}={value!r} is not a valid integer") from None


def _env_int(name: str, default: int) -> int:
    """Return ``name`` parsed as an int, or ``default`` if unset."""
    value = _env_optional_int(name)
    return default if value is None else value


def _env_bool(name: str, default: bool) -> bool:
    """Return the boolean ``name`` is set to, or ``default`` if unset.

    Accepts ``1``/``true``/``yes``/``on`` and ``0``/``false``/``no``/``off``,
    case-insensitively.

    Raises:
        ConfigurationError: If ``name`` is set to anything else.
    """
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in _ENV_TRUE_VALUES:
        return True
    if normalized in _ENV_FALSE_VALUES:
        return False
    raise ConfigurationError(f"{name}={value!r} is not a valid boolean (true/false, yes/no, on/off, 1/0)")


@dataclass(frozen=True)
class Options:
    """Everything this run was configured with, all from the environment.

    Attributes:
        queue_name: Quorum queue to declare, publish to and consume from.
        messages_to_send: How many messages the publish loop attempts.
        is_silent: ``True`` runs forever instead of waiting on stdin, for
            containerized/non-interactive runs.
        delay_message: Whether to pace the publish loop with a small sleep
            between messages.
        host: Broker host.
        port: Broker port, or ``None`` to let ``ConnectionParameters`` default
            it from ``tls``.
        user: SASL PLAIN username.
        password: SASL PLAIN password.
        virtual_host: RabbitMQ virtual host.
        tls: Whether to wrap the connection in TLS with a default SSL context.
    """

    queue_name: str = DEFAULT_QUEUE_NAME
    messages_to_send: int = DEFAULT_MESSAGES_TO_SEND
    is_silent: bool = False
    delay_message: bool = False
    host: str = "localhost"
    port: int | None = None
    user: str = "guest"
    password: str = "guest"  # noqa: S105 - the documented local-broker default
    virtual_host: str = "/"
    tls: bool = False


def read_options() -> Options:
    """Read every setting from its environment variable.

    Returns:
        The options this run uses.

    Raises:
        ConfigurationError: If ``MESSAGES_TO_SEND`` is not > 0, or any
            variable is set to a value its type cannot accept.
    """
    queue_name = _env_str("QUEUE_NAME", DEFAULT_QUEUE_NAME)
    if not queue_name:
        raise ConfigurationError("QUEUE_NAME must not be empty")
    messages_to_send = _env_int("MESSAGES_TO_SEND", DEFAULT_MESSAGES_TO_SEND)
    if messages_to_send <= 0:
        raise ConfigurationError(f"MESSAGES_TO_SEND must be > 0, got {messages_to_send}")
    return Options(
        queue_name=queue_name,
        messages_to_send=messages_to_send,
        is_silent=_env_bool("IS_SILENT", False),
        delay_message=_env_bool("DELAY_MESSAGE", False),
        host=_env_str("AMQP_HOST", "localhost"),
        port=_env_optional_int("AMQP_PORT"),
        user=_env_str("AMQP_USER", "guest"),
        password=_env_str("AMQP_PASSWORD", "guest"),
        virtual_host=_env_str("AMQP_VHOST", "/"),
        tls=_env_bool("AMQP_TLS", False),
    )


@dataclass(frozen=True)
class CounterSnapshot:
    """One consistent read of every counter.

    Attributes:
        accepted: Publish attempts the broker settled ``accepted``.
        released: Publish attempts the broker settled ``released``.
        rejected: Publish attempts the broker settled ``rejected``.
        failed: Publish attempts whose ``publish()`` call raised.
        received: Deliveries handed to the message handler.
    """

    accepted: int = 0
    released: int = 0
    rejected: int = 0
    failed: int = 0
    received: int = 0

    @property
    def settled(self) -> int:
        """Attempts the broker returned an outcome for, of any kind."""
        return self.accepted + self.released + self.rejected


class Counters:
    """The run's counters, shared by the publish loop and the consumer callback.

    All process-local: this is a one-shot demo, not a service with persisted
    metrics. Every increment and every read goes through one lock, so
    :meth:`snapshot` never observes a half-updated set.
    """

    def __init__(self) -> None:
        """Create a set of counters, all at zero."""
        self._lock = threading.Lock()
        self._snapshot = CounterSnapshot()

    def snapshot(self) -> CounterSnapshot:
        """Return a consistent read of every counter."""
        with self._lock:
            return self._snapshot

    def record_accepted(self) -> None:
        """Count one ``accepted`` publish outcome."""
        self._bump(accepted=1)

    def record_released(self) -> None:
        """Count one ``released`` publish outcome."""
        self._bump(released=1)

    def record_rejected(self) -> None:
        """Count one ``rejected`` publish outcome."""
        self._bump(rejected=1)

    def record_failed(self) -> None:
        """Count one publish attempt whose call raised instead of returning."""
        self._bump(failed=1)

    def record_received(self) -> None:
        """Count one delivery reaching the message handler."""
        self._bump(received=1)

    def _bump(self, **deltas: int) -> None:
        """Add ``deltas`` to the current snapshot, replacing it under the lock."""
        with self._lock:
            current = self._snapshot
            self._snapshot = CounterSnapshot(
                accepted=current.accepted + deltas.get("accepted", 0),
                released=current.released + deltas.get("released", 0),
                rejected=current.rejected + deltas.get("rejected", 0),
                failed=current.failed + deltas.get("failed", 0),
                received=current.received + deltas.get("received", 0),
            )


class ConnectionFailureReporter:
    """Logs a permanent connection loss and tells the rest of the run to stop.

    Wired onto ``ConnectionParameters.on_unexpected_close``, which only fires
    once ``RecoveryConfiguration``'s back-off policy has given up — at that
    point the connection is dead for good, so :attr:`stop_event` is set to cut
    the publish loop and the interactive/silent wait short instead of leaving
    them running against a connection that can never do anything again.
    """

    def __init__(self, stop_event: threading.Event) -> None:
        """Create a reporter that sets ``stop_event`` when it fires."""
        self._stop_event = stop_event

    def __call__(self, error: BaseException | None) -> None:
        """Log the closure once, at error level, and request a stop."""
        logger.error("%s [connection] closed permanently: %s", TAG, error)
        self._stop_event.set()


class StateWatcher:
    """Logs every connection state transition on its own thread.

    The client has no separate "status changed" notification channel (unlike
    the Go client's ``NotifyStatusChange``), so this polls
    :attr:`~rabbitmq_amqp_python_client.Connection.state` instead — cheap
    enough at this interval, and it never mutates anything the run depends on.
    """

    def __init__(self, connection: Connection, poll_interval: float = STATE_POLL_INTERVAL_SECONDS) -> None:
        """Create a watcher for ``connection``; :meth:`start` puts it to work."""
        self._connection = connection
        self._poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_state = connection.state

    def start(self) -> None:
        """Start watching; a second call while running does nothing."""
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="reliable-example-state-watcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching and wait for the thread to end."""
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(JOIN_TIMEOUT_SECONDS)

    def _run(self) -> None:
        while not self._stop.wait(self._poll_interval):
            current = self._connection.state
            if current is self._last_state:
                continue
            logger.info("%s [connection] status changed: %s -> %s", TAG, self._last_state.value, current.value)
            if current is ConnectionState.RECONNECTING:
                logger.info("%s [connection] reconnecting to the AMQP 1.0 server", TAG)
            self._last_state = current


class StatsPrinter:
    """Logs periodic stats every ``interval`` seconds, on its own thread."""

    def __init__(self, counters: Counters, interval: float = STATS_INTERVAL_SECONDS) -> None:
        """Create a printer for ``counters``; :meth:`start` puts it to work."""
        self._counters = counters
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at = 0.0

    def start(self) -> None:
        """Start ticking; a second call while running does nothing."""
        if self._thread is not None:
            return
        self._started_at = time.monotonic()
        self._thread = threading.Thread(target=self._run, name="reliable-example-stats", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop ticking and wait for the thread to end."""
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(JOIN_TIMEOUT_SECONDS)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self._print_tick()

    def _print_tick(self) -> None:
        snapshot = self._counters.snapshot()
        elapsed = max(time.monotonic() - self._started_at, 1e-9)
        rate = snapshot.settled / elapsed
        logger.info(
            "%s [Stats] sent=%d received=%d failed=%d messages/sec=%.2f",
            TAG,
            snapshot.settled,
            snapshot.received,
            snapshot.failed,
            rate,
        )


def build_message(index: int) -> Message:
    """Build the message published at ``index``, with the same shape as the Go example's."""
    return Message(
        f"{TAG} message id{index}",
        application_properties=ApplicationProperties(
            value={
                "message-id": index,
                "timestamp": int(time.time() * 1000),
                "from": "python-amqp1.0",
            }
        ),
    )


def validate_delivery(message: Message) -> bool:
    """Whether ``message`` has the body and application properties :func:`build_message` sets."""
    body = message.body_as_string()
    if "message id" not in body:
        return False
    properties = message.application_properties.value if message.application_properties is not None else {}
    return "message-id" in properties and "timestamp" in properties and "from" in properties


def classify_outcome(outcome: Outcome, counters: Counters) -> None:
    """Count one publish outcome in exactly one bucket.

    Never raises: an outcome state this client does not model cannot reach
    here, since ``OutcomeState`` has exactly the three members checked below.
    """
    if outcome.state is OutcomeState.ACCEPTED:
        counters.record_accepted()
    elif outcome.state is OutcomeState.RELEASED:
        counters.record_released()
    elif outcome.state is OutcomeState.REJECTED:
        counters.record_rejected()
    else:  # pragma: no cover - OutcomeState has exactly the three members above
        logger.warning("%s counting an unmodelled outcome state %r as a failure", TAG, outcome.state)
        counters.record_failed()


def build_consumer(connection: Connection, options: Options, counters: Counters) -> Consumer:
    """Attach the consumer that validates and counts every delivery.

    Built before the publisher, so nothing this run publishes can arrive
    before something is already listening. Unlike the Go example, there is no
    manual receive loop to re-block on a network error: the client re-attaches
    the consumer's link on its own once the connection recovers, and this
    callback simply keeps being invoked afterwards.
    """

    def on_delivery(context: Context, message: Message) -> None:
        if not validate_delivery(message):
            logger.error("%s [Consumer] received a malformed delivery on %r", TAG, options.queue_name)
        counters.record_received()
        try:
            context.accept()
        except AMQPError as error:  # the delivery still happened, so it still counts
            logger.warning("%s [Consumer] could not accept a delivery: %s", TAG, error)

    return connection.consumer_builder().queue(options.queue_name).message_handler(on_delivery).build()


def _wait_for_open(connection: Connection, stop_event: threading.Event) -> None:
    """Block until ``connection`` is open again, or ``stop_event`` is set.

    The client's own ``RecoveryConfiguration`` already paces the redial
    attempts, so this only needs to poll its outcome — no extra backoff of our
    own is needed here.
    """
    while not stop_event.is_set() and connection.state is not ConnectionState.OPEN:
        stop_event.wait(STATE_POLL_INTERVAL_SECONDS)


def publish_all(
    connection: Connection,
    publisher: Publisher,
    options: Options,
    counters: Counters,
    stop_event: threading.Event,
) -> None:
    """Publish ``options.messages_to_send`` messages, classifying every outcome.

    A publish that raises is logged and counted as failed, then this waits for
    the connection to come back before moving on to the next message — mirroring
    the Go example's ``signalBlock``, minus the manual bookkeeping that needs,
    since this client's publisher keeps working on its own once the connection
    recovers.
    """
    for index in range(options.messages_to_send):
        if stop_event.is_set():
            logger.info(
                "%s [Publisher] stopping, queue %r (%d/%d attempted)",
                TAG,
                options.queue_name,
                index,
                options.messages_to_send,
            )
            return
        try:
            result = publisher.publish(build_message(index), timeout=PUBLISH_TIMEOUT_SECONDS)
        except AMQPError as error:  # AMQPTimeoutError/PublisherError and anything else the link raises
            counters.record_failed()
            logger.info("%s [Publisher] blocked, queue %r: %s", TAG, options.queue_name, error)
            _wait_for_open(connection, stop_event)
            logger.info("%s [Publisher] unblocked, queue %r", TAG, options.queue_name)
            continue
        classify_outcome(result.outcome, counters)
        if options.delay_message:
            time.sleep(DELAY_MESSAGE_SECONDS)
    logger.info("%s [Publisher] finished, queue %r (%d attempted)", TAG, options.queue_name, options.messages_to_send)


def _best_effort(description: str, step: object) -> None:
    """Run one teardown step, logging whatever it raises rather than propagating it."""
    try:
        step()  # type: ignore[operator]
    except Exception as error:  # noqa: BLE001 - teardown continues whatever one step does
        logger.warning("%s ignoring an error while %s: %s", TAG, description, error)


def tear_down(
    connection: Connection,
    consumer: Consumer | None,
    publisher: Publisher | None,
    options: Options,
) -> None:
    """Close everything this run opened, in the Go example's own order.

    Every step runs even when an earlier one failed, and every failure is
    logged rather than raised.
    """
    if consumer is not None:
        _best_effort("closing the consumer", consumer.close)
    if publisher is not None:
        _best_effort("closing the publisher", publisher.close)
    management = connection.management()

    def _purge_and_delete() -> None:
        purged = management.queue(options.queue_name).purge()
        logger.info("%s purged %d message(s) from %r", TAG, purged, options.queue_name)
        management.queue(options.queue_name).delete()

    _best_effort(f"purging and deleting {options.queue_name!r}", _purge_and_delete)
    _best_effort("closing the connection", connection.close)
    logger.info("%s AMQP connection closed", TAG)


def run(options: Options) -> int:
    """Run the whole reliable publish/consume demo and return a process exit code."""
    counters = Counters()
    stop_event = threading.Event()
    reporter = ConnectionFailureReporter(stop_event)
    try:
        connection = Connection(
            ConnectionParameters(
                container_id=f"reliable-amqp10-python-{uuid.uuid4().hex[:8]}",
                host=options.host,
                port=options.port,
                user=options.user,
                password=options.password,
                virtual_host=options.virtual_host,
                tls=ssl.create_default_context() if options.tls else None,
                on_unexpected_close=reporter,
                recovery_configuration=RecoveryConfiguration(),  # activated=True, topology=False
            )
        )
    except (AMQPError, OSError) as error:
        logger.error("%s could not connect to %s:%s: %s", TAG, options.host, options.port or "default", error)
        return EXIT_SETUP_FAILED
    logger.info("%s AMQP connection opened", TAG)

    watcher = StateWatcher(connection)
    watcher.start()
    stats = StatsPrinter(counters)
    stats.start()

    consumer: Consumer | None = None
    publisher: Publisher | None = None
    try:
        try:
            info = connection.management().queue(options.queue_name).quorum().queue().declare()
            logger.info("%s declared quorum queue %r", TAG, info.name)
            consumer = build_consumer(connection, options, counters)
            publisher = connection.publisher_builder().queue(options.queue_name).build()
        except AMQPError as error:
            logger.error("%s could not declare %r or attach a link: %s", TAG, options.queue_name, error)
            return EXIT_SETUP_FAILED

        publish_thread = threading.Thread(
            target=publish_all,
            args=(connection, publisher, options, counters, stop_event),
            name="reliable-example-publisher",
            daemon=True,
        )
        publish_thread.start()

        if options.is_silent:
            logger.info("%s IS_SILENT is set: running until stopped", TAG)
            while not stop_event.is_set():
                time.sleep(1.0)
        else:
            print("press any key to close the connection")
            with contextlib.suppress(EOFError):  # no interactive stdin (e.g. piped input) closes right away
                input()
            stop_event.set()

        publish_thread.join(JOIN_TIMEOUT_SECONDS)
        return EXIT_OK
    finally:
        stats.stop()
        watcher.stop()
        tear_down(connection, consumer, publisher, options)


def main() -> int:
    """Read the configuration, run the demo, and return the process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    try:
        options = read_options()
    except ConfigurationError as error:
        logger.error("%s", error)
        return EXIT_INVALID_CONFIGURATION
    try:
        return run(options)
    except KeyboardInterrupt:
        logger.warning("%s interrupted", TAG)
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
