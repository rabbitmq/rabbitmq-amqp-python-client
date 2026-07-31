"""Auto-reconnection against a live RabbitMQ broker (step_040 §8).

The unexpected closure every test needs is forced from *outside* the client, with
the management HTTP API's ``DELETE /api/connections/{name}`` (§8.1) — the broker
really does drop the connection, so the frame reader detects exactly what it
would detect for a network partition or a killed node.

One deviation from §8.1: RabbitMQ 4.x does **not** register an AMQP 1.0
connection under its ``container-id``, so ``DELETE /api/connections/<container
id>`` answers ``404``. ``GET /api/connections`` does report ``container_id`` as a
field, though, so :func:`force_close` looks the connection up by that field and
deletes it by the ``name`` the broker assigned it. Matching on ``container_id``
keeps the test targeting the exact connection it opened, which matching on peer
address/port could not do reliably from inside a container-NAT setup. The
listing lags the connection by a few seconds, hence the polling.
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time
import urllib.error
import urllib.parse
import uuid

import pytest

from src import (
    AMQPError,
    Connection,
    ConnectionParameters,
    ConnectionState,
    Context,
    ManagementError,
    RecoveryConfiguration,
)
from src.wire import Message

pytestmark = pytest.mark.integration

#: How long to wait for the broker's connection listing to catch up.
LISTING_TIMEOUT_SECONDS = 30.0

#: How long to wait for a whole redial-and-re-attach cycle.
RECOVERY_TIMEOUT_SECONDS = 60.0

POLL_INTERVAL_SECONDS = 0.05
LISTING_POLL_INTERVAL_SECONDS = 0.5


class SlowPolicy:
    """A back-off policy with one long, predictable delay.

    Widens the ``RECONNECTING`` window enough for a test to observe it and to
    make a call inside it, without depending on how fast the broker answers.
    """

    def __init__(self, delay: float = 3.0, max_attempts: int = 10):
        self.delay = delay
        self.max_attempts = max_attempts
        self._attempt = 0

    @property
    def current_attempt(self) -> int:
        return self._attempt

    def next_delay(self) -> float:
        self._attempt += 1
        return self.delay

    def reset(self) -> None:
        self._attempt = 0

    def is_active(self) -> bool:
        return self._attempt <= self.max_attempts


def unique(prefix: str) -> str:
    """Return a name unique to this test run."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def closed_port() -> int:
    """Return a local port nothing is listening on, so a dial there is refused."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def find_connection(management_api, container_id: str, timeout: float = LISTING_TIMEOUT_SECONDS) -> dict:
    """Return the broker's listing entry for ``container_id``.

    Raises:
        AssertionError: If the broker never lists it within ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for entry in management_api("GET", "/api/connections") or []:
            if entry.get("container_id") == container_id:
                return entry
        time.sleep(LISTING_POLL_INTERVAL_SECONDS)
    raise AssertionError(f"the broker never listed a connection with container-id {container_id!r}")


def force_close(management_api, container_id: str) -> int:
    """Make the broker close the connection ``container_id`` opened (§8.1).

    Args:
        management_api: The HTTP API caller fixture.
        container_id: ``Open.container-id`` of the connection to kill.

    Returns:
        The ``connected_at`` of the connection that was killed, so a caller can
        tell the recovered connection apart from it.
    """
    entry = find_connection(management_api, container_id)
    name = urllib.parse.quote(str(entry["name"]), safe="")
    management_api("DELETE", f"/api/connections/{name}")
    return int(entry["connected_at"])


def wait_for_state(connection: Connection, state: ConnectionState, timeout: float) -> bool:
    """Whether ``connection`` is observed in ``state`` within ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if connection.state is state:
            return True
        time.sleep(POLL_INTERVAL_SECONDS)
    return False


def wait_for_recovery(management_api, connection: Connection, previous_connected_at: int) -> dict:
    """Wait until the connection is back up on a *new* transport (§8.2 steps 3 and 5).

    Waiting for ``OPEN`` alone is the pitfall §8.2 warns about: between the
    broker closing the socket and the frame reader noticing, ``State`` still
    reads the ``OPEN`` from before the drop. Requiring the broker to report a
    connection with a different ``connected_at`` makes the second ``OPEN``
    reading mean "recovered" rather than "never left".

    Returns:
        The broker's listing entry for the recovered connection.
    """
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if connection.state is ConnectionState.OPEN:
            for entry in management_api("GET", "/api/connections") or []:
                if (
                    entry.get("container_id") == connection.container_id
                    and int(entry["connected_at"]) != previous_connected_at
                ):
                    return entry
        time.sleep(LISTING_POLL_INTERVAL_SECONDS)
    raise AssertionError(f"the connection never came back: state is {connection.state}")


@pytest.fixture
def open_connection():
    """Return ``open(**overrides) -> Connection``, closing every one afterwards."""
    connections: list[Connection] = []

    def make(**overrides) -> Connection:
        overrides.setdefault("container_id", unique("test-auto-reconnect"))
        connection = Connection(ConnectionParameters(**overrides))
        connections.append(connection)
        return connection

    yield make

    for connection in connections:
        with contextlib.suppress(Exception):  # teardown must never mask a test failure
            connection.close()


@pytest.fixture
def delete_queues(management_api):
    """Return ``remember(name)``, deleting each remembered queue afterwards."""
    names: list[str] = []

    yield names.append

    for name in names:
        # An exclusive queue whose connection died is already gone.
        with contextlib.suppress(urllib.error.HTTPError):
            management_api("DELETE", f"/api/queues/%2F/{urllib.parse.quote(name, safe='')}")


class TestReconnectionSucceeds:
    def test_the_state_moves_open_reconnecting_open(self, management_api, open_connection):
        seen: list[BaseException | None] = []
        connection = open_connection(on_unexpected_close=seen.append, recovery_configuration=RecoveryConfiguration())
        assert connection.state is ConnectionState.OPEN

        connected_at = force_close(management_api, connection.container_id)
        # Tolerated: a fast detect-and-recover can land inside one poll interval.
        wait_for_state(connection, ConnectionState.RECONNECTING, timeout=5.0)
        entry = wait_for_recovery(management_api, connection, connected_at)

        assert connection.state is ConnectionState.OPEN
        assert int(entry["connected_at"]) != connected_at, "the client did not actually redial"
        assert seen == [], "on_unexpected_close must not fire when recovery succeeds"

    def test_the_reconnecting_state_is_observable(self, management_api, open_connection):
        connection = open_connection(recovery_configuration=RecoveryConfiguration(back_off_delay_policy=SlowPolicy()))
        connected_at = force_close(management_api, connection.container_id)
        assert wait_for_state(connection, ConnectionState.RECONNECTING, timeout=10.0)
        wait_for_recovery(management_api, connection, connected_at)

    def test_a_publisher_and_a_consumer_built_before_the_drop_keep_working(
        self, management_api, open_connection, delete_queues
    ):
        connection = open_connection(recovery_configuration=RecoveryConfiguration())
        queue = unique("reconnect-pubsub")
        delete_queues(queue)
        connection.management().queue(queue).declare()

        received: list[str] = []
        delivered = threading.Event()

        def handler(context: Context, message: Message) -> None:
            received.append(message.body_as_string())
            context.accept()
            delivered.set()

        publisher = connection.publisher_builder().queue(queue).build()
        consumer = connection.consumer_builder().queue(queue).message_handler(handler).build()
        publisher_id, consumer_id = publisher.id, consumer.id

        connected_at = force_close(management_api, connection.container_id)
        wait_for_recovery(management_api, connection, connected_at)

        # No caller-side recreation: the same objects must still be usable.
        result = publisher.publish(Message("after the drop"))
        assert result.outcome.state.value == "accepted"
        assert delivered.wait(15.0), "the re-attached consumer received nothing"
        assert received == ["after the drop"]
        assert (publisher.id, consumer.id) == (publisher_id, consumer_id)
        assert publisher.is_open and consumer.is_open

    def test_the_management_endpoint_is_usable_again(self, management_api, open_connection, delete_queues):
        connection = open_connection(recovery_configuration=RecoveryConfiguration())
        management = connection.management()
        connected_at = force_close(management_api, connection.container_id)
        wait_for_recovery(management_api, connection, connected_at)

        queue = unique("reconnect-management")
        delete_queues(queue)
        assert management.queue(queue).declare().name == queue
        assert connection.management() is management


class TestTopologyRecovery:
    def _declare_exclusive(self, connection: Connection, name: str) -> None:
        """Declare an exclusive queue, which the broker drops with its connection."""
        info = connection.management().queue(name).exclusive().declare()
        assert info.exclusive is True

    def test_an_exclusive_queue_is_recreated_when_topology_recovery_is_on(
        self, management_api, open_connection, delete_queues
    ):
        connection = open_connection(recovery_configuration=RecoveryConfiguration(topology=True))
        queue = unique("reconnect-exclusive-on")
        delete_queues(queue)
        self._declare_exclusive(connection, queue)

        connected_at = force_close(management_api, connection.container_id)
        wait_for_recovery(management_api, connection, connected_at)

        info = connection.management().queue_info(queue)
        assert info.name == queue, "topology recovery did not recreate the exclusive queue"

    def test_an_exclusive_queue_stays_gone_when_topology_recovery_is_off(
        self, management_api, open_connection, delete_queues
    ):
        connection = open_connection(recovery_configuration=RecoveryConfiguration(topology=False))
        queue = unique("reconnect-exclusive-off")
        delete_queues(queue)
        self._declare_exclusive(connection, queue)

        connected_at = force_close(management_api, connection.container_id)
        wait_for_recovery(management_api, connection, connected_at)

        with pytest.raises(ManagementError) as failure:
            connection.management().queue_info(queue)
        assert failure.value.status_code == 404


class TestCallsDuringTheGap:
    def test_a_publish_while_reconnecting_fails_with_the_usual_error(
        self, management_api, open_connection, delete_queues
    ):
        connection = open_connection(recovery_configuration=RecoveryConfiguration(back_off_delay_policy=SlowPolicy()))
        queue = unique("reconnect-gap")
        delete_queues(queue)
        connection.management().queue(queue).declare()
        publisher = connection.publisher_builder().queue(queue).build()

        connected_at = force_close(management_api, connection.container_id)
        assert wait_for_state(connection, ConnectionState.RECONNECTING, timeout=10.0)
        with pytest.raises(AMQPError):
            publisher.publish(Message("during the gap"), timeout=2.0)
        wait_for_recovery(management_api, connection, connected_at)

    def test_a_management_call_while_reconnecting_fails_with_the_usual_error(self, management_api, open_connection):
        connection = open_connection(recovery_configuration=RecoveryConfiguration(back_off_delay_policy=SlowPolicy()))
        management = connection.management()

        connected_at = force_close(management_api, connection.container_id)
        assert wait_for_state(connection, ConnectionState.RECONNECTING, timeout=10.0)
        with pytest.raises(AMQPError):
            management.queue_info("any-queue")
        wait_for_recovery(management_api, connection, connected_at)


class TestRecoveryDisabled:
    def test_the_callback_fires_at_once_and_the_state_never_reads_reconnecting(self, management_api, open_connection):
        fired = threading.Event()
        seen: list[BaseException | None] = []
        observed: list[ConnectionState] = []
        stop = threading.Event()

        def on_unexpected_close(error):
            seen.append(error)
            fired.set()

        connection = open_connection(
            on_unexpected_close=on_unexpected_close,
            recovery_configuration=RecoveryConfiguration(activated=False),
        )

        def sample():
            while not stop.is_set():
                observed.append(connection.state)
                time.sleep(0.001)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            force_close(management_api, connection.container_id)
            assert fired.wait(15.0), "on_unexpected_close never fired"
        finally:
            stop.set()
            sampler.join(5.0)

        assert connection.state is ConnectionState.CLOSED
        assert len(seen) == 1
        assert ConnectionState.RECONNECTING not in observed


class TestGiveUp:
    def test_a_redial_that_keeps_being_refused_ends_closed(self, management_api, open_connection):
        seen: list[BaseException | None] = []
        connection = open_connection(
            on_unexpected_close=seen.append,
            recovery_configuration=RecoveryConfiguration(back_off_delay_policy=SlowPolicy(delay=0.1, max_attempts=2)),
        )
        # The redial has to fail for the whole attempt budget, which only a dead
        # address guarantees; the parameters are re-read on every attempt, so
        # moving the port after the connection is up points just the redials at
        # a port nothing listens on.
        connection.parameters.port = closed_port()

        force_close(management_api, connection.container_id)
        assert wait_for_state(connection, ConnectionState.CLOSED, timeout=30.0)
        time.sleep(0.5)
        assert len(seen) == 1, "on_unexpected_close must fire exactly once when recovery gives up"
        connection.close()
        assert len(seen) == 1
