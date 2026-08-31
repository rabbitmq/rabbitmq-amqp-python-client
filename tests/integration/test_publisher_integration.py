"""Publishers against a live RabbitMQ broker.

Covers the outcomes only a real broker produces — ``accepted`` for a routed
message, ``released`` for an exchange with no bindings, and ``rejected`` with
structured details for a quorum queue whose ``x-overflow`` is
``reject-publish`` (step_070 §6). Consumers are a later step, so the one test
that has to prove a message really landed attaches a raw
:class:`~rabbitmq_amqp_python_client.ReceiverLink` itself.
"""

from __future__ import annotations

import contextlib
import time
import uuid

import pytest

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    ExchangeType,
    OutcomeState,
    OverflowStrategy,
    PublisherError,
    ReceiverLink,
    exchange_address,
    queue_address,
)
from rabbitmq_amqp_python_client.wire import Accepted, Message, Properties, Source

pytestmark = pytest.mark.integration

RECEIVE_TIMEOUT_SECONDS = 10.0
PUBLISH_TIMEOUT_SECONDS = 10.0

#: Length limit of the reject-publish quorum queue.
MAX_LENGTH = 5

#: Publishes and seconds allowed while waiting for the asynchronous quorum-queue
#: length accounting to start rejecting (step_070 §6 forbids assuming it happens
#: on exactly the ``MAX_LENGTH + 1``-th publish).
REJECT_MAX_ATTEMPTS = 50
REJECT_TIMEOUT_SECONDS = 30.0


def _name(prefix: str) -> str:
    """A unique name for one test's topology."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def connection():
    """An open connection to the local broker, closed on teardown."""
    opened = Connection(ConnectionParameters())
    try:
        yield opened
    finally:
        opened.close()


@pytest.fixture
def management(connection):
    """The connection's management endpoint."""
    return connection.management()


@pytest.fixture
def topology(management):
    """Declares queues and exchanges, deleting whatever it created afterwards."""

    class Registry:
        """Remembers the names it declared so teardown can remove them."""

        def __init__(self):
            self.queues = []
            self.exchanges = []

        def queue(self, name=""):
            """Declare a classic queue and return its reported info."""
            info = management.queue(name).declare()
            self.queues.append(info.name)
            return info

        def quorum_reject_publish_queue(self, name):
            """Declare a quorum queue that rejects publishes past ``MAX_LENGTH``."""
            info = (
                management.queue(name)
                .quorum()
                .queue()
                .max_length(MAX_LENGTH)
                .overflow_strategy(OverflowStrategy.REJECT_PUBLISH)
                .declare()
            )
            self.queues.append(info.name)
            return info

        def exchange(self, name, exchange_type=ExchangeType.DIRECT):
            """Declare an exchange and return its name."""
            management.exchange(name).type(exchange_type).declare()
            self.exchanges.append(name)
            return name

    registry = Registry()
    try:
        yield registry
    finally:
        for name in registry.queues:
            with contextlib.suppress(Exception):  # cleanup must not mask a failure
                management.queue(name).delete()
        for name in registry.exchanges:
            with contextlib.suppress(Exception):  # cleanup must not mask a failure
                management.exchange(name).delete()


@pytest.fixture
def publishers():
    """Collects publishers and closes them before the connection goes away."""
    created = []
    try:
        yield created
    finally:
        for publisher in created:
            with contextlib.suppress(Exception):  # cleanup must not mask a failure
                publisher.close()


@pytest.fixture
def broker_supports_rejection_details(management_api):
    """Whether the broker is new enough to send structured rejection details."""
    overview = management_api("GET", "/api/overview")
    version = str(overview.get("rabbitmq_version", "0"))
    parts = version.split(".")
    try:
        major, minor = int(parts[0]), int(parts[1])
    except (IndexError, ValueError):
        return False
    return (major, minor) >= (4, 3)


def _receive_one(connection, address):
    """Attach a receiver to ``address``, take one message, and detach."""
    session = connection.open_session()
    receiver = ReceiverLink()
    receiver.attach(session, source=Source(address=address))
    try:
        receiver.flow(1)
        delivery = receiver.receive(timeout=RECEIVE_TIMEOUT_SECONDS)
        if delivery is not None and not delivery.settled:
            receiver.settle(delivery.delivery_id, Accepted())
        return delivery
    finally:
        receiver.detach()
        session.end()


class TestPublishToQueue:
    def test_a_queue_publish_is_accepted(self, connection, topology, publishers):
        info = topology.queue(_name("pub-it"))
        publisher = connection.publisher_builder().queue(info.name).build()
        publishers.append(publisher)
        result = publisher.publish(Message("hello"), timeout=PUBLISH_TIMEOUT_SECONDS)
        assert result.outcome.state is OutcomeState.ACCEPTED
        assert result.outcome.rejection_details is None
        assert result.outcome.error is None

    def test_a_queue_specification_can_be_handed_to_the_builder(self, connection, management, topology, publishers):
        info = topology.queue(_name("pub-it-spec"))
        specification = management.queue(info.name)
        publisher = connection.publisher_builder().queue(specification).build()
        publishers.append(publisher)
        assert publisher.address == queue_address(info.name)
        assert publisher.publish(Message("hello")).outcome.state is OutcomeState.ACCEPTED

    def test_a_publish_to_a_missing_queue_is_refused_at_attach(self, connection):
        with pytest.raises(PublisherError):
            connection.publisher_builder().queue(_name("absent")).build()

    def test_many_publishes_share_one_link(self, connection, management, topology, publishers):
        info = topology.queue(_name("pub-it-many"))
        publisher = connection.publisher_builder().queue(info.name).build()
        publishers.append(publisher)
        outcomes = [publisher.publish(Message(f"m-{index}")).outcome.state for index in range(10)]
        assert outcomes == [OutcomeState.ACCEPTED] * 10
        assert management.queue_info(info.name).message_count == 10


class TestPublishToExchange:
    def test_an_unroutable_exchange_publish_is_released(self, connection, topology, publishers):
        exchange = topology.exchange(_name("pub-it-unbound"))
        publisher = connection.publisher_builder().exchange(exchange).build()
        publishers.append(publisher)
        result = publisher.publish(Message("nowhere"), timeout=PUBLISH_TIMEOUT_SECONDS)
        assert result.outcome.state is OutcomeState.RELEASED
        assert result.outcome.rejection_details is None

    def test_a_bound_routing_key_publish_is_accepted_and_lands(self, connection, management, topology, publishers):
        exchange = topology.exchange(_name("pub-it-ex"), ExchangeType.TOPIC)
        info = topology.queue(_name("pub-it-bound"))
        management.bind(source=exchange, destination=info.name, binding_key="order.created")
        publisher = connection.publisher_builder().exchange(exchange).key("order.created").build()
        publishers.append(publisher)
        assert publisher.address == exchange_address(exchange, "order.created")
        result = publisher.publish(Message("routed"), timeout=PUBLISH_TIMEOUT_SECONDS)
        assert result.outcome.state is OutcomeState.ACCEPTED
        delivery = _receive_one(connection, queue_address(info.name))
        assert delivery is not None
        assert delivery.message.body_as_string() == "routed"


class TestAnonymousPublisher:
    def test_one_publisher_addresses_several_destinations(self, connection, management, topology, publishers):
        first = topology.queue(_name("pub-it-anon-a"))
        second = topology.queue(_name("pub-it-anon-b"))
        exchange = topology.exchange(_name("pub-it-anon-ex"))
        management.bind(source=exchange, destination=second.name, binding_key="key")
        publisher = connection.publisher_builder().build()
        publishers.append(publisher)
        assert publisher.is_anonymous

        to_queue = publisher.publish(
            Message("direct", properties=Properties(to=queue_address(first.name))),
            timeout=PUBLISH_TIMEOUT_SECONDS,
        )
        to_exchange = publisher.publish(
            Message("routed", properties=Properties(to=exchange_address(exchange, "key"))),
            timeout=PUBLISH_TIMEOUT_SECONDS,
        )
        assert to_queue.outcome.state is OutcomeState.ACCEPTED
        assert to_exchange.outcome.state is OutcomeState.ACCEPTED
        assert _receive_one(connection, queue_address(first.name)).message.body_as_string() == "direct"
        assert _receive_one(connection, queue_address(second.name)).message.body_as_string() == "routed"

    def test_a_message_without_a_destination_is_refused(self, connection, publishers):
        publisher = connection.publisher_builder().build()
        publishers.append(publisher)
        with pytest.raises(PublisherError, match="properties.to"):
            publisher.publish(Message("hello"))


class TestRejectionDetails:
    """step_070 §6: a quorum queue at its length limit rejects with metadata."""

    def test_a_reject_publish_queue_rejects_with_details(
        self, connection, topology, publishers, broker_supports_rejection_details
    ):
        info = topology.quorum_reject_publish_queue(_name("pub-it-reject"))
        publisher = connection.publisher_builder().queue(info.name).build()
        publishers.append(publisher)

        deadline = time.monotonic() + REJECT_TIMEOUT_SECONDS
        rejected = None
        attempts = 0
        while rejected is None and attempts < REJECT_MAX_ATTEMPTS and time.monotonic() < deadline:
            attempts += 1
            result = publisher.publish(Message(f"m-{attempts}"), timeout=PUBLISH_TIMEOUT_SECONDS)
            if result.outcome.state is OutcomeState.REJECTED:
                rejected = result.outcome
            else:
                assert result.outcome.state is OutcomeState.ACCEPTED
                assert result.outcome.rejection_details is None
        assert rejected is not None, f"no rejection after {attempts} publishes"

        if not broker_supports_rejection_details:
            assert rejected.rejection_details is None
            return
        details = rejected.rejection_details
        assert details is not None, "RabbitMQ 4.3+ was expected to send structured rejection details"
        assert details.rejected_by_queue == info.name
        assert details.reason

    def test_an_accepted_publish_carries_no_details(self, connection, topology, publishers):
        info = topology.quorum_reject_publish_queue(_name("pub-it-under"))
        publisher = connection.publisher_builder().queue(info.name).build()
        publishers.append(publisher)
        result = publisher.publish(Message("under the limit"), timeout=PUBLISH_TIMEOUT_SECONDS)
        assert result.outcome.state is OutcomeState.ACCEPTED
        assert result.outcome.rejection_details is None


class TestLifecycle:
    def test_publishers_share_the_pub_sub_session(self, connection, topology, publishers):
        first_queue = topology.queue(_name("pub-it-share-a"))
        second_queue = topology.queue(_name("pub-it-share-b"))
        first = connection.publisher_builder().queue(first_queue.name).build()
        second = connection.publisher_builder().queue(second_queue.name).build()
        publishers.extend((first, second))
        assert first._session is second._session
        assert first.id != second.id

    def test_close_leaves_the_session_usable_for_a_new_publisher(self, connection, topology, publishers):
        info = topology.queue(_name("pub-it-reopen"))
        first = connection.publisher_builder().queue(info.name).build()
        session = first._session
        first.close()
        assert not first.is_open
        second = connection.publisher_builder().queue(info.name).build()
        publishers.append(second)
        assert second._session is session
        assert second.publish(Message("after close")).outcome.state is OutcomeState.ACCEPTED

    def test_publishing_after_close_is_refused(self, connection, topology):
        info = topology.queue(_name("pub-it-closed"))
        publisher = connection.publisher_builder().queue(info.name).build()
        publisher.close()
        with pytest.raises(PublisherError, match="closed"):
            publisher.publish(Message("hello"))
