"""The management API against a live RabbitMQ broker on localhost.

Every test drives the real link pair to ``/management``: it declares topology,
reads it back, and cleans it up afterwards. Anything the broker rejects locally
(percent encoding, argument types, expected status codes) shows up here rather
than in the unit suite.
"""

from __future__ import annotations

import contextlib
import uuid

import pytest

from src import (
    ClassicQueueMode,
    ClassicQueueVersion,
    Connection,
    ConnectionParameters,
    ExchangeType,
    LeaderLocatorStrategy,
    ManagementError,
    OverflowStrategy,
    QueueType,
    QuorumQueueDeadLetterStrategy,
    QuorumQueueDelayedRetryType,
    ReceiverLink,
    SenderLink,
)
from src.constants import EXCHANGE_ADDRESS_WITH_KEY_TEMPLATE
from src.management import (
    STATUS_BAD_REQUEST,
    STATUS_CONFLICT,
    STATUS_NOT_FOUND,
    encode_path_segment,
    queue_path,
)
from src.wire import Accepted, Message, Source, Target

pytestmark = pytest.mark.integration

RECEIVE_TIMEOUT_SECONDS = 10.0
SETTLE_TIMEOUT_SECONDS = 10.0


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
    """Builders that register whatever they declare, for deletion afterwards."""

    class Registry:
        """Hands out builders and remembers what they were asked to create."""

        def __init__(self):
            self.queues = []
            self.exchanges = []

        def queue(self, name=""):
            """Return a queue builder whose queue is deleted after the test."""
            specification = management.queue(name)
            self.queues.append(specification)
            return specification

        def exchange(self, name=""):
            """Return an exchange builder whose exchange is deleted after the test."""
            self.exchanges.append(name)
            return management.exchange(name)

    registry = Registry()
    yield registry
    for specification in registry.queues:
        if specification.queue_name:
            with contextlib.suppress(Exception):  # cleanup must not mask a test failure
                management.queue(specification.queue_name).delete()
    for name in registry.exchanges:
        if name:
            with contextlib.suppress(Exception):
                management.exchange(name).delete()


def _publish(connection, address, count):
    """Publish ``count`` accepted messages to ``address`` on a session of its own."""
    session = connection.open_session()
    sender = SenderLink()
    sender.attach(session, target=Target(address=address))
    try:
        for index in range(count):
            tag = f"tag-{index}".encode()
            pending = sender.register_pending(tag)
            sender.send_transfer(tag, Message(f"message-{index}"))
            assert isinstance(pending.wait(timeout=SETTLE_TIMEOUT_SECONDS), Accepted)
    finally:
        sender.detach()
        session.end()


def _receive_one(connection, address):
    """Receive and accept one message from ``address``, or return ``None``."""
    session = connection.open_session()
    receiver = ReceiverLink()
    receiver.attach(session, source=Source(address=address))
    try:
        receiver.flow(10)
        delivery = receiver.receive(timeout=RECEIVE_TIMEOUT_SECONDS)
        if delivery is not None:
            receiver.settle(delivery.delivery_id, Accepted())
        return delivery
    finally:
        receiver.detach()
        session.end()


class TestLinkPair:
    def test_management_is_open_and_shared_per_connection(self, connection):
        management = connection.management()
        assert management.is_open
        assert connection.management() is management

    def test_management_survives_many_sequential_requests(self, topology):
        specification = topology.queue(_name("mgmt-many"))
        specification.declare()
        for _ in range(20):
            assert specification.purge() == 0

    def test_closing_the_connection_closes_the_pair(self, topology):
        # A second connection, so closing it cannot disturb the fixture's own
        # management endpoint, which still has to clean the queue up afterwards.
        own = Connection(ConnectionParameters())
        management = own.management()
        name = topology.queue(_name("mgmt-close")).queue_name
        assert management.queue(name).declare().name == name
        own.close()
        assert not management.is_open
        with pytest.raises(ManagementError, match="not open"):
            management.queue_info(name)


class TestClassicQueue:
    def test_declare_reports_the_queue_state(self, topology):
        name = _name("classic")
        info = topology.queue(name).classic().queue().declare()
        assert info.name == name
        assert info.queue_type is QueueType.CLASSIC
        assert info.durable is True
        assert info.message_count == 0
        assert info.consumer_count == 0

    def test_declare_defaults_to_a_classic_queue(self, topology):
        info = topology.queue(_name("default")).declare()
        assert info.queue_type is QueueType.CLASSIC

    def test_declare_is_idempotent(self, topology):
        specification = topology.queue(_name("idempotent"))
        first = specification.declare()
        assert specification.declare() == first

    def test_declare_honours_exclusive_and_auto_delete(self, topology):
        info = topology.queue(_name("exclusive")).classic().queue().exclusive().auto_delete().declare()
        assert info.exclusive is True
        assert info.auto_delete is True

    def test_declare_accepts_the_classic_only_arguments(self, topology):
        specification = topology.queue(_name("classic-args"))
        specification.classic().max_priority(5).mode(ClassicQueueMode.DEFAULT).version(ClassicQueueVersion.V2)
        info = specification.declare()
        assert info.arguments["x-max-priority"] == 5
        assert info.arguments["x-queue-version"] == 2

    def test_declare_accepts_the_generic_numeric_arguments(self, topology):
        specification = (
            topology.queue(_name("generic-args"))
            .max_length(1000)
            .max_length_bytes(1_000_000)
            .message_ttl(600_000)
            .expires(3_600_000)
            .overflow_strategy(OverflowStrategy.REJECT_PUBLISH)
            .leader_locator(LeaderLocatorStrategy.BALANCED)
        )
        info = specification.declare()
        assert info.arguments["x-max-length"] == 1000
        assert info.arguments["x-max-length-bytes"] == 1_000_000
        assert info.arguments["x-message-ttl"] == 600_000
        assert info.arguments["x-expires"] == 3_600_000
        assert info.arguments["x-overflow"] == "reject-publish"

    def test_declare_accepts_a_dead_letter_configuration(self, topology):
        exchange = _name("dlx")
        topology.exchange(exchange).declare()
        specification = topology.queue(_name("dead-lettered"))
        specification.dead_letter_exchange(exchange).dead_letter_routing_key("dead")
        info = specification.declare()
        assert info.arguments["x-dead-letter-exchange"] == exchange
        assert info.arguments["x-dead-letter-routing-key"] == "dead"


class TestQuorumQueue:
    def test_declare_reports_a_leader_and_replicas(self, topology):
        info = topology.queue(_name("quorum")).quorum().queue().declare()
        assert info.queue_type is QueueType.QUORUM
        assert info.leader != ""
        assert info.replicas != ()

    def test_declare_forces_exclusive_and_auto_delete_off(self, topology):
        specification = topology.queue(_name("quorum-forced")).exclusive().auto_delete()
        info = specification.quorum().queue().declare()
        assert info.exclusive is False
        assert info.auto_delete is False
        assert specification.is_exclusive is False
        assert specification.is_auto_delete is False

    def test_declare_accepts_the_quorum_only_arguments(self, topology):
        specification = topology.queue(_name("quorum-args"))
        (
            specification.quorum()
            .delivery_limit(5)
            .dead_letter_strategy(QuorumQueueDeadLetterStrategy.AT_LEAST_ONCE)
            .quorum_initial_group_size(1)
            .quorum_target_group_size(1)
        )
        info = specification.declare()
        assert info.arguments["x-delivery-limit"] == 5
        assert info.arguments["x-dead-letter-strategy"] == "at-least-once"

    def test_declare_accepts_single_active_consumer(self, topology):
        specification = topology.queue(_name("quorum-sac")).single_active_consumer()
        info = specification.quorum().queue().declare()
        assert info.arguments["x-single-active-consumer"] is True

    def test_declare_accepts_delayed_retry(self, topology):
        specification = topology.queue(_name("quorum-retry"))
        (
            specification.quorum()
            .delayed_retry_type(QuorumQueueDelayedRetryType.FAILED)
            .delayed_retry_min(1_000)
            .delayed_retry_max(60_000)
        )
        try:
            info = specification.declare()
        except ManagementError as error:
            if error.status_code == STATUS_BAD_REQUEST:
                pytest.skip("this broker does not support x-delayed-retry-* (needs RabbitMQ 4.3+)")
            raise
        assert info.arguments["x-delayed-retry-type"] == "failed"
        assert info.arguments["x-delayed-retry-min"] == 1_000


class TestStreamQueue:
    def test_declare_reports_a_stream(self, topology):
        info = topology.queue(_name("stream")).stream().queue().declare()
        assert info.queue_type is QueueType.STREAM
        assert info.durable is True

    def test_declare_forces_exclusive_and_auto_delete_off(self, topology):
        specification = topology.queue(_name("stream-forced")).exclusive().auto_delete()
        info = specification.stream().queue().declare()
        assert info.exclusive is False
        assert info.auto_delete is False

    def test_declare_accepts_the_stream_only_arguments(self, topology):
        specification = topology.queue(_name("stream-args"))
        specification.stream().max_age(3600).max_segment_size_bytes(500_000).initial_cluster_size(1)
        info = specification.declare()
        assert info.arguments["x-max-age"] == "3600s"
        assert info.arguments["x-stream-max-segment-size-bytes"] == 500_000
        assert info.arguments["x-initial-cluster-size"] == 1


class TestGeneratedAndEncodedNames:
    def test_declare_without_a_name_generates_one(self, topology):
        specification = topology.queue()
        info = specification.declare()
        assert info.name.startswith("client.gen-")
        assert specification.queue_name == info.name
        assert topology.queue(info.name).purge() == 0

    def test_a_name_needing_percent_encoding_resolves(self, topology):
        name = f"{_name('needs encoding')} /+%~"
        info = topology.queue(name).declare()
        assert info.name == name
        # Every follow-up operation has to encode the same way to reach it.
        assert topology.queue(name).purge() == 0

    def test_a_non_ascii_name_resolves(self, topology):
        name = _name("café-Ünicode-名前")
        assert topology.queue(name).declare().name == name


class TestQueueInfo:
    def test_reads_back_a_declared_queue(self, topology, management):
        name = _name("get")
        declared = topology.queue(name).quorum().queue().declare()
        fetched = management.queue_info(name)
        assert fetched.name == declared.name
        assert fetched.queue_type is QueueType.QUORUM

    def test_a_name_needing_percent_encoding_resolves(self, topology, management):
        name = _name("get needs encoding")
        topology.queue(name).declare()
        assert management.queue_info(name).name == name

    def test_reports_404_for_a_queue_that_does_not_exist(self, management):
        with pytest.raises(ManagementError, match="not found") as failure:
            management.queue_info(_name("absent"))
        assert failure.value.status_code == STATUS_NOT_FOUND


class TestPurge:
    def test_reports_the_number_of_discarded_messages(self, connection, topology, management):
        name = _name("purge")
        topology.queue(name).declare()
        _publish(connection, queue_path(name), 5)
        assert topology.queue(name).purge() == 5
        assert management.queue_info(name).message_count == 0

    def test_reports_zero_for_an_empty_queue(self, topology):
        name = _name("purge-empty")
        topology.queue(name).declare()
        assert topology.queue(name).purge() == 0

    def test_reports_404_for_a_queue_that_does_not_exist(self, management):
        with pytest.raises(ManagementError, match="not found") as failure:
            management.queue(_name("absent")).purge()
        assert failure.value.status_code == STATUS_NOT_FOUND


class TestDelete:
    def test_returns_a_name_only_stub_and_removes_the_queue(self, topology, management):
        name = _name("delete")
        topology.queue(name).declare()
        stub = management.queue(name).delete()
        assert stub.name == name
        with pytest.raises(ManagementError, match="not found"):
            management.queue_info(name)

    def test_is_idempotent_for_a_queue_that_does_not_exist(self, management):
        # Unlike purge, the broker answers 200 for deleting an absent queue, so
        # the expected-code set accepts it and no error surfaces.
        name = _name("absent")
        assert management.queue(name).delete().name == name

    def test_deleting_twice_is_not_an_error(self, topology, management):
        name = _name("delete-twice")
        topology.queue(name).declare()
        management.queue(name).delete()
        management.queue(name).delete()


class TestConflict:
    def test_redeclaring_a_queue_with_another_type_is_a_conflict(self, topology, management):
        name = _name("conflict")
        topology.queue(name).classic().queue().declare()
        with pytest.raises(ManagementError, match="precondition failed") as failure:
            management.queue(name).quorum().queue().declare()
        assert failure.value.status_code == STATUS_CONFLICT

    def test_redeclaring_a_queue_with_other_arguments_is_a_conflict(self, topology, management):
        name = _name("conflict-args")
        topology.queue(name).max_length(100).declare()
        with pytest.raises(ManagementError) as failure:
            management.queue(name).max_length(200).declare()
        assert failure.value.status_code == STATUS_CONFLICT

    def test_redeclaring_an_exchange_with_another_type_is_a_conflict(self, topology, management):
        name = _name("conflict-exchange")
        topology.exchange(name).type(ExchangeType.DIRECT).declare()
        with pytest.raises(ManagementError, match="precondition failed") as failure:
            management.exchange(name).type(ExchangeType.TOPIC).declare()
        assert failure.value.status_code == STATUS_CONFLICT


class TestExchange:
    @pytest.mark.parametrize(
        "exchange_type",
        [ExchangeType.DIRECT, ExchangeType.FANOUT, ExchangeType.TOPIC, ExchangeType.HEADERS],
    )
    def test_declares_every_built_in_type(self, topology, exchange_type):
        assert topology.exchange(_name(f"ex-{exchange_type.value}")).type(exchange_type).declare() is None

    def test_declare_is_idempotent(self, topology):
        name = _name("ex-idempotent")
        topology.exchange(name).type(ExchangeType.TOPIC).declare()
        topology.exchange(name).type(ExchangeType.TOPIC).declare()

    def test_declares_an_auto_delete_exchange_with_arguments(self, topology):
        name = _name("ex-args")
        alternate = _name("ex-alternate")
        topology.exchange(alternate).declare()
        topology.exchange(name).auto_delete().argument("alternate-exchange", alternate).declare()

    def test_a_name_needing_percent_encoding_resolves(self, topology, management):
        name = _name("ex needs encoding")
        topology.exchange(name).declare()
        management.exchange(name).delete()

    def test_delete_removes_the_exchange(self, topology, management):
        name = _name("ex-delete")
        topology.exchange(name).declare()
        assert management.exchange(name).delete() is None
        # Binding to a deleted exchange must fail, which proves it is gone.
        queue = _name("ex-delete-queue")
        topology.queue(queue).declare()
        with pytest.raises(ManagementError):
            management.bind(source=name, destination=queue, binding_key="k")


class TestBindings:
    def test_binds_an_exchange_to_a_queue_and_routes_through_it(self, connection, topology, management):
        exchange, queue = _name("bind-ex"), _name("bind-q")
        topology.exchange(exchange).type(ExchangeType.DIRECT).declare()
        topology.queue(queue).declare()
        management.bind(source=exchange, destination=queue, binding_key="key")
        address = EXCHANGE_ADDRESS_WITH_KEY_TEMPLATE.format(
            name=encode_path_segment(exchange),
            key=encode_path_segment("key"),
        )
        _publish(connection, address, 1)
        delivery = _receive_one(connection, queue_path(queue))
        assert delivery is not None
        assert delivery.message.body_as_string() == "message-0"

    def test_lists_a_binding_it_created(self, topology, management):
        exchange, queue = _name("list-ex"), _name("list-q")
        topology.exchange(exchange).declare()
        topology.queue(queue).declare()
        management.bind(source=exchange, destination=queue, binding_key="key")
        entries = management.list_bindings(source=exchange, destination=queue, binding_key="key")
        assert len(entries) == 1
        assert entries[0]["binding_key"] == "key"

    def test_unbinds_a_binding_without_arguments(self, topology, management):
        exchange, queue = _name("unbind-ex"), _name("unbind-q")
        topology.exchange(exchange).declare()
        topology.queue(queue).declare()
        management.bind(source=exchange, destination=queue, binding_key="key")
        assert management.unbind(source=exchange, destination=queue, binding_key="key") is True
        assert management.list_bindings(source=exchange, destination=queue, binding_key="key") == []

    def test_binds_and_unbinds_with_arguments(self, topology, management):
        exchange, queue = _name("args-ex"), _name("args-q")
        arguments = {"x-match": "all", "region": "eu"}
        topology.exchange(exchange).type(ExchangeType.HEADERS).declare()
        topology.queue(queue).declare()
        management.bind(source=exchange, destination=queue, arguments=arguments)
        entries = management.list_bindings(source=exchange, destination=queue)
        assert len(entries) == 1
        assert entries[0]["arguments"] == arguments
        assert entries[0]["location"] != ""
        assert management.unbind(source=exchange, destination=queue, arguments=arguments) is True
        assert management.list_bindings(source=exchange, destination=queue) == []

    def test_unbinding_with_arguments_that_match_nothing_is_a_no_op(self, topology, management):
        exchange, queue = _name("nomatch-ex"), _name("nomatch-q")
        arguments = {"x-match": "all", "region": "eu"}
        topology.exchange(exchange).type(ExchangeType.HEADERS).declare()
        topology.queue(queue).declare()
        management.bind(source=exchange, destination=queue, arguments=arguments)
        assert (
            management.unbind(source=exchange, destination=queue, arguments={"x-match": "all", "region": "us"}) is False
        )
        assert len(management.list_bindings(source=exchange, destination=queue)) == 1
        management.unbind(source=exchange, destination=queue, arguments=arguments)

    def test_binds_an_exchange_to_an_exchange(self, connection, topology, management):
        upstream, downstream, queue = _name("up-ex"), _name("down-ex"), _name("e2e-q")
        topology.exchange(upstream).declare()
        topology.exchange(downstream).declare()
        topology.queue(queue).declare()
        management.bind(source=upstream, destination=downstream, binding_key="key", to_queue=False)
        management.bind(source=downstream, destination=queue, binding_key="key")
        entries = management.list_bindings(source=upstream, destination=downstream, binding_key="key", to_queue=False)
        assert len(entries) == 1
        address = EXCHANGE_ADDRESS_WITH_KEY_TEMPLATE.format(
            name=encode_path_segment(upstream),
            key=encode_path_segment("key"),
        )
        _publish(connection, address, 1)
        assert _receive_one(connection, queue_path(queue)) is not None
        assert management.unbind(source=upstream, destination=downstream, binding_key="key", to_queue=False) is True

    def test_binds_names_that_need_percent_encoding(self, topology, management):
        exchange, queue, key = _name("enc ex"), _name("enc q"), "a b/c"
        topology.exchange(exchange).declare()
        topology.queue(queue).declare()
        management.bind(source=exchange, destination=queue, binding_key=key)
        assert len(management.list_bindings(source=exchange, destination=queue, binding_key=key)) == 1
        assert management.unbind(source=exchange, destination=queue, binding_key=key) is True
        assert management.list_bindings(source=exchange, destination=queue, binding_key=key) == []
