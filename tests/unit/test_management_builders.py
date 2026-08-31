"""Queue/exchange specification builders, argument validation, and request shapes.

Every test here runs against :class:`RecordingManagement`, which records the
verb, path, body and expected-code set of each operation instead of sending it,
so the real body- and path-building code is exercised without a broker.
"""

from __future__ import annotations

from datetime import timedelta
from typing import NamedTuple

import pytest

from rabbitmq_amqp_python_client import (
    ClassicQueueMode,
    ClassicQueueVersion,
    ExchangeSpecification,
    ExchangeType,
    LeaderLocatorStrategy,
    OverflowStrategy,
    ProtocolError,
    QueueInfo,
    QueueSpecification,
    QueueType,
    QuorumQueueDeadLetterStrategy,
    QuorumQueueDelayedRetryType,
    ValidationError,
)
from rabbitmq_amqp_python_client.management import (
    EXPECTED_BIND,
    EXPECTED_DECLARE_EXCHANGE,
    EXPECTED_DECLARE_QUEUE,
    EXPECTED_DELETE_EXCHANGE,
    EXPECTED_DELETE_QUEUE,
    EXPECTED_LIST_BINDINGS,
    EXPECTED_PURGE_QUEUE,
    EXPECTED_QUEUE_INFO,
    EXPECTED_UNBIND,
    GENERATED_NAME_PREFIX,
    TEN_YEARS_MS,
    Management,
)


class Call(NamedTuple):
    """One recorded management request."""

    verb: str
    path: str
    body: object
    expected_codes: frozenset[int]


class RecordingManagement(Management):
    """A management endpoint that records requests instead of sending them.

    Deliberately skips ``Management.__init__``: none of the operation methods
    touch the connection or the link pair, only :meth:`_request`.
    """

    def __init__(self, response=None, topology_listener=None):
        self.calls: list[Call] = []
        self.response = response
        self._topology_listener = topology_listener

    def _request(self, verb, path, body, expected_codes):
        self.calls.append(Call(verb, path, body, frozenset(expected_codes)))
        return self.response

    @property
    def call(self) -> Call:
        """The single recorded request."""
        assert len(self.calls) == 1, f"expected one request, recorded {len(self.calls)}"
        return self.calls[0]


DECLARE_RESPONSE = {
    "name": "orders",
    "durable": True,
    "auto_delete": False,
    "exclusive": False,
    "type": "quorum",
    "arguments": {"x-queue-type": "quorum"},
    "leader": "rabbit@node1",
    "replicas": ["rabbit@node1", "rabbit@node2"],
    "message_count": 12,
    "consumer_count": 3,
}


@pytest.fixture
def management():
    """A recording management endpoint that answers every declare identically."""
    return RecordingManagement(dict(DECLARE_RESPONSE))


def _queue(management, name="orders"):
    """A queue builder wired to the recording endpoint."""
    return QueueSpecification(management, name)


class TestQueueSpecificationReaders:
    def test_reports_an_empty_name_when_none_was_set(self):
        assert QueueSpecification(RecordingManagement()).queue_name == ""

    def test_reads_back_every_generic_setter(self, management):
        specification = _queue(management).name("q").exclusive().auto_delete()
        assert specification.queue_name == "q"
        assert specification.is_exclusive is True
        assert specification.is_auto_delete is True

    def test_setters_default_to_true(self, management):
        assert _queue(management).exclusive().is_exclusive is True
        assert _queue(management).auto_delete().is_auto_delete is True

    def test_setters_accept_an_explicit_false(self, management):
        assert _queue(management).exclusive(False).is_exclusive is False

    def test_arguments_merge_rather_than_replace(self, management):
        specification = _queue(management).arguments({"a": 1}).arguments({"b": 2})
        assert specification.queue_arguments == {"a": 1, "b": 2}

    def test_queue_arguments_returns_a_copy(self, management):
        specification = _queue(management).arguments({"a": 1})
        specification.queue_arguments["a"] = 999
        assert specification.queue_arguments == {"a": 1}

    def test_every_setter_returns_the_same_builder(self, management):
        specification = _queue(management)
        assert specification.name("q") is specification
        assert specification.type(QueueType.CLASSIC) is specification
        assert specification.max_length(10) is specification
        assert specification.single_active_consumer() is specification


class TestQueueArguments:
    def test_typed_setters_write_the_documented_keys(self, management):
        arguments = (
            _queue(management)
            .type(QueueType.CLASSIC)
            .dead_letter_exchange("dlx")
            .dead_letter_routing_key("dlk")
            .overflow_strategy(OverflowStrategy.REJECT_PUBLISH_DLX)
            .max_length_bytes(1024)
            .max_length(500)
            .message_ttl(60_000)
            .expires(120_000)
            .leader_locator(LeaderLocatorStrategy.BALANCED)
            .single_active_consumer()
            .queue_arguments
        )
        assert arguments == {
            "x-queue-type": "classic",
            "x-dead-letter-exchange": "dlx",
            "x-dead-letter-routing-key": "dlk",
            "x-overflow": "reject-publish-dlx",
            "x-max-length-bytes": 1024,
            "x-max-length": 500,
            "x-message-ttl": 60_000,
            "x-expires": 120_000,
            "x-queue-leader-locator": "balanced",
            "x-single-active-consumer": True,
        }

    @pytest.mark.parametrize(
        ("strategy", "value"),
        [
            (OverflowStrategy.DROP_HEAD, "drop-head"),
            (OverflowStrategy.REJECT_PUBLISH, "reject-publish"),
            (OverflowStrategy.REJECT_PUBLISH_DLX, "reject-publish-dlx"),
        ],
    )
    def test_overflow_strategy_values(self, management, strategy, value):
        assert _queue(management).overflow_strategy(strategy).queue_arguments["x-overflow"] == value

    @pytest.mark.parametrize(
        ("strategy", "value"),
        [(LeaderLocatorStrategy.CLIENT_LOCAL, "client-local"), (LeaderLocatorStrategy.BALANCED, "balanced")],
    )
    def test_leader_locator_values(self, management, strategy, value):
        arguments = _queue(management).leader_locator(strategy).queue_arguments
        assert arguments["x-queue-leader-locator"] == value

    @pytest.mark.parametrize(
        ("queue_type", "value"),
        [(QueueType.QUORUM, "quorum"), (QueueType.CLASSIC, "classic"), (QueueType.STREAM, "stream")],
    )
    def test_queue_type_values(self, management, queue_type, value):
        assert _queue(management).type(queue_type).queue_arguments["x-queue-type"] == value

    def test_durations_accept_a_timedelta(self, management):
        arguments = _queue(management).message_ttl(timedelta(minutes=1)).expires(timedelta(hours=2)).queue_arguments
        assert arguments["x-message-ttl"] == 60_000
        assert arguments["x-expires"] == 7_200_000


class TestQueueArgumentBounds:
    """§5.4: every numeric bound is enforced before a frame is ever sent."""

    @pytest.mark.parametrize("value", [0, -1, -1024])
    def test_max_length_bytes_must_be_positive(self, management, value):
        with pytest.raises(ValidationError, match="x-max-length-bytes must be > 0"):
            _queue(management).max_length_bytes(value)

    @pytest.mark.parametrize("value", [0, -1])
    def test_max_length_must_be_positive(self, management, value):
        with pytest.raises(ValidationError, match="x-max-length must be > 0"):
            _queue(management).max_length(value)

    def test_message_ttl_accepts_zero(self, management):
        assert _queue(management).message_ttl(0).queue_arguments["x-message-ttl"] == 0

    def test_message_ttl_rejects_a_negative_value(self, management):
        with pytest.raises(ValidationError, match="x-message-ttl must be in 0"):
            _queue(management).message_ttl(-1)

    def test_message_ttl_accepts_exactly_ten_years(self, management):
        assert _queue(management).message_ttl(TEN_YEARS_MS).queue_arguments["x-message-ttl"] == TEN_YEARS_MS

    def test_message_ttl_rejects_more_than_ten_years(self, management):
        with pytest.raises(ValidationError, match="x-message-ttl must be in"):
            _queue(management).message_ttl(TEN_YEARS_MS + 1)

    def test_expires_rejects_zero(self, management):
        with pytest.raises(ValidationError, match="x-expires must be in 1"):
            _queue(management).expires(0)

    def test_expires_accepts_exactly_ten_years(self, management):
        assert _queue(management).expires(TEN_YEARS_MS).queue_arguments["x-expires"] == TEN_YEARS_MS

    def test_expires_rejects_more_than_ten_years(self, management):
        with pytest.raises(ValidationError, match="x-expires must be in"):
            _queue(management).expires(TEN_YEARS_MS + 1)

    def test_a_validation_error_is_also_a_value_error(self, management):
        with pytest.raises(ValueError, match="must be > 0"):
            _queue(management).max_length(0)

    def test_a_rejected_setter_leaves_the_builder_untouched(self, management):
        specification = _queue(management)
        with pytest.raises(ValidationError):
            specification.max_length(0)
        assert specification.queue_arguments == {}


class TestStreamSpecification:
    def test_selects_the_stream_type_immediately(self, management):
        specification = _queue(management)
        specification.stream()
        assert specification.queue_arguments["x-queue-type"] == "stream"

    def test_is_a_view_over_the_same_parent(self, management):
        specification = _queue(management)
        assert specification.stream().queue() is specification

    def test_writes_stream_arguments_onto_the_parent(self, management):
        specification = _queue(management)
        specification.stream().max_age(3600).max_segment_size_bytes(500_000).initial_cluster_size(
            3
        ).file_size_per_chunk(128)
        assert specification.queue_arguments == {
            "x-queue-type": "stream",
            "x-max-age": "3600s",
            "x-stream-max-segment-size-bytes": 500_000,
            "x-initial-cluster-size": 3,
            "x-stream-file-size-per-chunk": 128,
        }

    def test_max_age_is_encoded_as_a_seconds_string(self, management):
        specification = _queue(management)
        specification.stream().max_age(timedelta(hours=1))
        assert specification.queue_arguments["x-max-age"] == "3600s"

    def test_every_setter_returns_the_sub_builder(self, management):
        stream = _queue(management).stream()
        assert stream.max_age(1) is stream
        assert stream.max_segment_size_bytes(1) is stream
        assert stream.initial_cluster_size(1) is stream
        assert stream.file_size_per_chunk(1) is stream

    @pytest.mark.parametrize("value", [0, -1])
    def test_max_age_must_be_positive(self, management, value):
        with pytest.raises(ValidationError, match="x-max-age must be > 0"):
            _queue(management).stream().max_age(value)

    @pytest.mark.parametrize(
        ("setter", "argument"),
        [
            ("max_segment_size_bytes", "x-stream-max-segment-size-bytes"),
            ("initial_cluster_size", "x-initial-cluster-size"),
            ("file_size_per_chunk", "x-stream-file-size-per-chunk"),
        ],
    )
    @pytest.mark.parametrize("value", [0, -1])
    def test_stream_sizes_must_be_positive(self, management, setter, argument, value):
        with pytest.raises(ValidationError, match=f"{argument} must be > 0"):
            getattr(_queue(management).stream(), setter)(value)

    def test_chains_back_into_the_parent_in_one_expression(self, management):
        info = _queue(management).stream().max_age(60).queue().declare()
        assert management.call.body["arguments"]["x-max-age"] == "60s"
        assert info.name == "orders"


class TestQuorumQueueSpecification:
    def test_selects_the_quorum_type_immediately(self, management):
        specification = _queue(management)
        specification.quorum()
        assert specification.queue_arguments["x-queue-type"] == "quorum"

    def test_is_a_view_over_the_same_parent(self, management):
        specification = _queue(management)
        assert specification.quorum().queue() is specification

    def test_writes_quorum_arguments_onto_the_parent(self, management):
        specification = _queue(management)
        (
            specification.quorum()
            .dead_letter_strategy(QuorumQueueDeadLetterStrategy.AT_LEAST_ONCE)
            .delivery_limit(5)
            .quorum_initial_group_size(3)
            .quorum_target_group_size(5)
            .delayed_retry_type(QuorumQueueDelayedRetryType.FAILED)
            .delayed_retry_min(1_000)
            .delayed_retry_max(60_000)
        )
        assert specification.queue_arguments == {
            "x-queue-type": "quorum",
            "x-dead-letter-strategy": "at-least-once",
            "x-delivery-limit": 5,
            "x-quorum-initial-group-size": 3,
            "x-quorum-target-group-size": 5,
            "x-delayed-retry-type": "failed",
            "x-delayed-retry-min": 1_000,
            "x-delayed-retry-max": 60_000,
        }

    @pytest.mark.parametrize(
        ("strategy", "value"),
        [
            (QuorumQueueDeadLetterStrategy.AT_MOST_ONCE, "at-most-once"),
            (QuorumQueueDeadLetterStrategy.AT_LEAST_ONCE, "at-least-once"),
        ],
    )
    def test_dead_letter_strategy_values(self, management, strategy, value):
        specification = _queue(management)
        specification.quorum().dead_letter_strategy(strategy)
        assert specification.queue_arguments["x-dead-letter-strategy"] == value

    @pytest.mark.parametrize(
        ("retry_type", "value"),
        [
            (QuorumQueueDelayedRetryType.DISABLED, "disabled"),
            (QuorumQueueDelayedRetryType.ALL, "all"),
            (QuorumQueueDelayedRetryType.FAILED, "failed"),
            (QuorumQueueDelayedRetryType.RETURNED, "returned"),
        ],
    )
    def test_delayed_retry_type_values(self, management, retry_type, value):
        specification = _queue(management)
        specification.quorum().delayed_retry_type(retry_type)
        assert specification.queue_arguments["x-delayed-retry-type"] == value

    @pytest.mark.parametrize(
        ("setter", "argument"),
        [
            ("delivery_limit", "x-delivery-limit"),
            ("quorum_initial_group_size", "x-quorum-initial-group-size"),
            ("quorum_target_group_size", "x-quorum-target-group-size"),
            ("delayed_retry_min", "x-delayed-retry-min"),
            ("delayed_retry_max", "x-delayed-retry-max"),
        ],
    )
    @pytest.mark.parametrize("value", [0, -1])
    def test_quorum_numbers_must_be_positive(self, management, setter, argument, value):
        with pytest.raises(ValidationError, match=f"{argument} must be > 0"):
            getattr(_queue(management).quorum(), setter)(value)

    def test_delayed_retry_durations_accept_a_timedelta(self, management):
        specification = _queue(management)
        specification.quorum().delayed_retry_min(timedelta(seconds=1)).delayed_retry_max(timedelta(minutes=1))
        assert specification.queue_arguments["x-delayed-retry-min"] == 1_000
        assert specification.queue_arguments["x-delayed-retry-max"] == 60_000

    @pytest.mark.parametrize("setter", ["delayed_retry_min", "delayed_retry_max"])
    def test_a_delayed_retry_bound_requires_the_retry_type(self, management, setter):
        specification = _queue(management)
        getattr(specification.quorum(), setter)(1_000)
        with pytest.raises(ValidationError, match="require x-delayed-retry-type"):
            specification.declare()

    def test_both_delayed_retry_bounds_are_named_in_the_error(self, management):
        specification = _queue(management)
        specification.quorum().delayed_retry_min(1).delayed_retry_max(2)
        with pytest.raises(ValidationError, match="x-delayed-retry-min and x-delayed-retry-max require"):
            specification.declare()

    def test_a_delayed_retry_bound_is_accepted_once_the_type_is_set(self, management):
        specification = _queue(management)
        specification.quorum().delayed_retry_type(QuorumQueueDelayedRetryType.ALL).delayed_retry_min(1_000)
        specification.declare()
        assert management.call.body["arguments"]["x-delayed-retry-min"] == 1_000


class TestClassicQueueSpecification:
    def test_selects_the_classic_type_immediately(self, management):
        specification = _queue(management)
        specification.classic()
        assert specification.queue_arguments["x-queue-type"] == "classic"

    def test_is_a_view_over_the_same_parent(self, management):
        specification = _queue(management)
        assert specification.classic().queue() is specification

    def test_writes_classic_arguments_onto_the_parent(self, management):
        specification = _queue(management)
        specification.classic().max_priority(5).mode(ClassicQueueMode.LAZY).version(ClassicQueueVersion.V2)
        assert specification.queue_arguments == {
            "x-queue-type": "classic",
            "x-max-priority": 5,
            "x-queue-mode": "lazy",
            "x-queue-version": 2,
        }

    @pytest.mark.parametrize(("version", "value"), [(ClassicQueueVersion.V1, 1), (ClassicQueueVersion.V2, 2)])
    def test_version_is_sent_as_an_integer(self, management, version, value):
        specification = _queue(management)
        specification.classic().version(version)
        assert specification.queue_arguments["x-queue-version"] == value

    @pytest.mark.parametrize(
        ("mode", "value"), [(ClassicQueueMode.DEFAULT, "default"), (ClassicQueueMode.LAZY, "lazy")]
    )
    def test_mode_values(self, management, mode, value):
        specification = _queue(management)
        specification.classic().mode(mode)
        assert specification.queue_arguments["x-queue-mode"] == value

    @pytest.mark.parametrize("value", [1, 2, 128, 255])
    def test_max_priority_accepts_the_whole_valid_range(self, management, value):
        specification = _queue(management)
        specification.classic().max_priority(value)
        assert specification.queue_arguments["x-max-priority"] == value

    @pytest.mark.parametrize("value", [0, -1, 256, 1000])
    def test_max_priority_rejects_values_outside_one_to_255(self, management, value):
        with pytest.raises(ValidationError, match="x-max-priority must be in 1..255"):
            _queue(management).classic().max_priority(value)


class TestQueueDeclare:
    def test_sends_put_to_the_encoded_queue_path(self, management):
        _queue(management, "my queue").declare()
        assert management.call.verb == "PUT"
        assert management.call.path == "/queues/my%20queue"
        assert management.call.expected_codes == EXPECTED_DECLARE_QUEUE
        assert management.call.expected_codes == frozenset({200, 201, 409})

    def test_body_always_declares_a_durable_queue(self, management):
        _queue(management).declare()
        assert management.call.body == {"durable": True, "exclusive": False, "auto_delete": False, "arguments": {}}

    def test_body_carries_exclusive_and_auto_delete_for_a_classic_queue(self, management):
        _queue(management).classic().queue().exclusive().auto_delete().declare()
        assert management.call.body["exclusive"] is True
        assert management.call.body["auto_delete"] is True

    @pytest.mark.parametrize("queue_type", [QueueType.QUORUM, QueueType.STREAM])
    def test_replicated_types_force_exclusive_and_auto_delete_off(self, management, queue_type):
        specification = _queue(management).exclusive().auto_delete().type(queue_type)
        specification.declare()
        assert management.call.body["exclusive"] is False
        assert management.call.body["auto_delete"] is False
        # The forcing is written back, so the readers agree with what was sent.
        assert specification.is_exclusive is False
        assert specification.is_auto_delete is False

    def test_generates_a_name_when_none_was_set(self, management):
        specification = QueueSpecification(management)
        specification.declare()
        assert specification.queue_name.startswith(GENERATED_NAME_PREFIX)
        assert management.call.path == f"/queues/{specification.queue_name}"

    def test_keeps_a_name_that_was_set(self, management):
        specification = _queue(management, "orders")
        specification.declare()
        assert specification.queue_name == "orders"

    def test_parses_the_response_into_queue_info(self, management):
        info = _queue(management).declare()
        assert info == QueueInfo(
            name="orders",
            durable=True,
            auto_delete=False,
            exclusive=False,
            queue_type=QueueType.QUORUM,
            arguments={"x-queue-type": "quorum"},
            leader="rabbit@node1",
            replicas=("rabbit@node1", "rabbit@node2"),
            message_count=12,
            consumer_count=3,
        )

    def test_rejects_a_non_map_response(self):
        with pytest.raises(ProtocolError, match="expected a map in the queue declare response"):
            _queue(RecordingManagement("not a map")).declare()


class TestQueuePurgeAndDelete:
    def test_purge_deletes_the_messages_sub_resource(self, management):
        management.response = {"message_count": 42}
        assert _queue(management, "my queue").purge() == 42
        assert management.call.verb == "DELETE"
        assert management.call.path == "/queues/my%20queue/messages"
        assert management.call.body is None
        assert management.call.expected_codes == EXPECTED_PURGE_QUEUE

    def test_purge_reports_zero_when_the_broker_omits_the_count(self, management):
        management.response = {}
        assert _queue(management).purge() == 0

    def test_delete_returns_a_name_only_stub(self, management):
        info = _queue(management, "orders").delete()
        assert info == QueueInfo(name="orders")
        assert management.call.verb == "DELETE"
        assert management.call.path == "/queues/orders"
        assert management.call.body is None
        assert management.call.expected_codes == EXPECTED_DELETE_QUEUE

    @pytest.mark.parametrize("operation", ["purge", "delete"])
    def test_a_nameless_queue_cannot_be_purged_or_deleted(self, management, operation):
        with pytest.raises(ValidationError, match="a non-empty queue name is required"):
            getattr(QueueSpecification(management), operation)()


class TestQueueInfoParsing:
    def test_defaults_every_field_the_broker_omits(self):
        info = QueueInfo.from_body({"name": "q"})
        assert info == QueueInfo(name="q")
        assert info.queue_type is QueueType.CLASSIC
        assert info.arguments == {}
        assert info.replicas == ()

    def test_tolerates_null_leader_and_replicas(self):
        info = QueueInfo.from_body({"name": "q", "leader": None, "replicas": None})
        assert info.leader == ""
        assert info.replicas == ()

    @pytest.mark.parametrize("raw", ["quorum", "classic", "stream"])
    def test_parses_every_known_queue_type(self, raw):
        assert QueueInfo.from_body({"name": "q", "type": raw}).queue_type is QueueType(raw)

    def test_rejects_a_queue_type_it_does_not_know(self):
        with pytest.raises(ProtocolError, match="unknown queue type"):
            QueueInfo.from_body({"name": "q", "type": "mystery"})

    def test_is_frozen(self):
        info = QueueInfo(name="q")
        with pytest.raises(AttributeError):
            info.name = "other"  # type: ignore[misc]


class TestQueueInfoRequest:
    def test_gets_the_encoded_queue_path(self, management):
        management.response = {"name": "orders"}
        assert management.queue_info("my queue").name == "orders"
        assert management.call.verb == "GET"
        assert management.call.path == "/queues/my%20queue"
        assert management.call.body is None
        assert management.call.expected_codes == EXPECTED_QUEUE_INFO

    def test_requires_a_name(self, management):
        with pytest.raises(ValidationError, match="a non-empty queue name is required"):
            management.queue_info("")


class TestExchangeSpecification:
    def test_defaults_to_a_direct_exchange(self, management):
        assert ExchangeSpecification(management).exchange_type == "direct"

    def test_reads_back_every_setter(self, management):
        specification = ExchangeSpecification(management).name("events").auto_delete().type(ExchangeType.TOPIC)
        assert specification.exchange_name == "events"
        assert specification.is_auto_delete is True
        assert specification.exchange_type == "topic"

    def test_accepts_a_plugin_exchange_type_as_a_string(self, management):
        assert ExchangeSpecification(management).type("x-consistent-hash").exchange_type == "x-consistent-hash"

    def test_arguments_can_be_set_one_at_a_time_or_merged(self, management):
        specification = ExchangeSpecification(management).argument("a", 1).arguments({"b": 2, "c": 3})
        assert specification.exchange_arguments == {"a": 1, "b": 2, "c": 3}

    def test_exchange_arguments_returns_a_copy(self, management):
        specification = ExchangeSpecification(management).argument("a", 1)
        specification.exchange_arguments["a"] = 999
        assert specification.exchange_arguments == {"a": 1}

    def test_every_setter_returns_the_same_builder(self, management):
        specification = ExchangeSpecification(management)
        assert specification.name("x") is specification
        assert specification.auto_delete() is specification
        assert specification.type(ExchangeType.FANOUT) is specification
        assert specification.argument("a", 1) is specification
        assert specification.arguments({"b": 2}) is specification

    def test_declare_sends_put_to_the_encoded_exchange_path(self, management):
        ExchangeSpecification(management, "my exchange").type(ExchangeType.HEADERS).declare()
        assert management.call.verb == "PUT"
        assert management.call.path == "/exchanges/my%20exchange"
        assert management.call.expected_codes == EXPECTED_DECLARE_EXCHANGE
        assert management.call.expected_codes == frozenset({201, 204, 409})

    def test_declare_body_always_declares_a_durable_exchange(self, management):
        ExchangeSpecification(management, "events").declare()
        assert management.call.body == {
            "durable": True,
            "auto_delete": False,
            "type": "direct",
            "arguments": {},
        }

    def test_declare_body_lower_cases_the_type(self, management):
        ExchangeSpecification(management, "events").type("X-Consistent-Hash").declare()
        assert management.call.body["type"] == "x-consistent-hash"

    def test_declare_body_carries_auto_delete_and_arguments(self, management):
        ExchangeSpecification(management, "events").auto_delete().argument("alternate-exchange", "alt").declare()
        assert management.call.body["auto_delete"] is True
        assert management.call.body["arguments"] == {"alternate-exchange": "alt"}

    def test_delete_sends_delete_to_the_exchange_path(self, management):
        ExchangeSpecification(management, "events").delete()
        assert management.call.verb == "DELETE"
        assert management.call.path == "/exchanges/events"
        assert management.call.body is None
        assert management.call.expected_codes == EXPECTED_DELETE_EXCHANGE

    @pytest.mark.parametrize("operation", ["declare", "delete"])
    def test_a_nameless_exchange_cannot_be_declared_or_deleted(self, management, operation):
        with pytest.raises(ValidationError, match="a non-empty exchange name is required"):
            getattr(ExchangeSpecification(management), operation)()

    def test_declare_and_delete_return_nothing(self, management):
        assert ExchangeSpecification(management, "events").declare() is None
        assert ExchangeSpecification(management, "events").delete() is None


class TestBind:
    def test_posts_to_the_bindings_collection(self, management):
        management.bind(source="ex", destination="q", binding_key="key")
        assert management.call.verb == "POST"
        assert management.call.path == "/bindings"
        assert management.call.expected_codes == EXPECTED_BIND
        assert management.call.expected_codes == frozenset({204})

    def test_body_names_a_queue_destination(self, management):
        management.bind(source="ex", destination="q", binding_key="key")
        assert management.call.body == {
            "source": "ex",
            "binding_key": "key",
            "arguments": {},
            "destination_queue": "q",
        }

    def test_body_names_an_exchange_destination(self, management):
        management.bind(source="ex", destination="other", binding_key="key", to_queue=False)
        assert management.call.body == {
            "source": "ex",
            "binding_key": "key",
            "arguments": {},
            "destination_exchange": "other",
        }

    def test_body_never_names_both_destinations(self, management):
        management.bind(source="ex", destination="q")
        assert ("destination_queue" in management.call.body) != ("destination_exchange" in management.call.body)

    def test_body_carries_the_arguments(self, management):
        management.bind(source="ex", destination="q", arguments={"x-match": "all", "region": "eu"})
        assert management.call.body["arguments"] == {"x-match": "all", "region": "eu"}

    def test_body_defaults_the_binding_key_to_empty(self, management):
        management.bind(source="ex", destination="q")
        assert management.call.body["binding_key"] == ""

    def test_path_is_not_percent_encoded_because_names_ride_in_the_body(self, management):
        management.bind(source="my ex", destination="my q")
        assert management.call.path == "/bindings"
        assert management.call.body["source"] == "my ex"

    @pytest.mark.parametrize(("source", "destination"), [("", "q"), ("ex", "")])
    def test_requires_both_ends(self, management, source, destination):
        with pytest.raises(ValidationError, match="name is required"):
            management.bind(source=source, destination=destination)


class TestListBindings:
    def test_gets_the_query_path(self, management):
        management.response = []
        management.list_bindings(source="my ex", destination="my q", binding_key="a b")
        assert management.call.verb == "GET"
        assert management.call.path == "/bindings?src=my+ex&dstq=my+q&key=a+b"
        assert management.call.body is None
        assert management.call.expected_codes == EXPECTED_LIST_BINDINGS

    def test_returns_the_listed_bindings(self, management):
        management.response = [{"binding_key": "key", "arguments": {"a": 1}, "location": "/bindings/xyz"}]
        entries = management.list_bindings(source="ex", destination="q", binding_key="key")
        assert entries == [{"binding_key": "key", "arguments": {"a": 1}, "location": "/bindings/xyz"}]

    def test_treats_a_missing_body_as_no_bindings(self, management):
        management.response = None
        assert management.list_bindings(source="ex", destination="q") == []

    def test_rejects_a_body_that_is_not_a_list(self, management):
        management.response = {"not": "a list"}
        with pytest.raises(ProtocolError, match="expected a list in the list bindings response"):
            management.list_bindings(source="ex", destination="q")


class TestUnbind:
    def test_deletes_the_semicolon_path_when_there_are_no_arguments(self, management):
        assert management.unbind(source="ex", destination="q", binding_key="key") is True
        assert management.call.verb == "DELETE"
        assert management.call.path == "/bindings/src=ex;dstq=q;key=key;args="
        assert management.call.body is None
        assert management.call.expected_codes == EXPECTED_UNBIND

    def test_treats_an_empty_argument_map_as_no_arguments(self, management):
        management.unbind(source="ex", destination="q", binding_key="key", arguments={})
        assert management.call.path.endswith(";args=")

    def test_uses_dste_for_an_exchange_destination(self, management):
        management.unbind(source="ex", destination="other", binding_key="key", to_queue=False)
        assert management.call.path == "/bindings/src=ex;dste=other;key=key;args="

    def test_lists_then_deletes_when_there_are_arguments(self, management):
        management.response = [
            {"binding_key": "other", "arguments": {"a": 1}, "location": "/bindings/wrong-key"},
            {"binding_key": "key", "arguments": {"a": 2}, "location": "/bindings/wrong-args"},
            {"binding_key": "key", "arguments": {"a": 1}, "location": "/bindings/right"},
        ]
        assert management.unbind(source="ex", destination="q", binding_key="key", arguments={"a": 1}) is True
        assert [call.verb for call in management.calls] == ["GET", "DELETE"]
        assert management.calls[0].path == "/bindings?src=ex&dstq=q&key=key"
        assert management.calls[1].path == "/bindings/right"
        assert management.calls[1].expected_codes == EXPECTED_UNBIND

    def test_reports_no_match_as_a_no_op(self, management):
        management.response = [{"binding_key": "key", "arguments": {"a": 2}, "location": "/bindings/other"}]
        assert management.unbind(source="ex", destination="q", binding_key="key", arguments={"a": 1}) is False
        assert [call.verb for call in management.calls] == ["GET"]

    def test_reports_no_match_when_the_broker_lists_nothing(self, management):
        management.response = []
        assert management.unbind(source="ex", destination="q", arguments={"a": 1}) is False

    def test_skips_a_matching_entry_without_a_location(self, management):
        management.response = [{"binding_key": "key", "arguments": {"a": 1}}]
        assert management.unbind(source="ex", destination="q", binding_key="key", arguments={"a": 1}) is False

    def test_matches_arguments_regardless_of_key_ordering(self, management):
        management.response = [{"binding_key": "k", "arguments": {"b": 2, "a": 1}, "location": "/bindings/right"}]
        assert management.unbind(source="ex", destination="q", binding_key="k", arguments={"a": 1, "b": 2}) is True
        assert management.calls[1].path == "/bindings/right"

    @pytest.mark.parametrize(("source", "destination"), [("", "q"), ("ex", "")])
    def test_requires_both_ends(self, management, source, destination):
        with pytest.raises(ValidationError, match="name is required"):
            management.unbind(source=source, destination=destination)
