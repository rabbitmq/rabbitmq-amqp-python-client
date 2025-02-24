from datetime import timedelta

from rabbitmq_amqp_python_client import (
    ClassicQueueSpecification,
    ExchangeCustomSpecification,
    ExchangeSpecification,
    ExchangeToExchangeBindingSpecification,
    ExchangeToQueueBindingSpecification,
    ExchangeType,
    Management,
    QueueType,
    QuorumQueueSpecification,
    StreamSpecification,
)
from rabbitmq_amqp_python_client.exceptions import (
    ValidationCodeException,
)


def test_declare_delete_exchange(management: Management) -> None:

    exchange_name = "test-exchange"

    exchange_info = management.declare_exchange(
        ExchangeSpecification(name=exchange_name)
    )

    assert exchange_info.name == exchange_name

    management.delete_exchange(exchange_name)


def test_declare_delete_exchange_headers(management: Management) -> None:

    exchange_name = "test-exchange"

    exchange_info = management.declare_exchange(
        ExchangeSpecification(name=exchange_name, exchange_type=ExchangeType.headers)
    )

    assert exchange_info.name == exchange_name

    management.delete_exchange(exchange_name)


def test_declare_delete_exchange_custom(management: Management) -> None:

    exchange_name = "test-exchange"

    exchange_arguments = {}
    exchange_arguments["x-delayed-type"] = "direct"

    exchange_info = management.declare_exchange(
        ExchangeCustomSpecification(
            name=exchange_name,
            exchange_type="x-local-random",
            arguments=exchange_arguments,
        )
    )

    assert exchange_info.name == exchange_name

    management.delete_exchange(exchange_name)


def test_declare_delete_exchange_with_args(management: Management) -> None:

    exchange_name = "test-exchange-with-args"

    exchange_arguments = {}
    exchange_arguments["test"] = "test"

    exchange_info = management.declare_exchange(
        ExchangeSpecification(
            name=exchange_name,
            exchange_type=ExchangeType.topic,
            arguments=exchange_arguments,
        )
    )

    assert exchange_info.name == exchange_name
    assert exchange_info.exchange_type == ExchangeType.topic
    assert exchange_info.arguments == exchange_arguments

    management.delete_exchange(exchange_name)


def test_declare_purge_delete_queue(management: Management) -> None:
    queue_name = "my_queue"

    queue_info = management.declare_queue(QuorumQueueSpecification(name=queue_name))

    assert queue_info.name == queue_name

    management.purge_queue(queue_name)

    management.delete_queue(queue_name)


def test_bind_exchange_to_queue(management: Management) -> None:

    exchange_name = "test-bind-exchange-to-queue-exchange"
    queue_name = "test-bind-exchange-to-queue-queue"
    routing_key = "routing-key"

    management.declare_exchange(ExchangeSpecification(name=exchange_name))

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    binding_exchange_queue_path = management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    assert (
        binding_exchange_queue_path
        == "/bindings/src="
        + exchange_name
        + ";dstq="
        + queue_name
        + ";key="
        + routing_key
        + ";args="
    )

    management.delete_exchange(exchange_name)

    management.delete_queue(queue_name)

    management.unbind(binding_exchange_queue_path)


def test_bind_exchange_to_queue_without_key(management: Management) -> None:

    exchange_name = "test-bind-exchange-to-queue-exchange"
    queue_name = "test-bind-exchange-to-queue-queue"

    management.declare_exchange(ExchangeSpecification(name=exchange_name))

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    binding_exchange_queue_path = management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
        )
    )

    assert (
        binding_exchange_queue_path
        == "/bindings/src=" + exchange_name + ";dstq=" + queue_name + ";key=" + ";args="
    )

    management.unbind(binding_exchange_queue_path)

    management.delete_exchange(exchange_name)

    management.delete_queue(queue_name)


def test_bind_exchange_to_exchange_without_key(management: Management) -> None:

    source_exchange_name = "test-bind-exchange-to-queue-exchange"
    destination_exchange_name = "test-bind-exchange-to-queue-queue"

    management.declare_exchange(ExchangeSpecification(name=source_exchange_name))

    management.declare_exchange(ExchangeSpecification(name=destination_exchange_name))

    binding_exchange_queue_path = management.bind(
        ExchangeToExchangeBindingSpecification(
            source_exchange=source_exchange_name,
            destination_exchange=destination_exchange_name,
        )
    )

    assert (
        binding_exchange_queue_path
        == "/bindings/src="
        + source_exchange_name
        + ";dstq="
        + destination_exchange_name
        + ";key="
        + ";args="
    )

    management.unbind(binding_exchange_queue_path)

    management.delete_exchange(source_exchange_name)

    management.delete_exchange(destination_exchange_name)


def test_bind_unbind_by_binding_spec(management: Management) -> None:

    exchange_name = "test-bind-exchange-to-queue-exchange"
    queue_name = "test-bind-exchange-to-queue-queue"

    management.declare_exchange(ExchangeSpecification(name=exchange_name))

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
        )
    )

    management.unbind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
        )
    )

    management.delete_exchange(exchange_name)

    management.delete_queue(queue_name)


def test_bind_unbind_exchange_by_exchange_spec(management: Management) -> None:

    source_exchange_name = "test-bind-exchange-to-queue-exchange"
    destination_exchange_name = "test-bind-exchange-to-queue-queue"

    management.declare_exchange(ExchangeSpecification(name=source_exchange_name))

    management.declare_exchange(ExchangeSpecification(name=destination_exchange_name))

    binding_exchange_queue_path = management.bind(
        ExchangeToExchangeBindingSpecification(
            source_exchange=source_exchange_name,
            destination_exchange=destination_exchange_name,
        )
    )

    assert (
        binding_exchange_queue_path
        == "/bindings/src="
        + source_exchange_name
        + ";dstq="
        + destination_exchange_name
        + ";key="
        + ";args="
    )

    management.unbind(
        ExchangeToExchangeBindingSpecification(
            source_exchange=source_exchange_name,
            destination_exchange=destination_exchange_name,
        )
    )

    management.delete_exchange(source_exchange_name)

    management.delete_exchange(destination_exchange_name)


def test_bind_exchange_to_exchange(management: Management) -> None:

    source_exchange_name = "source_exchange"
    destination_exchange_name = "destination_exchange"
    routing_key = "routing-key"

    management.declare_exchange(ExchangeSpecification(name=source_exchange_name))

    management.declare_exchange(ExchangeSpecification(name=destination_exchange_name))

    binding_exchange_exchange_path = management.bind(
        ExchangeToExchangeBindingSpecification(
            source_exchange=source_exchange_name,
            destination_exchange=destination_exchange_name,
            binding_key=routing_key,
        )
    )

    assert (
        binding_exchange_exchange_path
        == "/bindings/src="
        + source_exchange_name
        + ";dstq="
        + destination_exchange_name
        + ";key="
        + routing_key
        + ";args="
    )

    management.unbind(binding_exchange_exchange_path)

    management.delete_exchange(source_exchange_name)

    management.delete_exchange(destination_exchange_name)


def test_queue_info_with_validations(management: Management) -> None:

    queue_name = "test_queue_info_with_validation"

    queue_specification = QuorumQueueSpecification(
        name=queue_name,
    )
    management.declare_queue(queue_specification)

    queue_info = management.queue_info(name=queue_name)

    management.delete_queue(queue_name)

    assert queue_info.name == queue_name
    assert queue_info.queue_type == QueueType.quorum
    assert queue_info.is_durable is True
    assert queue_info.message_count == 0


def test_queue_info_for_stream_with_validations(management: Management) -> None:

    stream_name = "test_stream_info_with_validation"

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management.declare_queue(queue_specification)

    stream_info = management.queue_info(name=stream_name)

    management.delete_queue(stream_name)

    assert stream_info.name == stream_name
    assert stream_info.queue_type == QueueType.stream
    assert stream_info.message_count == 0


def test_queue_precondition_fail(management: Management) -> None:
    test_failure = True

    queue_name = "test-queue_precondition_fail"

    queue_specification = QuorumQueueSpecification(name=queue_name, max_len_bytes=100)
    management.declare_queue(queue_specification)

    management.declare_queue(queue_specification)

    queue_specification = QuorumQueueSpecification(
        name=queue_name,
        max_len_bytes=200,
    )

    try:
        management.declare_queue(queue_specification)
    except ValidationCodeException:
        test_failure = False

    management.delete_queue(queue_name)

    assert test_failure is False


def test_declare_classic_queue(management: Management) -> None:

    queue_name = "test-declare_classic_queue"

    queue_specification = ClassicQueueSpecification(
        name=queue_name,
        is_auto_delete=False,
    )
    queue_info = management.declare_queue(queue_specification)

    assert queue_info.name == queue_specification.name

    management.delete_queue(queue_name)


def test_declare_classic_queue_with_args(management: Management) -> None:

    queue_name = "test-queue_with_args-2"

    queue_specification = ClassicQueueSpecification(
        name=queue_name,
        is_auto_delete=False,
        is_exclusive=False,
        is_durable=True,
        dead_letter_exchange="my_exchange",
        dead_letter_routing_key="my_key",
        max_len=500000,
        max_len_bytes=1000000000,
        message_ttl=timedelta(seconds=2),
        overflow_behaviour="reject-publish",
        auto_expires=timedelta(seconds=10),
        single_active_consumer=True,
        max_priority=100,
    )

    management.declare_queue(queue_specification)

    queue_info = management.queue_info(queue_name)

    assert queue_specification.name == queue_info.name
    assert queue_specification.is_auto_delete == queue_info.is_auto_delete
    assert queue_specification.is_exclusive == queue_info.is_exclusive
    assert queue_specification.is_durable == queue_info.is_durable
    assert (
        queue_specification.message_ttl.total_seconds() * 1000
    ) == queue_info.arguments["x-message-ttl"]
    assert queue_specification.overflow_behaviour == queue_info.arguments["x-overflow"]
    assert (
        queue_specification.auto_expires.total_seconds() * 1000
    ) == queue_info.arguments["x-expires"]
    assert queue_specification.max_priority == queue_info.arguments["x-max-priority"]

    assert (
        queue_specification.dead_letter_exchange
        == queue_info.arguments["x-dead-letter-exchange"]
    )
    assert (
        queue_specification.dead_letter_routing_key
        == queue_info.arguments["x-dead-letter-routing-key"]
    )
    assert queue_specification.max_len == queue_info.arguments["x-max-length"]
    assert (
        queue_specification.max_len_bytes == queue_info.arguments["x-max-length-bytes"]
    )

    assert (
        queue_specification.single_active_consumer
        == queue_info.arguments["x-single-active-consumer"]
    )

    management.delete_queue(queue_name)


def test_declare_quorum_queue_with_args(management: Management) -> None:

    queue_name = "test-queue_with_args"

    queue_specification = QuorumQueueSpecification(
        name=queue_name,
        dead_letter_exchange="my_exchange",
        dead_letter_routing_key="my_key",
        max_len=500000,
        max_len_bytes=1000000000,
        message_ttl=timedelta(seconds=2),
        overflow_behaviour="reject-publish",
        auto_expires=timedelta(seconds=2),
        single_active_consumer=True,
        deliver_limit=10,
        dead_letter_strategy="at-least-once",
        quorum_initial_group_size=5,
        cluster_target_group_size=5,
    )

    management.declare_queue(queue_specification)

    queue_info = management.queue_info(queue_name)

    assert queue_specification.name == queue_info.name
    assert queue_info.is_auto_delete is False
    assert queue_info.is_exclusive is False
    assert queue_info.is_durable is True
    assert (
        queue_specification.message_ttl.total_seconds() * 1000
    ) == queue_info.arguments["x-message-ttl"]
    assert queue_specification.overflow_behaviour == queue_info.arguments["x-overflow"]
    assert (
        queue_specification.auto_expires.total_seconds() * 1000
    ) == queue_info.arguments["x-expires"]

    assert (
        queue_specification.dead_letter_exchange
        == queue_info.arguments["x-dead-letter-exchange"]
    )
    assert (
        queue_specification.dead_letter_routing_key
        == queue_info.arguments["x-dead-letter-routing-key"]
    )
    assert queue_specification.max_len == queue_info.arguments["x-max-length"]
    assert (
        queue_specification.max_len_bytes == queue_info.arguments["x-max-length-bytes"]
    )

    assert (
        queue_specification.single_active_consumer
        == queue_info.arguments["x-single-active-consumer"]
    )

    assert queue_specification.deliver_limit == queue_info.arguments["x-deliver-limit"]
    assert (
        queue_specification.dead_letter_strategy
        == queue_info.arguments["x-dead-letter-strategy"]
    )
    assert (
        queue_specification.quorum_initial_group_size
        == queue_info.arguments["x-quorum-initial-group-size"]
    )
    assert (
        queue_specification.cluster_target_group_size
        == queue_info.arguments["x-quorum-target-group-size"]
    )

    management.delete_queue(queue_name)


def test_declare_stream_with_args(management: Management) -> None:

    stream_name = "test-stream_with_args"

    stream_specification = StreamSpecification(
        name=stream_name,
        max_len_bytes=1000,
        max_age=timedelta(seconds=200000),
        stream_max_segment_size_bytes=200,
        stream_filter_size_bytes=100,
        initial_group_size=5,
    )

    management.declare_queue(stream_specification)

    stream_info = management.queue_info(stream_name)

    assert stream_specification.name == stream_info.name
    assert stream_info.is_auto_delete is False
    assert stream_info.is_exclusive is False
    assert stream_info.is_durable is True
    assert (
        stream_specification.max_len_bytes
        == stream_info.arguments["x-max-length-bytes"]
    )
    assert (
        str(int(stream_specification.max_age.total_seconds())) + "s"
        == stream_info.arguments["x-max-age"]
    )
    assert (
        stream_specification.stream_max_segment_size_bytes
        == stream_info.arguments["x-stream-max-segment-size-bytes"]
    )
    assert (
        stream_specification.stream_filter_size_bytes
        == stream_info.arguments["x-stream-filter-size-bytes"]
    )
    assert (
        stream_specification.initial_group_size
        == stream_info.arguments["x-initial-group-size"]
    )

    management.delete_queue(stream_name)


def test_declare_classic_queue_with_invalid_args(management: Management) -> None:
    queue_name = "test-queue_with_args"
    test_failure = True

    queue_specification = ClassicQueueSpecification(
        name=queue_name,
        max_len=-5,
    )

    try:
        management.declare_queue(queue_specification)
    except ValidationCodeException:
        test_failure = False

    management.delete_queue(queue_name)

    assert test_failure is False
