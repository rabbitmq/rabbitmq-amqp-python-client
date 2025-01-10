from rabbitmq_amqp_python_client import (
    BindingSpecification,
    ClassicQueueSpecification,
    ExchangeSpecification,
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
        ExchangeSpecification(name=exchange_name, arguments={})
    )

    assert exchange_info.name == exchange_name

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

    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    binding_exchange_queue_path = management.bind(
        BindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    print(binding_exchange_queue_path)

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


def test_queue_info_with_validations(management: Management) -> None:

    queue_name = "test_queue_info_with_validation"

    queue_specification = QuorumQueueSpecification(
        name=queue_name,
    )
    management.declare_queue(queue_specification)

    queue_info = management.queue_info(queue_name=queue_name)

    management.delete_queue(queue_name)

    assert queue_info.name == queue_name
    assert queue_info.queue_type == queue_specification.queue_type
    assert queue_info.is_durable == queue_specification.is_durable
    assert queue_info.message_count == 0


def test_queue_info_for_stream_with_validations(management: Management) -> None:

    stream_name = "test_stream_info_with_validation"

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management.declare_queue(queue_specification)

    stream_info = management.queue_info(queue_name=stream_name)

    management.delete_queue(stream_name)

    assert stream_info.name == stream_name
    assert stream_info.queue_type == queue_specification.queue_type
    assert stream_info.message_count == 0


def test_queue_precondition_fail(management: Management) -> None:
    test_failure = True

    queue_name = "test-queue_precondition_fail"

    queue_specification = QuorumQueueSpecification(
        name=queue_name, is_auto_delete=False
    )
    management.declare_queue(queue_specification)

    management.declare_queue(queue_specification)

    queue_specification = QuorumQueueSpecification(
        name=queue_name,
        is_auto_delete=True,
    )

    management.delete_queue(queue_name)

    try:
        management.declare_queue(queue_specification)
    except ValidationCodeException:
        test_failure = False

    assert test_failure is False


def test_declare_classic_queue(management: Management) -> None:

    queue_name = "test-declare_classic_queue"

    queue_specification = QuorumQueueSpecification(
        name=queue_name,
        queue_type=QueueType.classic,
        is_auto_delete=False,
    )
    queue_info = management.declare_queue(queue_specification)

    assert queue_info.name == queue_specification.name
    assert queue_info.queue_type == queue_specification.queue_type

    management.delete_queue(queue_name)


def test_declare_classic_queue_with_args(management: Management) -> None:

    queue_name = "test-queue_with_args"

    queue_specification = ClassicQueueSpecification(
        name=queue_name,
        queue_type=QueueType.classic,
        is_auto_delete=False,
        dead_letter_exchange="my_exchange",
        dead_letter_routing_key="my_key",
        max_len=50000000,
        max_len_bytes=1000000000,
        expires=2000,
        single_active_consumer=True,
    )

    queue_info = management.declare_queue(queue_specification)

    assert queue_specification.name == queue_info.name
    assert queue_specification.is_auto_delete == queue_info.is_auto_delete
    assert queue_specification.dead_letter_exchange == queue_info.dead_letter_exchange
    assert (
        queue_specification.dead_letter_routing_key
        == queue_info.dead_letter_routing_key
    )
    assert queue_specification.max_len == queue_info.max_len
    assert queue_specification.max_len_bytes == queue_info.max_len_bytes
    assert queue_specification.expires == queue_info.expires
    assert (
        queue_specification.single_active_consumer == queue_info.single_active_consumer
    )

    management.delete_queue(queue_name)


def test_declare_classic_queue_with_invalid_args(management: Management) -> None:
    queue_name = "test-queue_with_args"
    test_failure = True

    queue_specification = ClassicQueueSpecification(
        name=queue_name,
        queue_type=QueueType.classic,
        max_len=-5,
    )

    try:
        management.declare_queue(queue_specification)
    except ValidationCodeException:
        test_failure = False

    management.delete_queue(queue_name)

    assert test_failure is False


def test_declare_stream_with_args(management: Management) -> None:
    stream_name = "test-stream_with_args"

    stream_specification = StreamSpecification(
        name=stream_name,
        max_len_bytes=1000000000,
        max_time_retention=10000000,
        max_segment_size_in_bytes=100000000,
        filter_size=1000,
        initial_group_size=3,
        leader_locator="node1",
    )

    stream_info = management.declare_queue(stream_specification)

    assert stream_specification.name == stream_info.name
    assert stream_specification.max_len_bytes == stream_info.max_len_bytes
    assert stream_specification.max_time_retention == stream_info.max_time_retention
    assert (
        stream_specification.max_segment_size_in_bytes
        == stream_info.max_segment_size_in_bytes
    )
    assert stream_specification.filter_size == stream_info.filter_size
    assert stream_specification.initial_group_size == stream_info.initial_group_size
    assert stream_specification.leader_locator == stream_info.leader_locator

    management.delete_queue(stream_name)
