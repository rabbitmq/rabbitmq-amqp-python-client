from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    QueueSpecification,
    QueueType,
)
from rabbitmq_amqp_python_client.exceptions import (
    ValidationCodeException,
)


def test_declare_delete_exchange() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    exchange_name = "test-exchange"
    management = connection.management()

    exchange_info = management.declare_exchange(
        ExchangeSpecification(name=exchange_name, arguments={})
    )

    assert exchange_info.name == exchange_name

    management.delete_exchange(exchange_name)

    connection.close()


def test_declare_purge_delete_queue() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    queue_name = "my_queue"
    management = connection.management()

    queue_info = management.declare_queue(
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum)
    )

    assert queue_info.name == queue_name

    management.purge_queue(queue_name)

    management.delete_queue(queue_name)

    connection.close()


def test_bind_exchange_to_queue() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    exchange_name = "test-bind-exchange-to-queue-exchange"
    queue_name = "test-bind-exchange-to-queue-queue"
    routing_key = "routing-key"
    management = connection.management()

    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum)
    )

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


def test_queue_info_with_validations() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    queue_name = "test_queue_info_with_validation"
    management = connection.management()

    queue_specification = QueueSpecification(
        name=queue_name,
        queue_type=QueueType.quorum,
    )
    management.declare_queue(queue_specification)

    queue_info = management.queue_info(queue_name=queue_name)

    management.delete_queue(queue_name)

    assert queue_info.name == queue_name
    assert queue_info.queue_type == queue_specification.queue_type
    assert queue_info.is_durable == queue_specification.is_durable
    assert queue_info.message_count == 0


def test_queue_precondition_fail() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    test_failure = True

    queue_name = "test-queue_precondition_fail"
    management = connection.management()

    queue_specification = QueueSpecification(
        name=queue_name, queue_type=QueueType.quorum, is_auto_delete=False
    )
    management.declare_queue(queue_specification)

    management.declare_queue(queue_specification)

    queue_specification = QueueSpecification(
        name=queue_name,
        queue_type=QueueType.quorum,
        is_auto_delete=True,
    )

    management.delete_queue(queue_name)

    try:
        management.declare_queue(queue_specification)
    except ValidationCodeException:
        test_failure = False

    assert test_failure is False


def test_declare_classic_queue() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    queue_name = "test-declare_classic_queue"
    management = connection.management()

    queue_specification = QueueSpecification(
        name=queue_name,
        queue_type=QueueType.classic,
        is_auto_delete=False,
    )
    queue_info = management.declare_queue(queue_specification)

    assert queue_info.name == queue_specification.name
    assert queue_info.queue_type == queue_specification.queue_type

    management.delete_queue(queue_name)


def test_declare_queue_with_args() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    queue_name = "test-queue_with_args"
    management = connection.management()

    queue_specification = QueueSpecification(
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
