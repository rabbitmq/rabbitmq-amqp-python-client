from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    QueueSpecification,
    QueueType,
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
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum, arguments={})
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
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum, arguments={})
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
