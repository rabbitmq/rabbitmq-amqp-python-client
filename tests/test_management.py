from rabbitmq_amqp_python_client import (
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

    # Still not working
    # management.delete_exchange(exchange_name)

    connection.close()


def test_declare_delete_queue() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    queue_name = "test-queue"
    management = connection.management()

    exchange_info = management.declare_queue(
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum, arguments={})
    )

    assert exchange_info.name == queue_name

    # Still not working
    # management.delete_queue(queue_name)

    connection.close()
