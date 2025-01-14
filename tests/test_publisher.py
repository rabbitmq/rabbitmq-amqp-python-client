import time

from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    Message,
    QuorumQueueSpecification,
    exchange_address,
)


def test_publish_queue(connection: Connection) -> None:

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    try:
        publisher = connection.publisher("/queues/" + queue_name)
        publisher.publish(Message(body="test"))
    except Exception:
        raised = True

    assert raised is False

    publisher.close()

    management.delete_queue(queue_name)
    management.close()


def test_publish_exchange(connection: Connection) -> None:

    exchange_name = "test-exchange"
    queue_name = "test-queue"
    management = connection.management()
    routing_key = "routing-key"

    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    management.bind(
        BindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    addr = exchange_address(exchange_name, routing_key)

    raised = False

    try:
        publisher = connection.publisher(addr)
        publisher.publish(Message(body="test"))
    except Exception:
        raised = True

    assert raised is False

    publisher.close()

    management.delete_exchange(exchange_name)
    management.delete_queue(queue_name)
    management.close()


def test_publish_purge(connection: Connection) -> None:

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    try:
        publisher = connection.publisher("/queues/" + queue_name)
        for i in range(100):
            publisher.publish(Message(body="test"))
    except Exception:
        raised = True

    time.sleep(5)

    message_purged = management.purge_queue(queue_name)

    assert raised is False
    assert message_purged == 100

    publisher.close()

    management.delete_queue(queue_name)
    management.close()
