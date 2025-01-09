from rabbitmq_amqp_python_client import (
    Connection,
    Message,
    QueueSpecification,
    QueueType,
)


def test_bind_exchange_to_queue() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum)
    )

    raised = False

    try:
        publisher = connection.publisher("/queues/" + queue_name)
        publisher.publish(Message(body="test"))
    except Exception:
        raised = True

    assert raised is False

    publisher.close()

    management.delete_queue(queue_name)
