import time

from rabbitmq_amqp_python_client import (
    Connection,
    Message,
    QuorumQueueSpecification,
)


def test_publish_exchange(connection: Connection) -> None:

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


def test_publish_purge(connection: Connection) -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    try:
        publisher = connection.publisher("/queues/" + queue_name)
        for i in range(20):
            publisher.publish(Message(body="test"))
    except Exception:
        raised = True

    time.sleep(4)

    message_purged = management.purge_queue(queue_name)

    assert raised is False
    assert message_purged == 20

    publisher.close()

    management.delete_queue(queue_name)
