from rabbitmq_amqp_python_client import (
    AddressHelper,
    ArgumentOutOfRangeException,
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    Message,
    QuorumQueueSpecification,
)


def test_publish_queue(connection: Connection) -> None:

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    publisher = None
    accepted = False

    try:
        publisher = connection.publisher("/queues/" + queue_name)
        status = publisher.publish(Message(body="test"))
        if status.ACCEPTED:
            accepted = True
    except Exception:
        raised = True

    if publisher is not None:
        publisher.close()

    management.delete_queue(queue_name)
    management.close()

    assert accepted is True
    assert raised is False


def test_publish_ssl(connection_ssl: Connection) -> None:

    queue_name = "test-queue"
    management = connection_ssl.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    try:
        publisher = connection_ssl.publisher("/queues/" + queue_name)
        publisher.publish(Message(body="test"))
    except Exception:
        raised = True

    publisher.close()

    management.delete_queue(queue_name)
    management.close()

    assert raised is False


def test_publish_to_invalid_destination(connection: Connection) -> None:

    queue_name = "test-queue"

    raised = False

    publisher = None
    try:
        publisher = connection.publisher("/invalid-destination/" + queue_name)
        publisher.publish(Message(body="test"))
    except ArgumentOutOfRangeException:
        raised = True
    except Exception:
        raised = False

    if publisher is not None:
        publisher.close()

    assert raised is True


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

    addr = AddressHelper.exchange_address(exchange_name, routing_key)

    raised = False
    accepted = False

    try:
        publisher = connection.publisher(addr)
        status = publisher.publish(Message(body="test"))
        if status.ACCEPTED:
            accepted = True
    except Exception:
        raised = True

    publisher.close()

    management.delete_exchange(exchange_name)
    management.delete_queue(queue_name)
    management.close()

    assert accepted is True
    assert raised is False


def test_publish_purge(connection: Connection) -> None:
    messages_to_publish = 20

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    try:
        publisher = connection.publisher("/queues/" + queue_name)
        for i in range(messages_to_publish):
            publisher.publish(Message(body="test"))
    except Exception:
        raised = True

    publisher.close()

    message_purged = management.purge_queue(queue_name)

    management.delete_queue(queue_name)
    management.close()

    assert raised is False
    assert message_purged == 20
