import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AmqpMessage,
    ArgumentOutOfRangeException,
    BindingSpecification,
    Connection,
    ConnectionClosed,
    Environment,
    ExchangeSpecification,
    QuorumQueueSpecification,
    StreamSpecification,
)

from .http_requests import delete_all_connections


def test_publish_queue(connection: Connection) -> None:

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    publisher = None
    accepted = False

    try:
        publisher = connection.publisher("/queues/" + queue_name)
        status = publisher.publish(AmqpMessage(body="test"))
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
        publisher.publish(AmqpMessage(body="test"))
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
        publisher.publish(AmqpMessage(body="test"))
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

    management.declare_exchange(ExchangeSpecification(name=exchange_name))

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
        status = publisher.publish(AmqpMessage(body="test"))
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
            publisher.publish(AmqpMessage(body="test"))
    except Exception:
        raised = True

    publisher.close()

    message_purged = management.purge_queue(queue_name)

    management.delete_queue(queue_name)
    management.close()

    assert raised is False
    assert message_purged == 20


def test_disconnection_reconnection() -> None:
    disconnected = False
    reconnected = False
    generic_exception_raised = False
    publisher = None
    queue_name = "test-queue"
    connection_test = None
    environment = Environment()

    def on_disconnected():

        nonlocal publisher
        nonlocal queue_name
        nonlocal connection_test

        # reconnect
        if connection_test is not None:
            connection_test = environment.connection(
                "amqp://guest:guest@localhost:5672/"
            )
            connection_test.dial()

        if publisher is not None:
            publisher = connection_test.publisher("/queues/" + queue_name)

        nonlocal reconnected
        reconnected = True

    connection_test = environment.connection(
        "amqp://guest:guest@localhost:5672/", on_disconnection_handler=on_disconnected
    )
    connection_test.dial()
    # delay
    time.sleep(5)
    messages_to_publish = 10000
    queue_name = "test-queue"
    management = connection_test.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    management.close()

    publisher = connection_test.publisher("/queues/" + queue_name)
    while True:

        for i in range(messages_to_publish):
            if i == 5:
                # simulate a disconnection
                delete_all_connections()
            try:
                publisher.publish(AmqpMessage(body="test"))

            except ConnectionClosed:
                disconnected = True
                continue

            except Exception:
                generic_exception_raised = True

        break

    publisher.close()

    # cleanup, we need to create a new connection as the previous one
    # was closed by the test

    connection_test = Connection("amqp://guest:guest@localhost:5672/")
    connection_test.dial()

    management = connection_test.management()

    # purge the queue and check number of published messages
    message_purged = management.purge_queue(queue_name)

    management.delete_queue(queue_name)
    management.close()

    environment.close()

    assert generic_exception_raised is False
    assert disconnected is True
    assert reconnected is True
    assert message_purged == messages_to_publish - 1


def test_queue_info_for_stream_with_validations(connection: Connection) -> None:

    stream_name = "test_stream_info_with_validation"
    messages_to_send = 200

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    print("before creating publisher")

    publisher = connection.publisher("/queues/" + stream_name)

    print("after creating publisher")

    for i in range(messages_to_send):

        publisher.publish(AmqpMessage(body="test"))
