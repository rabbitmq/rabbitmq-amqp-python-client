import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    ArgumentOutOfRangeException,
    Connection,
    ConnectionClosed,
    Environment,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
    StreamSpecification,
    ValidationCodeException,
)

from .http_requests import delete_all_connections
from .utils import create_binding, publish_per_message


def test_publish_queue(connection: Connection) -> None:

    queue_name = "test-queue"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    publisher = None
    accepted = False

    try:
        publisher = connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
        status = publisher.publish(Message(body="test"))
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
    except Exception:
        raised = True

    if publisher is not None:
        publisher.close()

    management.delete_queue(queue_name)
    management.close()

    assert accepted is True
    assert raised is False


def test_publish_per_message(connection: Connection) -> None:

    queue_name = "test-queue-1"
    queue_name_2 = "test-queue-2"
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))
    management.declare_queue(QuorumQueueSpecification(name=queue_name_2))

    raised = False

    publisher = None
    accepted = False
    accepted_2 = True

    try:
        publisher = connection.publisher()
        status = publish_per_message(
            publisher, addr=AddressHelper.queue_address(queue_name)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
        status = publish_per_message(
            publisher, addr=AddressHelper.queue_address(queue_name_2)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted_2 = True
    except Exception:
        raised = True

    if publisher is not None:
        publisher.close()

    purged_messages_queue_1 = management.purge_queue(queue_name)
    purged_messages_queue_2 = management.purge_queue(queue_name_2)
    management.delete_queue(queue_name)
    management.delete_queue(queue_name_2)
    management.close()

    assert accepted is True
    assert accepted_2 is True
    assert purged_messages_queue_1 == 1
    assert purged_messages_queue_2 == 1
    assert raised is False


def test_publish_ssl(connection_ssl: Connection) -> None:

    queue_name = "test-queue"
    management = connection_ssl.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    try:
        publisher = connection_ssl.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
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


def test_publish_per_message_to_invalid_destination(connection: Connection) -> None:

    queue_name = "test-queue-1"
    raised = False

    message = Message(body="test")
    message = AddressHelper.message_to_address_helper(
        message, "/invalid_destination/" + queue_name
    )
    publisher = connection.publisher()

    try:
        publisher.publish(message)
    except ArgumentOutOfRangeException:
        raised = True
    except Exception:
        raised = False

    if publisher is not None:
        publisher.close()

    assert raised is True


def test_publish_per_message_both_address(connection: Connection) -> None:

    queue_name = "test-queue-1"
    raised = False

    management = connection.management()
    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    publisher = connection.publisher(
        destination=AddressHelper.queue_address(queue_name)
    )

    try:
        message = Message(body="test")
        message = AddressHelper.message_to_address_helper(
            message, AddressHelper.queue_address(queue_name)
        )
        publisher.publish(message)
    except ValidationCodeException:
        raised = True

    if publisher is not None:
        publisher.close()

    management.delete_queue(queue_name)
    management.close()

    assert raised is True


def test_publish_exchange(connection: Connection) -> None:

    exchange_name = "test-exchange"
    queue_name = "test-queue"
    management = connection.management()
    routing_key = "routing-key"

    bind_name = create_binding(management, exchange_name, queue_name, routing_key)

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

    management.unbind(bind_name)
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
        publisher = connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
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


def test_disconnection_reconnection() -> None:
    disconnected = False
    generic_exception_raised = False

    environment = Environment("amqp://guest:guest@localhost:5672/", reconnect=True)

    connection_test = environment.connection()

    connection_test.dial()
    # delay
    time.sleep(5)
    messages_to_publish = 10000
    queue_name = "test-queue"
    management = connection_test.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    management.close()

    publisher = connection_test.publisher(
        destination=AddressHelper.queue_address(queue_name)
    )
    while True:

        for i in range(messages_to_publish):
            if i == 5:
                # simulate a disconnection
                delete_all_connections()
            try:
                publisher.publish(Message(body="test"))

            except ConnectionClosed:
                disconnected = True
                publisher = connection_test.publisher(
                    destination=AddressHelper.queue_address(queue_name)
                )
                continue

            except Exception:
                generic_exception_raised = True

        break

    publisher.close()

    # cleanup, we need to create a new connection as the previous one
    # was closed by the test

    connection_test = environment.connection()
    connection_test.dial()

    management = connection_test.management()

    # purge the queue and check number of published messages
    message_purged = management.purge_queue(queue_name)

    management.delete_queue(queue_name)
    management.close()

    environment.close()

    assert generic_exception_raised is False
    assert disconnected is True
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

    publisher = connection.publisher(
        destination=AddressHelper.queue_address(stream_name)
    )

    print("after creating publisher")

    for i in range(messages_to_send):

        publisher.publish(Message(body="test"))


def test_publish_per_message_exchange(connection: Connection) -> None:

    exchange_name = "test-exchange-per-message"
    queue_name = "test-queue-per-message"
    management = connection.management()
    routing_key = "routing-key-per-message"

    bind_name = create_binding(management, exchange_name, queue_name, routing_key)

    raised = False

    publisher = None
    accepted = False
    accepted_2 = False

    try:
        publisher = connection.publisher()
        status = publish_per_message(
            publisher, addr=AddressHelper.exchange_address(exchange_name, routing_key)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
        status = publish_per_message(
            publisher, addr=AddressHelper.queue_address(queue_name)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted_2 = True
    except Exception:
        raised = True

    # if publisher is not None:
    publisher.close()

    purged_messages_queue = management.purge_queue(queue_name)
    management.unbind(bind_name)
    management.delete_exchange(exchange_name)
    management.delete_queue(queue_name)

    management.close()

    assert accepted is True
    assert accepted_2 is True
    assert purged_messages_queue == 2
    assert raised is False
