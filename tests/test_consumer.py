from rabbitmq_amqp_python_client import (
    AddressHelper,
    ArgumentOutOfRangeException,
    Connection,
    QuorumQueueSpecification,
)

from .conftest import (
    ConsumerTestException,
    MyMessageHandlerAccept,
    MyMessageHandlerDiscard,
    MyMessageHandlerDiscardWithAnnotations,
    MyMessageHandlerNoack,
    MyMessageHandlerRequeue,
    MyMessageHandlerRequeueWithAnnotations,
    MyMessageHandlerRequeueWithInvalidAnnotations,
)
from .utils import (
    cleanup_dead_lettering,
    create_connection,
    publish_messages,
    setup_dead_lettering,
)


def test_consumer_sync_queue_accept(connection: Connection) -> None:

    queue_name = "test-queue-sync-accept"
    messages_to_send = 100
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)
    consumer = connection.consumer(addr_queue)

    consumed = 0

    # publish messages_to_send messages
    publish_messages(connection, messages_to_send, queue_name)

    # consumer synchronously without handler
    for i in range(messages_to_send):
        message = consumer.consume()
        if message.body == "test" + str(i):
            consumed = consumed + 1

    consumer.close()

    management.delete_queue(queue_name)
    management.close()

    assert consumed > 0


def test_consumer_invalid_destination(connection: Connection) -> None:

    queue_name = "test-queue-sync-invalid-accept"
    raised = False
    consumer = None
    try:
        consumer = connection.consumer("/invalid-destination/" + queue_name)
    except ArgumentOutOfRangeException:
        raised = True
    except Exception:
        raised = False

    if consumer is not None:
        consumer.close()

    assert raised is True


def test_consumer_async_queue_accept(connection: Connection) -> None:

    messages_to_send = 1000

    queue_name = "test-queue-async-accept"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = create_connection()
    consumer = connection_consumer.consumer(
        addr_queue, handler=MyMessageHandlerAccept()
    )

    try:
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)

    management.close()

    assert message_count == 0


def test_consumer_async_queue_no_ack(connection: Connection) -> None:

    messages_to_send = 1000

    queue_name = "test-queue-async-no-ack"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = create_connection()

    consumer = connection_consumer.consumer(addr_queue, handler=MyMessageHandlerNoack())

    try:
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)

    management.close()

    assert message_count > 0


def test_consumer_async_queue_with_discard(connection: Connection) -> None:
    messages_to_send = 1000

    queue_dead_lettering = "queue-dead-letter"
    queue_name = "test-queue-async-discard"
    exchange_dead_lettering = "exchange-dead-letter"
    binding_key = "key-dead-letter"

    management = connection.management()

    # configuring dead lettering
    bind_path = setup_dead_lettering(management)
    addr_queue = AddressHelper.queue_address(queue_name)

    management.declare_queue(
        QuorumQueueSpecification(
            name=queue_name,
            dead_letter_exchange=exchange_dead_lettering,
            dead_letter_routing_key=binding_key,
        )
    )

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = create_connection()

    consumer = connection_consumer.consumer(
        addr_queue, handler=MyMessageHandlerDiscard()
    )

    try:
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)

    message_count_dead_lettering = management.purge_queue(queue_dead_lettering)

    cleanup_dead_lettering(management, bind_path)

    management.close()

    assert message_count == 0
    # check dead letter queue
    assert message_count_dead_lettering == messages_to_send


def test_consumer_async_queue_with_discard_with_annotations(
    connection: Connection,
) -> None:
    messages_to_send = 1000

    queue_dead_lettering = "queue-dead-letter"
    queue_name = "test-queue-async-discard"
    exchange_dead_lettering = "exchange-dead-letter"
    binding_key = "key-dead-letter"

    management = connection.management()

    management.declare_queue(
        QuorumQueueSpecification(
            name=queue_name,
            dead_letter_exchange=exchange_dead_lettering,
            dead_letter_routing_key=binding_key,
        )
    )

    publish_messages(connection, messages_to_send, queue_name)

    bind_path = setup_dead_lettering(management)
    addr_queue = AddressHelper.queue_address(queue_name)
    addr_queue_dl = AddressHelper.queue_address(queue_dead_lettering)

    # we closed the connection so we need to open a new one
    connection_consumer = create_connection()

    consumer = connection_consumer.consumer(
        addr_queue, handler=MyMessageHandlerDiscardWithAnnotations()
    )

    try:
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    # check for added annotation
    new_consumer = connection.consumer(addr_queue_dl)
    message = new_consumer.consume()
    new_consumer.close()

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)

    message_count_dead_lettering = management.purge_queue(queue_dead_lettering)

    cleanup_dead_lettering(management, bind_path)

    management.close()

    assert "x-opt-string" in message.annotations

    assert message_count == 0
    # check dead letter queue
    assert message_count_dead_lettering == messages_to_send


def test_consumer_async_queue_with_requeue(connection: Connection) -> None:
    messages_to_send = 1000

    queue_name = "test-queue-async-requeue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = create_connection()

    consumer = connection_consumer.consumer(
        addr_queue, handler=MyMessageHandlerRequeue()
    )

    try:
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)
    management.close()

    assert message_count > 0


def test_consumer_async_queue_with_requeue_with_annotations(
    connection: Connection,
) -> None:
    messages_to_send = 1000

    queue_name = "test-queue-async-requeue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = create_connection()

    consumer = connection_consumer.consumer(
        addr_queue, handler=MyMessageHandlerRequeueWithAnnotations()
    )

    try:
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    # check for added annotation
    new_consumer = connection.consumer(addr_queue)
    message = new_consumer.consume()
    new_consumer.close()

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)
    management.close()

    assert "x-opt-string" in message.annotations

    assert message_count > 0


def test_consumer_async_queue_with_requeue_with_invalid_annotations(
    connection: Connection,
) -> None:
    messages_to_send = 1000
    test_failure = True

    queue_name = "test-queue-async-requeue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = create_connection()

    try:
        consumer = connection_consumer.consumer(
            addr_queue, handler=MyMessageHandlerRequeueWithInvalidAnnotations()
        )

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    except ArgumentOutOfRangeException:
        test_failure = False

    consumer.close()

    management.delete_queue(queue_name)
    management.close()

    assert test_failure is False
