from rabbitmq_amqp_python_client import (
    Connection,
    Message,
    QuorumQueueSpecification,
    queue_address,
)

from .conftest import (
    ConsumerTestException,
    MyMessageHandlerAccept,
    MyMessageHandlerDiscard,
    MyMessageHandlerNoack,
    MyMessageHandlerRequeue,
    create_connection,
)


def test_consumer_sync_queue_ack(connection: Connection) -> None:

    queue_name = "test-queue-ack"
    messages_to_send = 100
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)
    consumer = connection.consumer(addr_queue)

    consumed = 0

    # publish 10 messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))

    # consumer synchronously without handler
    for i in range(messages_to_send):
        message = consumer.consume()
        if message.body == "test" + str(i):
            consumed = consumed + 1

    assert consumed > 0

    publisher.close()
    consumer.close()

    management.delete_queue(queue_name)
    management.close()


def test_consumer_async_queue_accept(connection: Connection) -> None:

    messages_to_send = 1000

    queue_name = "test-queue_async_accept"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)

    # publish messages_to_send messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))
    publisher.close()

    # workaround: it looks like when the consumer finish to consume invalidate the connection
    # so for the moment we need to use one dedicated
    connection_consumer = create_connection()
    consumer = connection_consumer.consumer(
        addr_queue, handler=MyMessageHandlerAccept()
    )

    try:
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)

    management.close()

    assert message_count == 0


def test_consumer_async_queue_noack(connection: Connection) -> None:

    messages_to_send = 1000

    queue_name = "test-queue_async_noack"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)

    # publish messages_to_send messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))
    publisher.close()

    # workaround: it looks like when the consumer finish to consume invalidate the connection
    # so for the moment we need to use one dedicated
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

    queue_name = "test-queue_async_discard"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)

    # publish messages_to_send messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))
    publisher.close()

    # workaround: it looks like when the consumer finish to consume invalidate the connection
    # so for the moment we need to use one dedicated
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

    management.close()

    assert message_count == 0


def test_consumer_async_queue_with_requeue(connection: Connection) -> None:
    messages_to_send = 1000

    queue_name = "test-queue_async_requeue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)

    # publish messages_to_send messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))
    publisher.close()

    # workaround: it looks like when the consumer finish to consume invalidate the connection
    # so for the moment we need to use one dedicated
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
