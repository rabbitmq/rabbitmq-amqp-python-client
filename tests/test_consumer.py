from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    ExchangeType,
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
    MyMessageHandlerRequeueWithAnnotations,
)
from .utils import create_connection


def test_consumer_sync_queue_accept(connection: Connection) -> None:

    queue_name = "test-queue-sync-accept"
    messages_to_send = 100
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)
    consumer = connection.consumer(addr_queue)

    consumed = 0

    # publish messages_to_send messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))

    publisher.close()

    # consumer synchronously without handler
    for i in range(messages_to_send):
        message = consumer.consume()
        if message.body == "test" + str(i):
            consumed = consumed + 1

    consumer.close()

    assert consumed > 0

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

    consumer.close()

    message_count = management.purge_queue(queue_name)

    management.close()

    assert message_count == 0


def test_consumer_async_queue_no_ack(connection: Connection) -> None:

    messages_to_send = 1000

    queue_name = "test-queue_async_no_ack"

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

    exchange_dead_lettering = "exchange-dead-letter"
    queue_dead_lettering = "queue-dead-letter"
    queue_name = "test-queue_async_discard"
    binding_key = "key_dead_letter"

    management = connection.management()

    # configuring dead lettering
    management.declare_exchange(
        ExchangeSpecification(
            name=exchange_dead_lettering,
            exchange_type=ExchangeType.fanout,
            arguments={},
        )
    )
    management.declare_queue(QuorumQueueSpecification(name=queue_dead_lettering))
    management.bind(
        BindingSpecification(
            source_exchange=exchange_dead_lettering,
            destination_queue=queue_dead_lettering,
            binding_key=binding_key,
        )
    )

    management.declare_queue(
        QuorumQueueSpecification(
            name=queue_name,
            dead_letter_exchange=exchange_dead_lettering,
            dead_letter_routing_key=binding_key,
        )
    )

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

    message_count_dead_lettering = management.purge_queue(queue_dead_lettering)

    management.delete_queue(queue_dead_lettering)

    management.close()

    assert message_count == 0
    # check dead letter queue
    assert message_count_dead_lettering == messages_to_send


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


def test_consumer_async_queue_with_requeue_with_annotations(
    connection: Connection,
) -> None:
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

    assert "x-opt-string" in message.annotations

    message_count = management.purge_queue(queue_name)

    management.delete_queue(queue_name)
    management.close()

    assert message_count > 0
