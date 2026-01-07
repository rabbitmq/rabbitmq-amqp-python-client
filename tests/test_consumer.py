import time
from datetime import datetime

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    ArgumentOutOfRangeException,
    Connection,
    Environment,
    Event,
    Message,
    QuorumQueueSpecification,
    StreamConsumerOptions,
    StreamSpecification,
)
from rabbitmq_amqp_python_client.utils import Converter

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
        if Converter.bytes_to_string(message.body) == "test{}".format(i):
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


def test_consumer_async_queue_accept(
    connection: Connection, environment: Environment
) -> None:

    messages_to_send = 1000

    queue_name = "test-queue-async-accept"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = environment.connection()
    connection_consumer.dial()
    consumer = connection_consumer.consumer(
        addr_queue, message_handler=MyMessageHandlerAccept()
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


def test_consumer_async_queue_no_ack(
    connection: Connection, environment: Environment
) -> None:

    messages_to_send = 1000

    queue_name = "test-queue-async-no-ack"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = environment.connection()
    connection_consumer.dial()

    consumer = connection_consumer.consumer(
        addr_queue, message_handler=MyMessageHandlerNoack()
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


def test_consumer_async_queue_with_discard(
    connection: Connection, environment: Environment
) -> None:
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
    connection_consumer = environment.connection()
    connection_consumer.dial()

    consumer = connection_consumer.consumer(
        addr_queue, message_handler=MyMessageHandlerDiscard()
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
    connection: Connection, environment: Environment
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
    connection_consumer = environment.connection()
    connection_consumer.dial()

    consumer = connection_consumer.consumer(
        addr_queue, message_handler=MyMessageHandlerDiscardWithAnnotations()
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


def test_consumer_async_queue_with_requeue(
    connection: Connection, environment: Environment
) -> None:
    messages_to_send = 1000

    queue_name = "test-queue-async-requeue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = environment.connection()
    connection_consumer.dial()

    consumer = connection_consumer.consumer(
        addr_queue, message_handler=MyMessageHandlerRequeue()
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
    connection: Connection, environment: Environment
) -> None:
    messages_to_send = 1000

    queue_name = "test-queue-async-requeue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = environment.connection()
    connection_consumer.dial()

    consumer = connection_consumer.consumer(
        addr_queue, message_handler=MyMessageHandlerRequeueWithAnnotations()
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
    environment: Environment,
) -> None:
    messages_to_send = 1000
    test_failure = True

    queue_name = "test-queue-async-requeue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    publish_messages(connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = environment.connection()
    connection_consumer.dial()

    try:
        consumer = connection_consumer.consumer(
            addr_queue, message_handler=MyMessageHandlerRequeueWithInvalidAnnotations()
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


class MyMessageHandlerDatetimeOffset(AMQPMessagingHandler):
    def __init__(self, expected_prefix: str, expected_count: int):
        super().__init__()
        self._received = 0
        self._expected_prefix = expected_prefix
        self._expected_count = expected_count

    def on_message(self, event: Event):
        message_body = Converter.bytes_to_string(event.message.body)
        # Verify that we only receive messages with the expected prefix
        assert message_body.startswith(self._expected_prefix), (
            f"Expected message to start with '{self._expected_prefix}', "
            f"but got '{message_body}'"
        )
        self.delivery_context.accept(event)
        self._received = self._received + 1
        if self._received == self._expected_count:
            raise ConsumerTestException("consumed")


def test_stream_consumer_offset_datetime(
    connection: Connection, environment: Environment
) -> None:
    """
    Test that StreamConsumerOptions with offset_specification=datetime.now()
    only consumes messages published after the specified datetime.
    """
    consumer = None
    stream_name = "test-stream-consumer-datetime-offset"
    messages_before = 10
    messages_after = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # Publish first set of messages
    publisher = connection.publisher(addr_queue)
    for i in range(messages_before):
        publisher.publish(
            Message(
                body=Converter.string_to_bytes(f"Before datetime: {i}"),
            )
        )
    publisher.close()

    # Wait a bit to ensure timestamp difference
    time.sleep(2)

    # Capture the datetime - consumer should only receive messages after this
    starting_from_here = datetime.now()

    # Wait a tiny bit to ensure messages are published after the datetime
    time.sleep(0.1)

    # Publish second set of messages after the datetime
    publisher = connection.publisher(addr_queue)
    for i in range(messages_after):
        publisher.publish(
            Message(
                body=Converter.string_to_bytes(f"After datetime: {i}"),
            )
        )
    publisher.close()

    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerDatetimeOffset(
                expected_prefix="After datetime:", expected_count=messages_after
            ),
            consumer_options=StreamConsumerOptions(
                offset_specification=starting_from_here
            ),
        )

        consumer.run()
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            consumer.close()
        management.delete_queue(stream_name)
        management.close()


def test_stream_consumer_offset_datetime_no_messages_before(
    connection: Connection, environment: Environment
) -> None:
    """
    Test that StreamConsumerOptions with offset_specification=datetime.now()
    does not consume any messages when all messages were published before the datetime.
    """
    consumer = None
    stream_name = "test-stream-consumer-datetime-offset-no-before"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # Publish messages
    publisher = connection.publisher(addr_queue)
    for i in range(messages_to_send):
        publisher.publish(
            Message(
                body=Converter.string_to_bytes(f"Message: {i}"),
            )
        )
    publisher.close()

    # Wait a bit
    time.sleep(2)

    # Capture a datetime after all messages were published
    starting_from_here = datetime.now()

    # Wait a bit more to ensure the datetime is definitely after all messages
    time.sleep(0.5)

    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            consumer_options=StreamConsumerOptions(
                offset_specification=starting_from_here
            ),
        )

        # Try to consume with a short timeout - should not receive any messages
        try:
            consumer.consume(timeout=1)
            # If we get here, we received a message which is unexpected
            assert False, "Expected no messages to be consumed"
        except Exception:
            # Expected - no messages should be available
            pass
    finally:
        if consumer is not None:
            consumer.close()
        management.delete_queue(stream_name)
        management.close()


def test_stream_consumer_offset_datetime_mixed_messages(
    connection: Connection, environment: Environment
) -> None:
    """
    Test that StreamConsumerOptions with offset_specification=datetime.now()
    correctly filters messages when messages are published both before and after.
    """
    consumer = None
    stream_name = "test-stream-consumer-datetime-offset-mixed"
    messages_before = 5
    messages_after = 5

    queue_specification = StreamSpecification(name=stream_name)
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # Publish first batch
    publisher = connection.publisher(addr_queue)
    for i in range(messages_before):
        publisher.publish(
            Message(
                body=Converter.string_to_bytes(f"Batch1: {i}"),
            )
        )
    publisher.close()

    # Wait and capture datetime
    time.sleep(2)
    starting_from_here = datetime.now()
    time.sleep(0.1)

    # Publish second batch after datetime
    publisher = connection.publisher(addr_queue)
    for i in range(messages_after):
        publisher.publish(
            Message(
                body=Converter.string_to_bytes(f"Batch2: {i}"),
            )
        )
    publisher.close()

    # Publish a third batch after the datetime (should be consumed)
    time.sleep(0.1)
    publisher = connection.publisher(addr_queue)
    for i in range(3):
        publisher.publish(
            Message(
                body=Converter.string_to_bytes(f"Batch3: {i}"),
            )
        )
    publisher.close()

    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerDatetimeOffset(
                expected_prefix="Batch", expected_count=messages_after + 3
            ),
            consumer_options=StreamConsumerOptions(
                offset_specification=starting_from_here
            ),
        )

        consumer.run()
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            consumer.close()
        management.delete_queue(stream_name)
        management.close()
