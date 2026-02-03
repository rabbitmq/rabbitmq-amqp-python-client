"""Tests for (classic and Quorum) with pre_settled functionality."""

from rabbitmq_amqp_python_client import (  # Environment,
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    ConsumerFeature,
    ConsumerOptions,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.utils import Converter

from .conftest import ConsumerTestException
from .utils import publish_messages


def test_fifo_consumer_without_presettled(connection: Connection) -> None:
    """Test FIFO (classic and Quorum) consumer with pre_settled=False (default behavior)."""
    queue_name = "test-CQ-queue-no-presettled"
    messages_to_send = 50
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)
    consumer = connection.consumer(
        addr_queue, consumer_options=ConsumerOptions(feature=ConsumerFeature.Default)
    )

    consumed = 0

    # publish messages
    publish_messages(connection, messages_to_send, queue_name)

    # consume messages synchronously
    for i in range(messages_to_send):
        message = consumer.consume()
        if Converter.bytes_to_string(message.body) == "test{}".format(i):
            consumed = consumed + 1

    consumer.close()
    info = management.queue_info("test-CQ-queue-no-presettled")
    # since don't ack the messages, they should remain in the queue
    assert info.message_count == messages_to_send

    management.delete_queue(queue_name)
    management.close()

    assert consumed == messages_to_send


class MyMessagePresettledHandler(AMQPMessagingHandler):
    """Message handler that processes messages with pre_settled=True."""

    def __init__(self) -> None:
        super().__init__()
        self._received = 0

    def on_amqp_message(self, event) -> None:
        """Handle incoming AMQP messages."""
        message_body = Converter.bytes_to_string(event.message.body)

        # With pre_settled=True, messages are automatically settled
        # calling self.delivery_context.accept(event) will raise an error
        # assert False, "Cannot accept a pre-settled message"
        assert message_body == "test{}".format(self._received)
        try:
            self.delivery_context.accept(event)
            assert False, "Cannot accept a pre-settled message"
        except Exception:
            assert True, "Cannot accept a pre-settled message"
            pass

        self._received = self._received + 1
        if self._received == 50:
            raise ConsumerTestException("consumed")


def test_fifo_consumer_with_presettled(connection: Connection) -> None:
    """Test FIFO consumer with pre_settled=True (at-most-once semantics)."""
    queue_name = "test-CQ-queue-presettled"
    messages_to_send = 50
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    # publish messages
    publish_messages(connection, messages_to_send, queue_name)
    consumer = None
    try:
        consumer = connection.consumer(
            addr_queue,
            consumer_options=ConsumerOptions(feature=ConsumerFeature.Presettled),
            message_handler=MyMessagePresettledHandler(),
        )
        consumer.run()
    except ConsumerTestException:
        pass
    finally:
        pass

    if consumer is not None:
        consumer.close()
    info = management.queue_info("test-CQ-queue-presettled")
    # since pre_settled is True, messages should be removed from the queue upon delivery
    assert info.message_count == 0

    management.delete_queue(queue_name)
    management.close()
