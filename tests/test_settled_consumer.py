"""Tests for (classic and Quorum) with pre_settled functionality."""

from rabbitmq_amqp_python_client import (  # Environment,
    AddressHelper,
    Connection,
    ConsumerFeature,
    ConsumerOptions,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.utils import Converter

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


def test_fifo_consumer_with_presettled(connection: Connection) -> None:
    """Test FIFO consumer with pre_settled=True (at-most-once semantics)."""
    queue_name = "test-CQ-queue-presettled"
    messages_to_send = 50
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)
    consumer = connection.consumer(
        addr_queue, consumer_options=ConsumerOptions(feature=ConsumerFeature.Presettled)
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
    info = management.queue_info("test-CQ-queue-presettled")
    # since pre_settled is True, messages should be removed from the queue upon delivery
    assert info.message_count == 0

    management.delete_queue(queue_name)
    management.close()

    assert consumed == messages_to_send
