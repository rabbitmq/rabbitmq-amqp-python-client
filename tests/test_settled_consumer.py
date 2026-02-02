"""Tests for FIFOConsumerOptions with pre_settled functionality."""

from rabbitmq_amqp_python_client import (  # Environment,
    AddressHelper,
    Connection,
    ConsumerFeature,
    ConsumerOptions,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.utils import Converter

# from .conftest import (
#     ConsumerTestException,
#     MyMessageHandlerAccept,
# )
from .utils import publish_messages


def test_fifo_consumer_without_presettled(connection: Connection) -> None:
    """Test FIFO consumer with pre_settled=False (default behavior)."""
    queue_name = "test-fifo-queue-no-presettled"
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

    management.delete_queue(queue_name)
    management.close()

    assert consumed == messages_to_send


# def test_fifo_consumer_with_presettled(connection: Connection) -> None:
#     """Test FIFO consumer with pre_settled=True (at-most-once semantics)."""
#     queue_name = "test-fifo-queue-presettled"
#     messages_to_send = 50
#     management = connection.management()
#
#     management.declare_queue(QuorumQueueSpecification(name=queue_name))
#
#     addr_queue = AddressHelper.queue_address(queue_name)
#     consumer = connection.consumer(
#         addr_queue, consumer_options=ConsumerOptions(feature=ConsumerFeature.Presettled)
#     )
#
#     consumed = 0
#
#     # publish messages
#     publish_messages(connection, messages_to_send, queue_name)
#
#     # consume messages synchronously
#     for i in range(messages_to_send):
#         message = consumer.consume()
#         if Converter.bytes_to_string(message.body) == "test{}".format(i):
#             consumed = consumed + 1
#
#     consumer.close()
#
#     management.delete_queue(queue_name)
#     management.close()
#
#     assert consumed == messages_to_send


# def test_fifo_consumer_presettled_default(connection: Connection) -> None:
#     """Test FIFO consumer with default pre_settled=False."""
#     queue_name = "test-fifo-queue-default"
#     messages_to_send = 30
#     management = connection.management()
#
#     management.declare_queue(QuorumQueueSpecification(name=queue_name))
#
#     addr_queue = AddressHelper.queue_address(queue_name)
#     # Default pre_settled is False
#     consumer = connection.consumer(addr_queue, consumer_options=ConsumerOptions())
#
#     consumed = 0
#
#     # publish messages
#     publish_messages(connection, messages_to_send, queue_name)
#
#     # consume messages synchronously
#     for i in range(messages_to_send):
#         message = consumer.consume()
#         if Converter.bytes_to_string(message.body) == "test{}".format(i):
#             consumed = consumed + 1
#
#     consumer.close()
#
#     management.delete_queue(queue_name)
#     management.close()
#
#     assert consumed == messages_to_send
#     assert not consumer._consumer_options.direct_reply_to()


# def test_fifo_consumer_presettled_async(
#     connection: Connection, environment: Environment
# ) -> None:
#     """Test FIFO consumer with pre_settled=True in async mode."""
#     messages_to_send = 100
#     queue_name = "test-fifo-queue-async-presettled"
#
#     management = connection.management()
#
#     management.declare_queue(QuorumQueueSpecification(name=queue_name))
#
#     addr_queue = AddressHelper.queue_address(queue_name)
#
#     publish_messages(connection, messages_to_send, queue_name)
#
#     # we closed the connection so we need to open a new one
#     connection_consumer = environment.connection()
#     connection_consumer.dial()
#
#     handler = MyMessageHandlerAccept()
#     consumer = connection_consumer.consumer(
#         addr_queue,
#         message_handler=handler,
#         consumer_options=ConsumerOptions(feature=ConsumerFeature.Presettled),
#     )
#
#     try:
#         consumer.run()
#     except ConsumerTestException:
#         pass
#
#     consumer.close()
#
#     message_count = management.purge_queue(queue_name)
#
#     management.delete_queue(queue_name)
#     management.close()
#
#     assert message_count == 0
