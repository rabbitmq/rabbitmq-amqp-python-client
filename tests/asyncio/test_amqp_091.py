import functools

import pika
import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AsyncConnection,
    Converter,
    OutcomeState,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.qpid.proton import Message

from .fixtures import *  # noqa: F401, F403


@pytest.mark.asyncio
async def test_publish_queue(async_connection: AsyncConnection) -> None:
    queue_name = "amqp091-queue"
    management = await async_connection.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    publisher = None
    accepted = False

    try:
        publisher = await async_connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
        status = await publisher.publish(
            Message(body=Converter.string_to_bytes("my_test_string_for_amqp"))
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
    except Exception:
        raised = True

    if publisher is not None:
        await publisher.close()

    assert accepted is True
    assert raised is False

    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters("localhost", credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    def on_message(chan, method_frame, header_frame, body, userdata=None):
        """Called when a message is received. Log message and ack it."""
        chan.basic_ack(delivery_tag=method_frame.delivery_tag)
        assert body is not None
        body_text = Converter.bytes_to_string(body)
        assert body_text is not None
        assert body_text == "my_test_string_for_amqp"
        channel.stop_consuming()

    on_message_callback = functools.partial(on_message, userdata="on_message_userdata")
    channel.basic_qos(
        prefetch_count=1,
    )
    channel.basic_consume(queue_name, on_message_callback)

    channel.start_consuming()
    connection.close()

    await management.delete_queue(queue_name)
    await management.close()
