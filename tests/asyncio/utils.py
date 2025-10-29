from rabbitmq_amqp_python_client import (
    AddressHelper,
    AsyncPublisher,
    Delivery,
    Message,
)
from rabbitmq_amqp_python_client.utils import Converter


async def async_publish_per_message(publisher: AsyncPublisher, addr: str) -> Delivery:
    message = Message(body=Converter.string_to_bytes("test"))
    message = AddressHelper.message_to_address_helper(message, addr)
    status = await publisher.publish(message)
    return status
