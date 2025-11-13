from typing import Optional

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AsyncConnection,
    AsyncManagement,
    AsyncPublisher,
    Delivery,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    ExchangeType,
    Message,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.utils import Converter


async def async_publish_per_message(publisher: AsyncPublisher, addr: str) -> Delivery:
    message = Message(body=Converter.string_to_bytes("test"))
    message = AddressHelper.message_to_address_helper(message, addr)
    status = await publisher.publish(message)
    return status


async def async_publish_messages(
    connection: AsyncConnection,
    messages_to_send: int,
    queue_name: str,
    filters: Optional[list[str]] = None,
) -> None:
    annotations = {}
    if filters is not None:
        for filterItem in filters:
            annotations = {"x-stream-filter-value": filterItem}

    publisher = await connection.publisher("/queues/" + queue_name)
    # publish messages_to_send messages
    for i in range(messages_to_send):
        await publisher.publish(
            Message(
                body=Converter.string_to_bytes("test{}".format(i)),
                annotations=annotations,
            )
        )
    await publisher.close()


async def async_setup_dead_lettering(management: AsyncManagement) -> str:
    exchange_dead_lettering = "exchange-dead-letter"
    queue_dead_lettering = "queue-dead-letter"
    binding_key = "key_dead_letter"

    # configuring dead lettering
    await management.declare_exchange(
        ExchangeSpecification(
            name=exchange_dead_lettering,
            exchange_type=ExchangeType.fanout,
            arguments={},
        )
    )
    await management.declare_queue(QuorumQueueSpecification(name=queue_dead_lettering))
    bind_path = await management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_dead_lettering,
            destination_queue=queue_dead_lettering,
            binding_key=binding_key,
        )
    )

    return bind_path


async def async_cleanup_dead_lettering(
    management: AsyncManagement, bind_path: str
) -> None:
    exchange_dead_lettering = "exchange-dead-letter"
    queue_dead_lettering = "queue-dead-letter"

    await management.unbind(bind_path)
    await management.delete_exchange(exchange_dead_lettering)
    await management.delete_queue(queue_dead_lettering)
