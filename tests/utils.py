import base64
from datetime import datetime
from typing import Optional

import jwt

from rabbitmq_amqp_python_client import (
    AddressHelper,
    Connection,
    Delivery,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    ExchangeType,
    Management,
    Message,
    Publisher,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.utils import string_to_bytes


def publish_messages(
    connection: Connection,
    messages_to_send: int,
    queue_name,
    filters: Optional[list[str]] = None,
) -> None:
    annotations = {}
    if filters is not None:
        for filterItem in filters:
            annotations = {"x-stream-filter-value": filterItem}

    publisher = connection.publisher("/queues/" + queue_name)
    # publish messages_to_send messages
    for i in range(messages_to_send):
        publisher.publish(
            Message(body=string_to_bytes("test{}".format(i)), annotations=annotations)
        )
    publisher.close()


def publish_per_message(publisher: Publisher, addr: str) -> Delivery:
    message = Message(body=string_to_bytes("test"))
    message = AddressHelper.message_to_address_helper(message, addr)
    status = publisher.publish(message)
    return status


def setup_dead_lettering(management: Management) -> str:
    exchange_dead_lettering = "exchange-dead-letter"
    queue_dead_lettering = "queue-dead-letter"
    binding_key = "key_dead_letter"

    # configuring dead lettering
    management.declare_exchange(
        ExchangeSpecification(
            name=exchange_dead_lettering,
            exchange_type=ExchangeType.fanout,
            arguments={},
        )
    )
    management.declare_queue(QuorumQueueSpecification(name=queue_dead_lettering))
    bind_path = management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_dead_lettering,
            destination_queue=queue_dead_lettering,
            binding_key=binding_key,
        )
    )

    return bind_path


def create_binding(
    management: Management, exchange_name: str, queue_name: str, routing_key: str
) -> str:
    management.declare_exchange(ExchangeSpecification(name=exchange_name))

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    bind_name = management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    return bind_name


def cleanup_dead_lettering(management: Management, bind_path: str) -> None:
    exchange_dead_lettering = "exchange-dead-letter"
    queue_dead_lettering = "queue-dead-letter"

    management.unbind(bind_path)
    management.delete_exchange(exchange_dead_lettering)
    management.delete_queue(queue_dead_lettering)


def token(duration: datetime) -> str:
    # Decode the base64 key
    decoded_key = base64.b64decode("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH")

    # Define the claims
    claims = {
        "iss": "unit_test",
        "aud": "rabbitmq",
        "exp": int(duration.timestamp()),
        "scope": ["rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"],
        "random": random_string(6),
    }

    # Create the token with the claims and sign it
    jwt_token = jwt.encode(
        claims, decoded_key, algorithm="HS256", headers={"kid": "token-key"}
    )

    return jwt_token


# Helper function to generate a random string (replace with your implementation)
def random_string(length: int) -> str:
    import random
    import string

    return "".join(random.choices(string.ascii_letters + string.digits, k=length))
