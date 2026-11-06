from urllib.parse import unquote

import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    Environment,
    Event,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.address_helper import (
    encode_path_segment,
)
from rabbitmq_amqp_python_client.utils import Converter

from .conftest import ConsumerTestException


def test_encoding_queue_simple() -> None:
    queue = "my_queue"

    address = AddressHelper.queue_address(queue)

    assert address == "/queues/my_queue"


def test_encoding_queue_hex() -> None:
    queue = "my_queue>"

    address = AddressHelper.queue_address(queue)

    assert address == "/queues/my_queue%3E"


def test_encoding_exchange_hex() -> None:
    queue = "my_exchange/()"

    address = AddressHelper.exchange_address(queue)

    assert address == "/exchanges/my_exchange%2F%28%29"


# --- O1: UTF-8 percent-encoding -------------------------------------------

ASCII_CORPUS = [
    "ordini-2026",
    "my_queue",
    "my_queue>",
    "my_exchange/()",
    "code con spazi",
    "a/b?c#d;e=f&g",
    "q" * 64,
    "".join(chr(code) for code in range(32, 127)),
]


@pytest.mark.parametrize("value", ASCII_CORPUS)
def test_ascii_encoding_round_trips(value: str) -> None:
    """ASCII names stay a valid, single-level path segment."""
    encoded = encode_path_segment(value)

    assert unquote(encoded) == value
    assert encoded.isascii()


def test_unreserved_characters_are_not_encoded() -> None:
    assert encode_path_segment("AZaz09-._~") == "AZaz09-._~"


@pytest.mark.parametrize("value", ["città/italia", "emoji-🚀-queue", "日本語"])
def test_unicode_is_utf8_percent_encoded(value: str) -> None:
    encoded = encode_path_segment(value)

    assert unquote(encoded) == value
    assert encoded.isascii(), "the URI must contain only ASCII characters"


def test_unicode_examples() -> None:
    assert encode_path_segment("città/italia") == "citt%C3%A0%2Fitalia"
    assert encode_path_segment("emoji-🚀-queue") == "emoji-%F0%9F%9A%80-queue"


def test_public_address_uses_utf8_encoding() -> None:
    assert AddressHelper.queue_address("città") == "/queues/citt%C3%A0"


def test_none_and_empty() -> None:
    assert encode_path_segment(None) == ""
    assert encode_path_segment("") == ""


class _CaptureBodyHandler(AMQPMessagingHandler):
    """Accepts the first message, stores its body and stops the consumer."""

    def __init__(self) -> None:
        super().__init__()
        self.body = ""

    def on_message(self, event: Event) -> None:
        self.body = Converter.bytes_to_string(event.message.body)
        self.delivery_context.accept(event)
        raise ConsumerTestException("consumed")


def test_non_ascii_queue_name_end_to_end(
    connection: Connection, environment: Environment
) -> None:
    """The broker accepts the UTF-8 percent-encoded address for declare,
    publish and consume."""
    queue_name = "città-🚀"
    body = "ciao 🚀"
    management = connection.management()
    consumer = None
    publisher = None
    connection_consumer = None
    handler = _CaptureBodyHandler()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))
    try:
        addr_queue = AddressHelper.queue_address(queue_name)
        assert addr_queue == "/queues/citt%C3%A0-%F0%9F%9A%80"

        publisher = connection.publisher(destination=addr_queue)
        status = publisher.publish(Message(body=Converter.string_to_bytes(body)))
        assert status.remote_state == OutcomeState.ACCEPTED

        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(addr_queue, message_handler=handler)
        try:
            consumer.run()
        except ConsumerTestException:
            pass

        assert handler.body == body
    finally:
        if consumer is not None:
            consumer.close()
        if publisher is not None:
            publisher.close()
        management.delete_queue(queue_name)
        management.close()
