from urllib.parse import unquote

import pytest

from rabbitmq_amqp_python_client import AddressHelper
from rabbitmq_amqp_python_client.address_helper import encode_path_segment


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
