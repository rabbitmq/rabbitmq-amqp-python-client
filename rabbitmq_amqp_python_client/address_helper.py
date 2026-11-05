import os
from typing import Optional
from urllib.parse import quote

from .entities import (
    ExchangeToExchangeBindingSpecification,
    ExchangeToQueueBindingSpecification,
)
from .qpid.proton._message import Message


# Feature flag: selects the encode_path_segment implementation.
# Default "legacy" (current behaviour): "quote" also fixes Unicode,
# so it must be enabled deliberately, not as a side effect.
ENCODER_ENV_VAR = "RABBITMQ_AMQP_CLIENT_ENCODER"
ENCODER_LEGACY = "legacy"
ENCODER_QUOTE = "quote"
_ENCODERS = (ENCODER_LEGACY, ENCODER_QUOTE)


def _resolve_encoder() -> str:
    """Read the flag from the environment, once at module import."""
    value = os.environ.get(ENCODER_ENV_VAR, ENCODER_LEGACY).strip().lower()
    if value not in _ENCODERS:
        raise ValueError(
            f"{ENCODER_ENV_VAR}={value!r} is not valid: "
            f"use {ENCODER_LEGACY!r} or {ENCODER_QUOTE!r}"
        )
    return value


_ENCODER = _resolve_encoder()


def _set_encoder(encoder: str) -> None:
    """Switch implementation at runtime. Hook for tests and A/B benchmarks."""
    global _ENCODER
    if encoder not in _ENCODERS:
        raise ValueError(f"encoder {encoder!r} is not valid: use {_ENCODERS}")
    _ENCODER = encoder


def _is_unreserved(char: str) -> bool:
    # According to RFC 3986, unreserved characters are A-Z, a-z, 0-9, '-', '.', '_', and '~'
    return char.isalnum() or char in "-._~"


def _encode_path_segment_legacy(input_string: Optional[str]) -> str:
    """Original implementation (O1): per-character loop.

    Note: ``str.isalnum()`` is also true for non-ASCII letters, which are left
    raw in the URI; for characters outside the BMP ``ord()`` produces more than
    two hex digits (``🚀`` → ``%1F680``), which is not a valid percent-encoding.
    """
    encoded = []

    # Iterate over each character in the input string
    if input_string is not None:
        for char in input_string:
            # Check if the character is an unreserved character
            if _is_unreserved(char):
                encoded.append(char)  # Append as is
            else:
                # Encode character to %HH format
                encoded.append(f"%{ord(char):02X}")

        return "".join(encoded)

    return ""


def _encode_path_segment_quote(input_string: Optional[str]) -> str:
    """O1: UTF-8 percent-encoding per byte via ``urllib.parse.quote``.

    ``safe="-._~"`` removes ``/`` from the safe set: the original code encodes
    it (``%2F``) and ASCII behaviour must stay identical.
    """
    if input_string is None:
        return ""
    return quote(input_string, safe="-._~")


def encode_path_segment(input_string: Optional[str]) -> str:
    if _ENCODER == ENCODER_QUOTE:
        return _encode_path_segment_quote(input_string)
    return _encode_path_segment_legacy(input_string)


class AddressHelper:
    """
    Helper class for constructing and managing AMQP addresses.

    This class provides static methods for creating properly formatted addresses
    for various AMQP operations including exchanges, queues, and bindings.
    """

    @staticmethod
    def exchange_address(exchange_name: str, routing_key: str = "") -> str:
        """
        Create an address for an exchange, optionally with a routing key.

        Args:
            exchange_name: The name of the exchange
            routing_key: Optional routing key

        Returns:
            str: The formatted exchange address
        """
        if routing_key == "":
            path = "/exchanges/" + encode_path_segment(exchange_name)
        else:
            path = (
                "/exchanges/"
                + encode_path_segment(exchange_name)
                + "/"
                + encode_path_segment(routing_key)
            )

        return path

    @staticmethod
    def queue_address(name: str) -> str:
        """
        Create an address for a queue.

        Args:
            name: The name of the queue

        Returns:
            str: The formatted queue address
        """
        path = "/queues/" + encode_path_segment(name)

        return path

    @staticmethod
    def purge_queue_address(name: str) -> str:
        """
        Create an address for purging a queue.

        Args:
            name: The name of the queue to purge

        Returns:
            str: The formatted purge queue address
        """
        path = "/queues/" + encode_path_segment(name) + "/messages"

        return path

    @staticmethod
    def path_address() -> str:
        path = "/bindings"

        return path

    @staticmethod
    def binding_path_with_exchange_queue(
        bind_specification: ExchangeToQueueBindingSpecification,
    ) -> str:
        """
        Create a binding path for an exchange-to-queue binding.

        Args:
            bind_specification: The specification for the binding

        Returns:
            str: The formatted binding path
        """
        if bind_specification.binding_key is not None:
            key = ";key=" + encode_path_segment(bind_specification.binding_key)
        else:
            key = ";key="

        binding_path_wth_exchange_queue_key = (
            "/bindings"
            + "/"
            + "src="
            + encode_path_segment(bind_specification.source_exchange)
            + ";"
            + "dstq="
            + encode_path_segment(bind_specification.destination_queue)
            + key
            + ";args="
        )
        return binding_path_wth_exchange_queue_key

    @staticmethod
    def binding_path_with_exchange_exchange(
        bind_specification: ExchangeToExchangeBindingSpecification,
    ) -> str:
        """
        Create a binding path for an exchange-to-exchange binding.

        Args:
            bind_specification: The specification for the binding

        Returns:
            str: The formatted binding path
        """
        binding_path_wth_exchange_exchange_key = (
            "/bindings"
            + "/"
            + "src="
            + encode_path_segment(bind_specification.source_exchange)
            + ";"
            + "dstq="
            + encode_path_segment(bind_specification.destination_exchange)
            + ";key="
            + encode_path_segment(bind_specification.binding_key)
            + ";args="
        )
        return binding_path_wth_exchange_exchange_key

    @staticmethod
    def message_to_address_helper(message: Message, address: str) -> Message:
        """
        Set the address on a message.

        Args:
            message: The message to modify
            address: The address to set

        Returns:
            Message: The modified message with the new address
        """
        message.address = address
        return message


def validate_address(address: str) -> bool:
    if address.startswith("/queues") or address.startswith("/exchanges"):
        return True
    return False
