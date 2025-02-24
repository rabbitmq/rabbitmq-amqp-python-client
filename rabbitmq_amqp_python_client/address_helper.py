from typing import Optional

from .entities import (
    ExchangeToExchangeBindingSpecification,
    ExchangeToQueueBindingSpecification,
)
from .qpid.proton._message import Message


def _is_unreserved(char: str) -> bool:
    # According to RFC 3986, unreserved characters are A-Z, a-z, 0-9, '-', '.', '_', and '~'
    return char.isalnum() or char in "-._~"


def encode_path_segment(input_string: Optional[str]) -> str:
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
