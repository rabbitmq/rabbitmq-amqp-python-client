import logging
from typing import Optional

from .address_helper import validate_address
from .exceptions import (
    ArgumentOutOfRangeException,
    ValidationCodeException,
)
from .options import SenderOptionUnseattle
from .qpid.proton._delivery import Delivery
from .qpid.proton._endpoints import Endpoint
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingSender,
)

logger = logging.getLogger(__name__)


class Publisher:
    """
    A publisher class for sending messages to RabbitMQ via AMQP 1.0 protocol.

    This class handles the publishing of messages to either a predefined address
    or to addresses specified in individual messages. It manages a blocking
    connection to RabbitMQ and ensures proper message delivery.

    Attributes:
        _sender (Optional[BlockingSender]): The sender for publishing messages
        _conn (BlockingConnection): The underlying connection to RabbitMQ
        _addr (str): The default address to publish to, if specified
    """

    def __init__(self, conn: BlockingConnection, addr: str = ""):
        """
        Initialize a new Publisher instance.

        Args:
            conn: The blocking connection to use for publishing
            addr: Optional default address to publish to. If provided, all messages
                 will be sent to this address unless overridden.
        """
        self._sender: Optional[BlockingSender] = None
        self._conn = conn
        self._addr = addr
        self._publishers: list[Publisher] = []
        self._open()

    def _update_connection(self, conn: BlockingConnection) -> None:
        self._conn = conn
        self._sender = self._create_sender(self._addr)

    def _open(self) -> None:
        if self._sender is None:
            logger.debug("Creating Sender")
            self._sender = self._create_sender(self._addr)

    def _set_publishers_list(self, publishers: []) -> None:  # type: ignore
        self._publishers = publishers

    def publish(self, message: Message) -> Delivery:
        """
        Publish a message to RabbitMQ.

        The message can be sent to either the publisher's default address or
        to an address specified in the message itself, but not both.

        Args:
            message: The message to publish

        Returns:
            Delivery: The delivery confirmation from RabbitMQ

        Raises:
            ValidationCodeException: If address is specified in both message and publisher
            ArgumentOutOfRangeException: If message address format is invalid
        """
        if (self._addr != "") and (message.address is not None):
            raise ValidationCodeException(
                "address specified in both message and publisher"
            )

        if not isinstance(message.body, (bytes, type(None))):
            raise ArgumentOutOfRangeException(
                "Message body must be of type bytes or None"
            )

        if not message.inferred:
            raise ArgumentOutOfRangeException("Message inferred must be True")

        if self._addr != "":
            if self._sender is not None:
                return self._sender.send(message)
        else:
            if message.address != "":
                if not validate_address(message.address):
                    raise ArgumentOutOfRangeException(
                        "destination address must start with /queues or /exchanges"
                    )
                if self.is_open:
                    delivery = self._sender.send(message)  # type: ignore
                    return delivery

    def close(self) -> None:
        """
        Close the publisher connection.

        Closes the sender if it exists and cleans up resources.
        """
        logger.debug("Closing Sender")
        if self.is_open:
            self._sender.close()  # type: ignore
            self._sender = None
            if self in self._publishers:
                self._publishers.remove(self)

    def _create_sender(self, addr: str) -> BlockingSender:
        return self._conn.create_sender(addr, options=SenderOptionUnseattle(addr))

    def _is_sender_closed(self) -> bool:
        if self._sender is None:
            return True
        return bool(
            self._sender.link.state & (Endpoint.LOCAL_CLOSED | Endpoint.REMOTE_CLOSED)
        )

    @property
    def is_open(self) -> bool:
        """Check if publisher is open and ready to send messages."""
        if self._sender is not None:
            return not self._is_sender_closed()
        return False

    @property
    def address(self) -> str:
        """Get the current publisher address."""
        return self._addr
