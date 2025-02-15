import logging
from typing import Optional

from .address_helper import validate_address
from .exceptions import (
    ArgumentOutOfRangeException,
    ValidationCodeException,
)
from .options import SenderOptionUnseattle
from .qpid.proton._delivery import Delivery
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingSender,
)

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(self, conn: BlockingConnection, addr: str = ""):
        self._sender: Optional[BlockingSender] = None
        self._conn = conn
        self._addr = addr
        self._open()

    def _open(self) -> None:
        if self._sender is None:
            logger.debug("Creating Sender")
            self._sender = self._create_sender(self._addr)

    def publish(self, message: Message) -> Delivery:
        if (self._addr != "") and (message.address is not None):
            raise ValidationCodeException(
                "address specified in both message and publisher"
            )

        if self._addr != "":
            if self._sender is not None:
                return self._sender.send(message)
        else:
            if message.address != "":
                if validate_address(message.address) is False:
                    raise ArgumentOutOfRangeException(
                        "destination address must start with /queues or /exchanges"
                    )
                if self._sender is not None:
                    delivery = self._sender.send(message)
                    return delivery

    def close(self) -> None:
        logger.debug("Closing Sender")
        if self._sender is not None:
            self._sender.close()

    def _create_sender(self, addr: str) -> BlockingSender:
        return self._conn.create_sender(addr, options=SenderOptionUnseattle(addr))
