import logging
from typing import Optional

from .options import SenderOption
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingSender,
)

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(self, conn: BlockingConnection, addr: str):
        self._sender: Optional[BlockingSender] = None
        self._conn = conn
        self._addr = addr
        self._open()

    def _open(self) -> None:
        if self._sender is None:
            logger.debug("Creating Sender")
            self._sender = self._create_sender(self._addr)

    def publish(self, message: Message) -> None:
        if self._sender is not None:
            self._sender.send(message)

    def close(self) -> None:
        logger.debug("Closing Sender")
        if self._sender is not None:
            self._sender.close()

    def _create_sender(self, addr: str) -> BlockingSender:
        return self._conn.create_sender(addr, options=SenderOption(addr))
