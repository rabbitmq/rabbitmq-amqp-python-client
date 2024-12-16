import logging
from typing import Optional

from proton import Message
from proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)

from .options import SenderOption

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(self, conn: BlockingConnection, addr: str):
        self._sender: Optional[BlockingSender] = None
        self._receiver: Optional[BlockingReceiver] = None
        self._conn = conn
        self._addr = addr
        self._open()

    def _open(self) -> None:
        print("addr is " + str(self._addr))
        if self._sender is None:
            logger.debug("Creating Sender")
            self._sender = self._create_sender(self._addr)

    def publish(self, message: Message) -> None:
        if self._sender is not None:
            self._sender.send(message)

    def close(self) -> None:
        if self._sender is not None:
            self._sender.close()
        # if self._receiver is not None:
        #    self._receiver.close()

    def _create_sender(self, addr: str) -> BlockingSender:
        return self._conn.create_sender(addr, options=SenderOption(addr))
