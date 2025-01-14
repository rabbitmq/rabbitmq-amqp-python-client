import logging
from typing import Optional

from .options import ReceiverOption
from .qpid.proton._handlers import MessagingHandler
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingReceiver,
)

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(
        self,
        conn: BlockingConnection,
        addr: str,
        handler: Optional[MessagingHandler] = None,
    ):
        self._receiver: Optional[BlockingReceiver] = None
        self._conn = conn
        self._addr = addr
        self._handler = handler
        self._open()

    def _open(self) -> None:
        if self._receiver is None:
            logger.debug("Creating Sender")
            self._receiver = self._create_receiver(self._addr)

    def consume(self) -> Message:
        if self._receiver is not None:
            return self._receiver.receive()

    def close(self) -> None:
        logger.debug("Closing Sender and Receiver")
        if self._receiver is not None:
            self._receiver.close()
        if self._receiver is not None:
            self._receiver.close()

    def _create_receiver(self, addr: str) -> BlockingReceiver:
        return self._conn.create_receiver(
            addr, options=ReceiverOption(addr), handler=self._handler
        )
