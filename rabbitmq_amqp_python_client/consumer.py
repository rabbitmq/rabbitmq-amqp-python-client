import logging
from typing import Optional

from .options import ReceiverOptionUnsettled, ReceiverOptionUnsettledWithFilters
from .qpid.proton._handlers import MessagingHandler
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingReceiver,
)

from.entities import StreamFilterOptions

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(
        self,
        conn: BlockingConnection,
        addr: str,
        handler: Optional[MessagingHandler] = None,
        stream_options: Optional[StreamFilterOptions] = None
    ):
        self._receiver: Optional[BlockingReceiver] = None
        self._conn = conn
        self._addr = addr
        self._handler = handler
        self._stream_options = stream_options
        self._open()

    def _open(self) -> None:
        if self._receiver is None:
            logger.debug("Creating Sender")
            self._receiver = self._create_receiver(self._addr)

    def consume(self) -> Message:
        if self._receiver is not None:
            return self._receiver.receive()

    def close(self) -> None:
        logger.debug("Closing the receiver")
        if self._receiver is not None:
            self._receiver.close()

    def run(self) -> None:
        logger.debug("Running the consumer: starting to consume")
        if self._receiver is not None:
            self._receiver.container.run()

    def stop(self) -> None:
        logger.debug("Stopping the consumer: starting to consume")
        if self._receiver is not None:
            self._receiver.container.stop_events()
            self._receiver.container.stop()

    def _create_receiver(self, addr: str) -> BlockingReceiver:
        logger.debug("Creating the receiver")
        if self._stream_options is None:
            receiver = self._conn.create_receiver(
            addr, options=ReceiverOptionUnsettled(addr), handler=self._handler
            )
            receiver.credit = 1
        else:
            print("stream option is not None")
            receiver = self._conn.create_receiver(
                addr, options=ReceiverOptionUnsettledWithFilters(addr, self._stream_options), handler=self._handler
            )
            receiver.credit = 1

        return receiver

