import logging
from typing import Optional

from .consumer import Consumer
from .management import Management
from .publisher import Publisher
from .qpid.proton._handlers import MessagingHandler
from .qpid.proton.utils import BlockingConnection
from typing import Callable, Any, Annotated, TypeVar

logger = logging.getLogger(__name__)

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], None], "Message callback type"]

class Connection:
    def __init__(self, addr: str, on_disconnection_handler: Optional[CB] = None):
        self._addr: str = addr
        self._conn: BlockingConnection
        self._management: Management
        self._on_disconnection_handler = on_disconnection_handler

    def dial(self) -> None:
        logger.debug("Establishing a connection to the amqp server")
        self._conn = BlockingConnection(self._addr, on_disconnection_handler=self._on_disconnection_handler)
        self._open()
        logger.debug("Connection to the server established")

    def _open(self) -> None:
        self._management = Management(self._conn)
        self._management.open()

    def management(self) -> Management:
        return self._management

    # closes the connection to the AMQP 1.0 server.
    def close(self) -> None:
        logger.debug("Closing connection")
        print("closing connection")
        self._conn.close()
        print("after closing connection")

    def publisher(self, destination: str) -> Publisher:
        publisher = Publisher(self._conn, destination)
        return publisher

    def consumer(
        self, destination: str, handler: Optional[MessagingHandler] = None
    ) -> Consumer:
        consumer = Consumer(self._conn, destination, handler)
        return consumer
