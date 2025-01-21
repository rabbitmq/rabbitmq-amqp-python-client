import logging
from typing import Optional

from .address_helper import validate_address
from .consumer import Consumer
from .exceptions import ArgumentOutOfRangeException
from .management import Management
from .publisher import Publisher
from .qpid.proton._handlers import MessagingHandler
from .qpid.proton.utils import BlockingConnection

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, addr: str):
        self._addr: str = addr
        self._conn: BlockingConnection
        self._management: Management

    def dial(self) -> None:
        logger.debug("Establishing a connection to the amqp server")
        self._conn = BlockingConnection(self._addr)
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
        self._conn.close()

    def publisher(self, destination: str) -> Publisher:
        if validate_address(destination) is False:
            raise ArgumentOutOfRangeException(
                "destination address must start with /queues or /exchanges"
            )
        publisher = Publisher(self._conn, destination)
        return publisher

    def consumer(
        self, destination: str, handler: Optional[MessagingHandler] = None
    ) -> Consumer:
        if validate_address(destination) is False:
            raise ArgumentOutOfRangeException(
                "destination address must start with /queues or /exchanges"
            )
        consumer = Consumer(self._conn, destination, handler)
        return consumer
