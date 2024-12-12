import logging

from proton.utils import BlockingConnection

from .management import Management
from .publisher import Publisher

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
        self._conn.close()

    def publisher(self, destination: str) -> Publisher:
        publisher = Publisher(self._conn, destination)
        return publisher

    # TODO: returns the current status of the connection.
    # def status(self) -> int:
    #    pass
