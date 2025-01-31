import logging
from typing import Optional

from .address_helper import validate_address
from .consumer import Consumer
from .exceptions import ArgumentOutOfRangeException
from .management import Management
from .publisher import Publisher
from .qpid.proton._handlers import MessagingHandler
from .qpid.proton._transport import SSLDomain
from .qpid.proton.utils import BlockingConnection
from .ssl_configuration import SslConfigurationContext

logger = logging.getLogger(__name__)


class Connection:
    def __init__(
        self, addr: str, ssl_context: Optional[SslConfigurationContext] = None
    ):
        self._addr: str = addr
        self._conn: BlockingConnection
        self._management: Management
        self._conf_ssl_context: Optional[SslConfigurationContext] = ssl_context
        self._ssl_domain = None

    def dial(self) -> None:
        logger.debug("Establishing a connection to the amqp server")
        if self._conf_ssl_context is not None:
            logger.debug("Enabling SSL")

            self._ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
            if self._ssl_domain is not None:
                self._ssl_domain.set_trusted_ca_db(self._conf_ssl_context.ca_cert)
            # for mutual authentication
            if self._conf_ssl_context.client_cert is not None:
                logger.debug("Enabling mutual authentication as well")
                if self._ssl_domain is not None:
                    self._ssl_domain.set_credentials(
                        self._conf_ssl_context.client_cert.client_cert,
                        self._conf_ssl_context.client_cert.client_key,
                        self._conf_ssl_context.client_cert.password,
                    )
        self._conn = BlockingConnection(self._addr, ssl_domain=self._ssl_domain)
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
