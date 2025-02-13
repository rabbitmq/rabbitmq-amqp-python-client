import logging
from typing import Annotated, Callable, Optional, TypeVar

from .address_helper import validate_address
from .consumer import Consumer
from .entities import StreamOptions
from .exceptions import ArgumentOutOfRangeException
from .management import Management
from .publisher import Publisher
from .qpid.proton._handlers import MessagingHandler
from .qpid.proton._transport import SSLDomain
from .qpid.proton.utils import BlockingConnection
from .ssl_configuration import SslConfigurationContext

logger = logging.getLogger(__name__)

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], None], "Message callback type"]


class Connection:
    def __init__(
        self,
        # single-node mode
        uri: Optional[str] = None,
        # multi-node mode
        uris: Optional[list[str]] = None,
        ssl_context: Optional[SslConfigurationContext] = None,
        on_disconnection_handler: Optional[CB] = None,  # type: ignore
    ):
        if uri is None and uris is None:
            raise ValueError("You need to specify at least an addr or a list of addr")
        self._addr: Optional[str] = uri
        self._addrs: Optional[list[str]] = uris
        self._conn: BlockingConnection
        self._management: Management
        self._on_disconnection_handler = on_disconnection_handler
        self._conf_ssl_context: Optional[SslConfigurationContext] = ssl_context
        self._ssl_domain = None
        self._connections = []  # type: ignore
        self._index: int = -1

    def _set_environment_connection_list(self, connections: []):  # type: ignore
        self._connections = connections

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
        self._conn = BlockingConnection(
            url=self._addr,
            urls=self._addrs,
            ssl_domain=self._ssl_domain,
            on_disconnection_handler=self._on_disconnection_handler,
        )
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
        self._connections.remove(self)

    def publisher(self, destination: str) -> Publisher:
        if validate_address(destination) is False:
            raise ArgumentOutOfRangeException(
                "destination address must start with /queues or /exchanges"
            )
        publisher = Publisher(self._conn, destination)
        return publisher

    def consumer(
        self,
        destination: str,
        message_handler: Optional[MessagingHandler] = None,
        stream_filter_options: Optional[StreamOptions] = None,
        credit: Optional[int] = None,
    ) -> Consumer:
        if validate_address(destination) is False:
            raise ArgumentOutOfRangeException(
                "destination address must start with /queues or /exchanges"
            )
        consumer = Consumer(
            self._conn, destination, message_handler, stream_filter_options, credit
        )
        return consumer
