import logging
from typing import (
    Annotated,
    Callable,
    Optional,
    TypeVar,
    Union,
)

import typing_extensions

from .address_helper import validate_address
from .consumer import Consumer
from .entities import StreamOptions
from .exceptions import ArgumentOutOfRangeException
from .management import Management
from .publisher import Publisher
from .qpid.proton._handlers import MessagingHandler
from .qpid.proton._transport import SSLDomain
from .qpid.proton.utils import BlockingConnection
from .ssl_configuration import (
    CurrentUserStore,
    FriendlyName,
    LocalMachineStore,
    PKCS12Store,
    PosixSslConfigurationContext,
    Unambiguous,
    WinSslConfigurationContext,
)

logger = logging.getLogger(__name__)

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], None], "Message callback type"]


class Connection:
    """
    Main connection class for interacting with RabbitMQ via AMQP 1.0 protocol.

    This class manages the connection to RabbitMQ and provides factory methods for
    creating publishers, consumers, and management interfaces. It supports both
    single-node and multi-node configurations, as well as SSL/TLS connections.

    """

    def __init__(
        self,
        # single-node mode
        uri: Optional[str] = None,
        # multi-node mode
        uris: Optional[list[str]] = None,
        ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = None,
        reconnect: bool = False,
    ):
        """
         Initialize a new Connection instance.

         Args:
             uri: Single node connection URI
             uris: List of URIs for multi-node setup
             ssl_context: SSL configuration for secure connections
             on_disconnection_handler: Callback for handling disconnection events

        Raises:
             ValueError: If neither uri nor uris is provided
        """
        if uri is not None and uris is not None:
            raise ValueError(
                "Cannot specify both 'uri' and 'uris'. Choose one connection mode."
            )
        if uri is None and uris is None:
            raise ValueError("Must specify either 'uri' or 'uris' for connection.")
        self._addr: Optional[str] = uri
        self._addrs: Optional[list[str]] = uris
        self._conn: BlockingConnection
        self._management: Management
        self._conf_ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = ssl_context
        self._reconnect = reconnect
        self._ssl_domain = None
        self._connections = []  # type: ignore
        self._index: int = -1
        self._publishers: list[Publisher] = []
        self._consumers: list[Consumer] = []

    def _set_environment_connection_list(self, connections: []):  # type: ignore
        self._connections = connections

    def dial(self) -> None:
        """
        Establish a connection to the AMQP server.

        Configures SSL if specified and establishes the connection using the
        provided URI(s). Also initializes the management interface.
        """
        logger.debug("Establishing a connection to the amqp server")
        if self._conf_ssl_context is not None:
            logger.debug("Enabling SSL")

            self._ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
            assert self._ssl_domain

            if isinstance(self._conf_ssl_context, PosixSslConfigurationContext):
                ca_cert = self._conf_ssl_context.ca_cert
            elif isinstance(self._conf_ssl_context, WinSslConfigurationContext):
                ca_cert = self._win_store_to_cert(self._conf_ssl_context.ca_store)
            else:
                typing_extensions.assert_never(self._conf_ssl_context)
            self._ssl_domain.set_trusted_ca_db(ca_cert)

            # for mutual authentication
            if self._conf_ssl_context.client_cert is not None:
                logger.debug("Enabling mutual authentication as well")

                if isinstance(self._conf_ssl_context, PosixSslConfigurationContext):
                    client_cert = self._conf_ssl_context.client_cert.client_cert
                    client_key = self._conf_ssl_context.client_cert.client_key
                    password = self._conf_ssl_context.client_cert.password
                elif isinstance(self._conf_ssl_context, WinSslConfigurationContext):
                    client_cert = self._win_store_to_cert(
                        self._conf_ssl_context.client_cert.store
                    )
                    disambiguation_method = (
                        self._conf_ssl_context.client_cert.disambiguation_method
                    )
                    if isinstance(disambiguation_method, Unambiguous):
                        client_key = None
                    elif isinstance(disambiguation_method, FriendlyName):
                        client_key = disambiguation_method.name
                    else:
                        typing_extensions.assert_never(disambiguation_method)

                    password = self._conf_ssl_context.client_cert.password
                else:
                    typing_extensions.assert_never(self._conf_ssl_context)

                self._ssl_domain.set_credentials(
                    client_cert,
                    client_key,
                    password,
                )

        if self._reconnect is False:
            self._conn = BlockingConnection(
                url=self._addr,
                urls=self._addrs,
                ssl_domain=self._ssl_domain,
            )
        else:
            self._conn = BlockingConnection(
                url=self._addr,
                urls=self._addrs,
                ssl_domain=self._ssl_domain,
                on_disconnection_handler=self._on_disconnection,
            )

        self._open()
        logger.debug("Connection to the server established")

    def _win_store_to_cert(
        self, store: Union[LocalMachineStore, CurrentUserStore, PKCS12Store]
    ) -> str:
        if isinstance(store, LocalMachineStore):
            ca_cert = f"lmss:{store.name}"
        elif isinstance(store, CurrentUserStore):
            ca_cert = f"ss:{store.name}"
        elif isinstance(store, PKCS12Store):
            ca_cert = store.path
        else:
            typing_extensions.assert_never(store)
        return ca_cert

    def _open(self) -> None:
        self._management = Management(self._conn)
        self._management.open()

    def management(self) -> Management:
        """
        Get the management interface for this connection.

        Returns:
            Management: The management interface for performing administrative tasks
        """
        return self._management

    # closes the connection to the AMQP 1.0 server.
    def close(self) -> None:
        """
        Close the connection to the AMQP 1.0 server.

        Closes the underlying connection and removes it from the connection list.
        """
        logger.debug("Closing connection")
        try:
            for publisher in self._publishers:
                publisher.close()
            for consumer in self._consumers:
                consumer.close()
            self._conn.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise e

        finally:
            if self in self._connections:
                self._connections.remove(self)

    def publisher(self, destination: str = "") -> Publisher:
        """
        Create a new publisher instance.

        Args:
            destination: Optional default destination for published messages

        Returns:
            Publisher: A new publisher instance

        Raises:
            ArgumentOutOfRangeException: If destination address format is invalid
        """
        if destination != "":
            if validate_address(destination) is False:
                raise ArgumentOutOfRangeException(
                    "destination address must start with /queues or /exchanges"
                )
        publisher = Publisher(self._conn, destination)
        self._publishers.append(publisher)
        return self._publishers[self._publishers.index(publisher)]

    def consumer(
        self,
        destination: str,
        message_handler: Optional[MessagingHandler] = None,
        stream_filter_options: Optional[StreamOptions] = None,
        credit: Optional[int] = None,
    ) -> Consumer:
        """
        Create a new consumer instance.

        Args:
            destination: The address to consume from
            message_handler: Optional handler for processing messages
            stream_filter_options: Optional configuration for stream consumption
            credit: Optional credit value for flow control

        Returns:
            Consumer: A new consumer instance

        Raises:
            ArgumentOutOfRangeException: If destination address format is invalid
        """
        if validate_address(destination) is False:
            raise ArgumentOutOfRangeException(
                "destination address must start with /queues or /exchanges"
            )
        consumer = Consumer(
            self._conn, destination, message_handler, stream_filter_options, credit
        )
        self._consumers.append(consumer)
        return consumer

    def _on_disconnection(self) -> None:

        print("disconnected")

        if self in self._connections:
            self._connections.remove(self)

        print("reconnecting")
        self._conn = BlockingConnection(
            url=self._addr,
            urls=self._addrs,
            ssl_domain=self._ssl_domain,
            on_disconnection_handler=self._on_disconnection,
        )
        self._open()
        self._connections.append(self)

        for index, publisher in enumerate(self._publishers):
            # publisher = self._publishers.pop(index)
            # address = publisher.address
            self._publishers.remove(publisher)
            # self._publishers.insert(index, Publisher(self._conn, address))

        for i, consumer in enumerate(self._consumers):
            self._consumers.remove(consumer)
