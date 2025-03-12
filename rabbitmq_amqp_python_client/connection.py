import logging
import random
import time
from datetime import timedelta
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
from .entities import RecoveryConfiguration, StreamOptions
from .exceptions import (
    ArgumentOutOfRangeException,
    ValidationCodeException,
)
from .management import Management
from .publisher import Publisher
from .qpid.proton._exceptions import ConnectionException
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
        recovery_configuration: RecoveryConfiguration = RecoveryConfiguration(),
    ):
        """
         Initialize a new Connection instance.

         Args:
             uri: Single node connection URI
             uris: List of URIs for multi-node setup
             ssl_context: SSL configuration for secure connections
             reconnect: Ability to automatically reconnect in case of disconnections from the server

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
        self._conf_ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = ssl_context
        self._managements: list[Management] = []
        self._recovery_configuration: RecoveryConfiguration = recovery_configuration
        self._ssl_domain = None
        self._connections = []  # type: ignore
        self._index: int = -1
        self._publishers: list[Publisher] = []
        self._consumers: list[Consumer] = []

        # Some recovery_configuration validation
        if recovery_configuration.back_off_reconnect_interval < timedelta(seconds=1):
            raise ValidationCodeException(
                "back_off_reconnect_interval must be > 1 second"
            )

        if recovery_configuration.MaxReconnectAttempts < 1:
            raise ValidationCodeException("MaxReconnectAttempts must be at least 1")

    def _set_environment_connection_list(self, connections: []):  # type: ignore
        self._connections = connections

    def _open_connections(self, reconnect_handlers: bool = False) -> None:

        logger.debug("inside connection._open_connections creating connection")
        if self._recovery_configuration.active_recovery is False:
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

        if reconnect_handlers is True:
            logger.debug("reconnecting managements, publishers and consumers handlers")
            for i, management in enumerate(self._managements):
                # Update the broken connection and sender in the management
                self._managements[i]._update_connection(self._conn)

            for i, publisher in enumerate(self._publishers):
                # Update the broken connection and sender in the publisher
                self._publishers[i]._update_connection(self._conn)

            for i, consumer in enumerate(self._consumers):
                # Update the broken connection and sender in the consumer
                self._consumers[i]._update_connection(self._conn)

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

        self._open_connections()
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

    def management(self) -> Management:
        """
        Get the management interface for this connection.

        Returns:
            Management: The management interface for performing administrative tasks
        """
        if len(self._managements) == 0:
            management = Management(self._conn)
            management.open()
            self._managements.append(management)

        return self._managements[0]

    # closes the connection to the AMQP 1.0 server.
    def close(self) -> None:
        """
        Close the connection to the AMQP 1.0 server.

        Closes the underlying connection and removes it from the connection list.
        """
        logger.debug("Closing connection")
        try:
            for publisher in self._publishers[:]:
                publisher.close()
            for consumer in self._consumers[:]:
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
        publisher._set_publishers_list(self._publishers)
        self._publishers.append(publisher)
        return publisher

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

        logger.debug("_on_disconnection: disconnection detected")
        if self in self._connections:
            self._connections.remove(self)

        base_delay = self._recovery_configuration.back_off_reconnect_interval
        max_delay = timedelta(minutes=1)

        for attempt in range(self._recovery_configuration.MaxReconnectAttempts):

            logger.debug("attempting a reconnection")
            jitter = timedelta(milliseconds=random.randint(0, 500))
            delay = base_delay + jitter

            if delay > max_delay:
                delay = max_delay

            time.sleep(delay.total_seconds())

            try:

                self._open_connections(reconnect_handlers=True)

                self._connections.append(self)

            except ConnectionException as e:
                base_delay *= 2
                logger.error(
                    "Reconnection attempt failed",
                    "attempt",
                    attempt,
                    "error",
                    str(e),
                )
                # maximum attempts reached without establishing a connection
                if attempt == self._recovery_configuration.MaxReconnectAttempts - 1:
                    logger.error("Not able to reconnect")
                    raise ConnectionException
                else:
                    continue

            # connection established
            else:
                logger.debug("reconnected successful")
                return

    @property
    def active_producers(self) -> int:
        """Returns the number of active publishers"""
        return len(self._publishers)

    @property
    def active_consumers(self) -> int:
        """Returns the number of active consumers"""
        return len(self._consumers)
