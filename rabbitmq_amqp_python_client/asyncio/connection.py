from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional, Union

from ..address_helper import validate_address
from ..connection import Connection
from ..consumer import Consumer
from ..entities import (
    ConsumerOptions,
    OAuth2Options,
    RecoveryConfiguration,
)
from ..exceptions import ArgumentOutOfRangeException
from ..management import Management
from ..publisher import Publisher
from ..qpid.proton._handlers import MessagingHandler
from ..ssl_configuration import (
    PosixSslConfigurationContext,
    WinSslConfigurationContext,
)
from .consumer import AsyncConsumer
from .management import AsyncManagement
from .publisher import AsyncPublisher

logger = logging.getLogger(__name__)


class AsyncConnection:
    """
    Asyncio-compatible facade around Connection.

    This class manages the connection to RabbitMQ and provides factory methods for
    creating publishers, consumers, and management interfaces. It supports both
    single-node and multi-node configurations, as well as SSL/TLS connections.

    Note:
        The underlying Proton BlockingConnection is NOT thread-safe. A lock is used
        to serialize all operations.

    Attributes:
        _connection (Connection): The underlying synchronous Connection
        _connection_lock (asyncio.Lock): Lock for coordinating access to the shared connection
        _async_publishers (list[AsyncPublisher]): List of active async publishers
        _async_consumers (list[AsyncConsumer]): List of active async consumers
        _async_managements (list[AsyncManagement]): List of active async management interfaces
        _remove_callback (Optional[Callable[[AsyncConnection], None]]): Callback on close
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        uris: Optional[list[str]] = None,
        ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = None,
        oauth2_options: Optional[OAuth2Options] = None,
        recovery_configuration: RecoveryConfiguration = RecoveryConfiguration(),
    ):
        """
        Initialize AsyncConnection.

        Args:
            uri: Optional single-node connection URI
            uris: Optional multi-node connection URIs
            ssl_context: Optional SSL/TLS configuration
            oauth2_options: Optional OAuth2 configuration
            recovery_configuration: Configuration for automatic recovery

        Raises:
            ValidationCodeException: If recovery configuration is invalid
        """
        self._connection = Connection(
            uri=uri,
            uris=uris,
            ssl_context=ssl_context,
            oauth2_options=oauth2_options,
            recovery_configuration=recovery_configuration,
        )
        self._connection_lock = asyncio.Lock()
        self._async_publishers: list[AsyncPublisher] = []
        self._async_consumers: list[AsyncConsumer] = []
        self._async_managements: list[AsyncManagement] = []
        self._remove_callback: Optional[Callable[[AsyncConnection], None]] = None

    async def dial(self) -> None:
        """
        Establish a connection to the AMQP server.

        Configures SSL if specified and establishes the connection using the
        provided URI(s). Also initializes the management interface.
        """
        async with self._connection_lock:
            await asyncio.to_thread(self._connection.dial)

    def _set_remove_callback(
        self, callback: Optional[Callable[[AsyncConnection], None]]
    ) -> None:
        """Set callback to be called when connection is closed."""
        self._remove_callback = callback

    def _remove_publisher(self, publisher: AsyncPublisher) -> None:
        """Remove a publisher from the active list."""
        if publisher in self._async_publishers:
            self._async_publishers.remove(publisher)

    def _remove_consumer(self, consumer: AsyncConsumer) -> None:
        """Remove a consumer from the active list."""
        if consumer in self._async_consumers:
            self._async_consumers.remove(consumer)

    def _remove_management(self, management: AsyncManagement) -> None:
        """Remove a management interface from the active list."""
        if management in self._async_managements:
            self._async_managements.remove(management)

    async def close(self) -> None:
        """
        Close the connection to the AMQP 1.0 server.

        Closes the underlying connection and removes it from the connection list.
        """
        logger.debug("Closing async connection")
        try:
            for async_publisher in self._async_publishers[:]:
                await async_publisher.close()
            for async_consumer in self._async_consumers[:]:
                await async_consumer.close()
            for async_management in self._async_managements[:]:
                await async_management.close()

            async with self._connection_lock:
                await asyncio.to_thread(self._connection.close)
        except Exception as e:
            logger.error(f"Error closing async connections: {e}")
            raise e
        finally:
            if self._remove_callback is not None:
                self._remove_callback(self)

    def _set_connection_managements(self, management: Management) -> None:
        if len(self._connection._managements) == 0:
            self._connection._managements = [management]

    async def management(
        self,
    ) -> AsyncManagement:
        """
        Get the management interface for this connection.

        Returns:
            AsyncManagement: The management interface for performing administrative tasks
        """
        if len(self._async_managements) > 0:
            return self._async_managements[0]

        async_management: Optional[AsyncManagement] = None
        async with self._connection_lock:
            if len(self._async_managements) == 0:
                async_management = AsyncManagement(
                    self._connection._conn,
                    connection_lock=self._connection_lock,
                )

                self._set_connection_managements(
                    async_management._management
                )  # TODO: check this
                self._async_managements.append(async_management)

                async_management._set_remove_callback(self._remove_management)

        if async_management is not None:
            await async_management.open()

        return self._async_managements[0]

    def _set_connection_publishers(self, publisher: Publisher) -> None:
        """Set the list of publishers in the underlying connection."""
        publisher._set_publishers_list(
            [async_publisher._publisher for async_publisher in self._async_publishers]
        )
        self._connection._publishers.append(publisher)

    async def publisher(self, destination: str = "") -> AsyncPublisher:
        """
        Create a new publisher instance.

        Args:
            destination: Optional default destination for published messages

        Returns:
            AsyncPublisher: A new publisher instance

        Raises:
            RuntimeError: If publisher creation fails
            ArgumentOutOfRangeException: If destination address format is invalid
        """
        if destination != "":
            if not validate_address(destination):
                raise ArgumentOutOfRangeException(
                    "destination address must start with /queues or /exchanges"
                )

        async with self._connection_lock:
            async_publisher = AsyncPublisher(
                self._connection._conn,
                destination,
                connection_lock=self._connection_lock,
            )
            await async_publisher.open()
            if async_publisher._publisher is None:
                raise RuntimeError("Failed to create publisher")
            self._set_connection_publishers(
                async_publisher._publisher
            )  # TODO: check this
            self._async_publishers.append(async_publisher)

            async_publisher._set_remove_callback(self._remove_publisher)

            return async_publisher

    def _set_connection_consumers(self, consumer: Consumer) -> None:
        """Set the list of consumers in the underlying connection."""
        self._connection._consumers.append(consumer)

    async def consumer(
        self,
        destination: str,
        message_handler: Optional[MessagingHandler] = None,
        consumer_options: Optional[ConsumerOptions] = None,
        credit: Optional[int] = None,
    ) -> AsyncConsumer:
        """
        Create a new consumer instance.

        Args:
            destination: The address to consume from
            message_handler: Optional handler for processing messages
            consumer_options: Optional configuration for queue consumption. Each queue has its own consumer options.
            credit: Optional credit value for flow control

        Returns:
            AsyncConsumer: A new consumer instance

        Raises:
            RuntimeError: If consumer creation fails
            ArgumentOutOfRangeException: If destination address format is invalid
        """
        if not validate_address(destination):
            raise ArgumentOutOfRangeException(
                "destination address must start with /queues or /exchanges"
            )
        if consumer_options is not None:
            consumer_options.validate(
                {
                    "4.0.0": self._connection._is_server_version_gte("4.0.0"),
                    "4.1.0": self._connection._is_server_version_gte("4.1.0"),
                    "4.2.0": self._connection._is_server_version_gte("4.2.0"),
                }
            )

        async with self._connection_lock:
            async_consumer = AsyncConsumer(
                self._connection._conn,
                destination,
                message_handler,  # pyright: ignore[reportArgumentType]
                consumer_options,
                credit,
                connection_lock=self._connection_lock,
            )
            await async_consumer.open()
            if async_consumer._consumer is None:
                raise RuntimeError("Failed to create consumer")
            self._set_connection_consumers(async_consumer._consumer)  # TODO: check this
            self._async_consumers.append(async_consumer)

            async_consumer._set_remove_callback(self._remove_consumer)

            return async_consumer

    async def refresh_token(self, token: str) -> None:
        """
        Refresh the oauth token

        Args:
            token: the oauth token to refresh

        Raises:
            ValidationCodeException: If oauth is not enabled
        """
        async with self._connection_lock:
            await asyncio.to_thread(self._connection.refresh_token, token)

    async def __aenter__(self) -> AsyncConnection:
        """ "Async context manager entry."""
        await self.dial()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[object],
    ) -> None:
        """Async context manager exit."""
        await self.close()

    @property
    def active_producers(self) -> int:
        """Get the number of active producers of the connection."""
        return len(self._async_publishers)

    @property
    def active_consumers(self) -> int:
        """Get the number of active consumers of the connection."""
        return len(self._async_consumers)
