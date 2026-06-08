from __future__ import annotations

import asyncio
import logging
from typing import Optional, Union

from ..entities import (
    AmqpUri,
    OAuth2Options,
    RecoveryConfiguration,
)
from ..ssl_configuration import (
    PosixSslConfigurationContext,
    WinSslConfigurationContext,
)
from .connection import AsyncConnection

logger = logging.getLogger(__name__)


class AsyncEnvironment:
    """
    Asyncio-compatible facade around Environment.

    This class serves as a connection pooler to maintain compatibility with other clients.
    It manages a collection of connections and provides methods for creating and managing
    these connections.

    Attributes:
        _connections (list[AsyncConnection]): List of active async connections managed by this environment
        _connections_lock (asyncio.Lock): Lock for coordinating access to the connections list
        _uri (Optional[str]): Single node connection URI
        _uris (Optional[list[str]]): List of URIs for multi-node setup
        _ssl_context (Union[PosixSslConfigurationContext, WinSslConfigurationContext, None]):
            SSL configuration for secure connections
        _oauth2_options (Optional[OAuth2Options]): OAuth2 options for authentication
        _recovery_configuration (RecoveryConfiguration): Configuration for connection recovery
    """

    def __init__(
        self,  # single-node mode
        uri: Optional[str] = None,
        # multi-node mode
        uris: Optional[list[str]] = None,
        # structured single-node mode
        amqp_uri: Optional[AmqpUri] = None,
        ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = None,
        oauth2_options: Optional[OAuth2Options] = None,
        recovery_configuration: RecoveryConfiguration = RecoveryConfiguration(),
    ):
        """
        Initialize AsyncEnvironment.

        Exactly one of ``uri``, ``uris``, or ``amqp_uri`` must be provided.

        Args:
            uri: Single node connection URI string (e.g. ``"amqp://guest:guest@localhost:5672/"``).
            uris: List of URI strings for multi-node setup.
            amqp_uri: Structured single-node connection parameters as an :class:`AmqpUri`
                instance.  Converted to a URI string internally.
            ssl_context: SSL configuration for secure connections.
            oauth2_options: OAuth2 options for authentication.
            recovery_configuration: Configuration for connection recovery.

        Raises:
            ValueError: If more than one of 'uri', 'uris', 'amqp_uri' is specified,
                or if none is specified.
        """
        set_count = sum(x is not None for x in [uri, uris, amqp_uri])
        if set_count > 1:
            raise ValueError(
                "Cannot specify more than one of 'uri', 'uris', or 'amqp_uri'. "
                "Choose one connection mode."
            )
        if set_count == 0:
            raise ValueError(
                "Must specify exactly one of 'uri', 'uris', or 'amqp_uri' for connection."
            )

        if amqp_uri is not None:
            self._uri: Optional[str] = amqp_uri.to_uri()
            self._uris: Optional[list[str]] = None
        else:
            self._uri = uri
            self._uris = uris
        self._ssl_context = ssl_context
        self._oauth2_options = oauth2_options
        self._recovery_configuration = recovery_configuration
        self._connections: list[AsyncConnection] = []
        self._connections_lock = asyncio.Lock()

    def _remove_connection(self, connection: AsyncConnection) -> None:
        """Remove a connection from the environment's tracking list."""
        if connection in self._connections:
            self._connections.remove(connection)

    async def connection(self) -> AsyncConnection:
        """
        Create and return a new connection.

        This method supports both single-node and multi-node configurations, with optional
        SSL/TLS security and disconnection handling.

        Returns:
            AsyncConnection: A new connection instance

        Raises:
            ValueError: If neither uri nor uris is provided
        """
        async with self._connections_lock:
            connection = AsyncConnection(
                uri=self._uri,
                uris=self._uris,
                ssl_context=self._ssl_context,
                oauth2_options=self._oauth2_options,
                recovery_configuration=self._recovery_configuration,
            )
            logger.debug("AsyncEnvironment: Creating new async connection")
            self._connections.append(connection)

            connection._set_remove_callback(self._remove_connection)

            return connection

    async def close(self) -> None:
        """
        Close all active connections.

        Iterates through all connections managed by this environment and closes them.
        This method should be called when shutting down the application to ensure
        proper cleanup of resources.
        """
        errors = []

        async with self._connections_lock:
            connections_to_close = self._connections[:]

        for connection in connections_to_close:
            try:
                await connection.close()
            except Exception as e:
                errors.append(e)
                logger.error(f"Exception closing async connection: {e}")

        if errors:
            raise RuntimeError(
                f"Errors closing async connections: {'; '.join([str(e) for e in errors])}"
            )

    async def connections(self) -> list[AsyncConnection]:
        """
        Get the list of active connections.

        Returns:
            list[AsyncConnection]: List of all active connections managed by this environment
        """
        return self._connections

    async def __aenter__(self) -> AsyncEnvironment:
        """Async context manager entry."""
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
    def active_connections(self) -> int:
        """Returns the number of active connections"""
        return len(self._connections)
