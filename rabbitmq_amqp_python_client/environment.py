# For the moment this is just a Connection pooler to keep compatibility with other clients
import logging
from typing import (
    Annotated,
    Callable,
    Optional,
    TypeVar,
    Union,
)

from .connection import Connection
from .entities import RecoveryConfiguration
from .ssl_configuration import (
    PosixSslConfigurationContext,
    WinSslConfigurationContext,
)

logger = logging.getLogger(__name__)

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], None], "Message callback type"]


class Environment:
    """
    Environment class for managing AMQP connections.

    This class serves as a connection pooler to maintain compatibility with other clients.
    It manages a collection of connections and provides methods for creating and managing
    these connections.

    Attributes:
        _connections (list[Connection]): List of active connections managed by this environment
    """

    def __init__(
        self,  # single-node mode
        uri: Optional[str] = None,
        # multi-node mode
        uris: Optional[list[str]] = None,
        ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = None,
        recovery_configuration: RecoveryConfiguration = RecoveryConfiguration(),
    ):
        """
        Initialize a new Environment instance.

        Creates an empty list to track active connections.

        Args:
            uri: Single node connection URI
            uris: List of URIs for multi-node setup
            ssl_context: SSL configuration for secure connections
            on_disconnection_handler: Callback for handling disconnection events

        """
        if uri is not None and uris is not None:
            raise ValueError(
                "Cannot specify both 'uri' and 'uris'. Choose one connection mode."
            )
        if uri is None and uris is None:
            raise ValueError("Must specify either 'uri' or 'uris' for connection.")
        self._uri = uri
        self._uris = uris
        self._ssl_context = ssl_context
        self._recovery_configuration = recovery_configuration
        self._connections: list[Connection] = []

    def connection(
        self,
    ) -> Connection:
        """
        Create and return a new connection.

        This method supports both single-node and multi-node configurations, with optional
        SSL/TLS security and disconnection handling.

        Returns:
            Connection: A new connection instance

        Raises:
            ValueError: If neither uri nor uris is provided
        """
        connection = Connection(
            uri=self._uri,
            uris=self._uris,
            ssl_context=self._ssl_context,
            recovery_configuration=self._recovery_configuration,
        )
        logger.debug("Environment: Creating and returning a new connection")
        self._connections.append(connection)
        connection._set_environment_connection_list(self._connections)
        return connection

    # closes all active connections
    def close(self) -> None:
        """
        Close all active connections.

        Iterates through all connections managed by this environment and closes them.
        This method should be called when shutting down the application to ensure
        proper cleanup of resources.
        """
        errors = []
        for connection in self._connections:
            try:
                connection.close()
            except Exception as e:
                errors.append(f"Exception closing connection: {str(e)}")
                logger.error(f"Exception closing connection: {e}")

        if errors:
            raise RuntimeError(f"Errors closing connections: {'; '.join(errors)}")

    def connections(self) -> list[Connection]:
        """
        Get the list of active connections.

        Returns:
            list[Connection]: List of all active connections managed by this environment
        """
        return self._connections

    def __enter__(self) -> "Environment":
        """Context manager support"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[no-untyped-def]
        """Close all connections when the context terminate."""
        self.close()

    @property
    def active_connections(self) -> int:
        """Returns the number of active connections"""
        return len(self._connections)
