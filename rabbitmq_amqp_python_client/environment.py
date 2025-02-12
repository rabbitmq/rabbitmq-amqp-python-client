# For the moment this is just a Connection pooler to keep compatibility with other clients
import logging
from typing import Annotated, Callable, Optional, TypeVar

from .connection import Connection
from .ssl_configuration import SslConfigurationContext

logger = logging.getLogger(__name__)

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], None], "Message callback type"]


class Environment:

    def __init__(self):  # type: ignore

        self._connections: list[Connection] = []

    def connection(
        self,
        # single-node mode
        uri: Optional[str] = None,
        # multi-node mode
        uris: Optional[list[str]] = None,
        ssl_context: Optional[SslConfigurationContext] = None,
        on_disconnection_handler: Optional[CB] = None,  # type: ignore
    ) -> Connection:
        connection = Connection(
            uri=uri,
            uris=uris,
            ssl_context=ssl_context,
            on_disconnection_handler=on_disconnection_handler,
        )
        logger.debug("Environment: Creating and returning a new connection")
        self._connections.append(connection)
        connection._set_environment_connection_list(self._connections)
        return connection

    # closes all active connections
    def close(self) -> None:
        logger.debug("Environment: Closing all pending connections")
        for connection in self._connections:
            connection.close()

    def connections(self) -> list[Connection]:
        return self._connections
