from typing import Annotated, Callable, Optional, TypeVar

from .connection import Connection
from .ssl_configuration import SslConfigurationContext

MT = TypeVar("MT")
CB = Annotated[Callable[[MT], None], "Message callback type"]


class Environment:

    def __init__(self):  # type: ignore

        self._connections = []

    def connection(
        self,
        # single-node mode
        url: Optional[str] = None,
        # multi-node mode
        urls: Optional[list[str]] = None,
        ssl_context: Optional[SslConfigurationContext] = None,
        on_disconnection_handler: Optional[CB] = None,  # type: ignore
    ) -> Connection:
        connection = Connection(
            url=url,
            urls=urls,
            ssl_context=ssl_context,
            on_disconnection_handler=on_disconnection_handler,
        )

        self._connections.append(connection)
        return connection

    def close(self) -> None:
        for connection in self._connections:
            connection._close()
