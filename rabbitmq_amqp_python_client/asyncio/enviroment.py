import asyncio
import logging
from typing import Optional, Union

from ..entities import OAuth2Options, RecoveryConfiguration
from ..ssl_configuration import (
    PosixSslConfigurationContext,
    WinSslConfigurationContext,
)
from .connection import AsyncConnection

logger = logging.getLogger(__name__)


class AsyncEnvironment:
    """Asyncio-compatible facade around Environment."""

    def __init__(
        self,  # single-node mode
        uri: Optional[str] = None,
        # multi-node mode
        uris: Optional[list[str]] = None,
        ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = None,
        oauth2_options: Optional[OAuth2Options] = None,
        recovery_configuration: RecoveryConfiguration = RecoveryConfiguration(),
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        if uri is not None and uris is not None:
            raise ValueError(
                "Cannot specify both 'uri' and 'uris'. Choose one connection mode."
            )
        if uri is None and uris is None:
            raise ValueError("Must specify either 'uri' or 'uris' for connection.")

        self._uri = uri
        self._uris = uris
        self._ssl_context = ssl_context
        self._oauth2_options = oauth2_options
        self._recovery_configuration = recovery_configuration
        self._loop = loop
        self._connections: list[AsyncConnection] = []
        self._connections_lock = asyncio.Lock()

    async def connection(self) -> AsyncConnection:
        async with self._connections_lock:
            connection = AsyncConnection(
                uri=self._uri,
                uris=self._uris,
                ssl_context=self._ssl_context,
                oauth2_options=self._oauth2_options,
                recovery_configuration=self._recovery_configuration,
                loop=self._loop,
            )
            logger.debug("AsyncEnvironment: Creating new async connection")
            self._connections.append(connection)
            return connection

    async def close(self) -> None:
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
            raise RuntimeError(f"Errors closing async connections: {'; '.join(errors)}")

    async def connections(self) -> list[AsyncConnection]:
        return self._connections

    async def __aenter__(self) -> "AsyncEnvironment":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[object],
    ) -> None:
        await self.close()

    @property
    def active_connections(self) -> int:
        return len(self._connections)
