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
    """Asyncio-compatible facade around Connection."""

    def __init__(
        self,
        uri: Optional[str] = None,
        uris: Optional[list[str]] = None,
        ssl_context: Union[
            PosixSslConfigurationContext, WinSslConfigurationContext, None
        ] = None,
        oauth2_options: Optional[OAuth2Options] = None,
        recovery_configuration: RecoveryConfiguration = RecoveryConfiguration(),
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._connection = Connection(
            uri=uri,
            uris=uris,
            ssl_context=ssl_context,
            oauth2_options=oauth2_options,
            recovery_configuration=recovery_configuration,
        )
        self._loop = loop
        self._connection_lock = asyncio.Lock()
        self._async_publishers: list[AsyncPublisher] = []
        self._async_consumers: list[AsyncConsumer] = []
        self._async_managements: list[AsyncManagement] = []
        self._remove_callback = None

    async def dial(self) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(None, self._connection.dial)

    def _set_remove_callback(
        self, callback: Optional[Callable[["AsyncConnection"], None]]
    ) -> None:
        self._remove_callback = callback

    def _remove_publisher(self, publisher: AsyncPublisher) -> None:
        if publisher in self._async_publishers:
            self._async_publishers.remove(publisher)

    def _remove_consumer(self, consumer: AsyncConsumer) -> None:
        if consumer in self._async_consumers:
            self._async_consumers.remove(consumer)

    def _remove_management(self, management: AsyncManagement) -> None:
        if management in self._async_managements:
            self._async_managements.remove(management)

    async def close(self) -> None:
        logger.debug("Closing async connection")
        try:
            for async_publisher in self._async_publishers[:]:
                await async_publisher.close()
            for async_consumer in self._async_consumers[:]:
                await async_consumer.close()
            for async_management in self._async_managements[:]:
                await async_management.close()

            async with self._connection_lock:
                await self._event_loop.run_in_executor(None, self._connection.close)
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
        if len(self._async_managements) > 0:
            return self._async_managements[0]

        async_management: Optional[AsyncManagement] = None
        async with self._connection_lock:
            if len(self._async_managements) == 0:
                async_management = AsyncManagement(
                    self._connection._conn,
                    loop=self._event_loop,
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
        publisher._set_publishers_list(
            [async_publisher._publisher for async_publisher in self._async_publishers]
        )
        self._connection._publishers.append(publisher)

    async def publisher(self, destination: str = "") -> AsyncPublisher:
        if destination != "":
            if not validate_address(destination):
                raise ArgumentOutOfRangeException(
                    "destination address must start with /queues or /exchanges"
                )

        async with self._connection_lock:
            async_publisher = AsyncPublisher(
                self._connection._conn,
                destination,
                loop=self._event_loop,
                connection_lock=self._connection_lock,
            )
            self._set_connection_publishers(
                async_publisher._publisher
            )  # TODO: check this
            self._async_publishers.append(async_publisher)

            async_publisher._set_remove_callback(self._remove_publisher)

            return async_publisher

    def _set_connection_consumers(self, consumer: Consumer) -> None:
        self._connection._consumers.append(consumer)

    async def consumer(
        self,
        destination: str,
        message_handler: Optional[MessagingHandler] = None,
        consumer_options: Optional[ConsumerOptions] = None,
        credit: Optional[int] = None,
    ) -> AsyncConsumer:
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
                message_handler,  # type: ignore
                consumer_options,
                credit,
                loop=self._event_loop,
                connection_lock=self._connection_lock,
            )
            self._set_connection_consumers(async_consumer._consumer)  # TODO: check this
            self._async_consumers.append(async_consumer)

            async_consumer._set_remove_callback(self._remove_consumer)

            return async_consumer

    async def refresh_token(self, token: str) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(
                None,
                self._connection.refresh_token,
                token,
            )

    async def __aenter__(self) -> "AsyncConnection":
        await self.dial()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[object],
    ) -> None:
        await self.close()

    @property
    def _event_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop or asyncio.get_running_loop()

    @property
    def active_producers(self) -> int:
        return len(self._async_publishers)

    @property
    def active_consumers(self) -> int:
        return len(self._async_consumers)
