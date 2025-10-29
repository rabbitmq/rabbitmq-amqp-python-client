import asyncio
import logging
from types import TracebackType
from typing import Callable, Optional, Type

from ..publisher import Publisher
from ..qpid.proton._delivery import Delivery
from ..qpid.proton._message import Message
from ..qpid.proton.utils import BlockingConnection

logger = logging.getLogger(__name__)


class AsyncPublisher:
    """Asyncio-compatible facade around Publisher."""

    def __init__(
        self,
        conn: BlockingConnection,
        addr: str = "",
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        connection_lock: Optional[asyncio.Lock] = None,
    ):
        self._publisher = Publisher(conn, addr)
        self._loop = loop
        self._connection_lock = connection_lock or asyncio.Lock()
        self._remove_callback = None

    def _set_remove_callback(
        self, callback: Optional[Callable[["AsyncPublisher"], None]]
    ) -> None:
        self._remove_callback = callback

    async def publish(self, message: Message) -> Delivery:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None, self._publisher.publish, message
            )

    async def close(self) -> None:
        if not self.is_open:
            return
        async with self._connection_lock:
            await self._event_loop.run_in_executor(None, self._publisher.close)

        if self._remove_callback is not None:
            self._remove_callback(self)

    async def __aenter__(self) -> "AsyncPublisher":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    @property
    def _event_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop or asyncio.get_running_loop()

    @property
    def is_open(self) -> bool:
        return self._publisher.is_open

    @property
    def address(self) -> str:
        return self._publisher.address
