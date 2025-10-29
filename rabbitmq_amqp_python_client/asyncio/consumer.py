from __future__ import annotations

import asyncio
import logging
from types import TracebackType
from typing import Callable, Literal, Optional, Type, Union

from ..amqp_consumer_handler import AMQPMessagingHandler
from ..consumer import Consumer
from ..entities import ConsumerOptions
from ..qpid.proton._message import Message
from ..qpid.proton.utils import BlockingConnection

logger = logging.getLogger(__name__)


class AsyncConsumer:
    """Asyncio-compatible facade around Consumer."""

    def __init__(
        self,
        conn: BlockingConnection,
        addr: str,
        handler: Optional[AMQPMessagingHandler] = None,
        stream_options: Optional[ConsumerOptions] = None,
        credit: Optional[int] = None,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        connection_lock: Optional[asyncio.Lock] = None,
    ):
        self._consumer = Consumer(conn, addr, handler, stream_options, credit)
        self._loop = loop
        self._connection_lock = connection_lock or asyncio.Lock()

    def _set_remove_callback(
        self, callback: Optional[Callable[[AsyncConsumer], None]]
    ) -> None:
        self._remove_callback = callback

    async def consume(
        self, timeout: Union[None, Literal[False], float] = False
    ) -> Message:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None, self._consumer.consume, timeout
            )

    async def close(self) -> None:
        try:
            async with self._connection_lock:
                await self._event_loop.run_in_executor(None, self._consumer.close)
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
            raise
        finally:
            if self._remove_callback is not None:
                self._remove_callback(self)

    async def run(self) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(None, self._consumer.run)

    async def stop(self) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(None, self._consumer.stop)

    async def __aenter__(self) -> AsyncConsumer:
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
    def address(self) -> str:
        return self._consumer.address

    @property
    def handler(self) -> Optional[AMQPMessagingHandler]:
        return self._consumer.handler
