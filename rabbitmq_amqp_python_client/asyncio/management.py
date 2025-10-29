from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional, Union

from ..entities import (
    ExchangeCustomSpecification,
    ExchangeSpecification,
    ExchangeToExchangeBindingSpecification,
    ExchangeToQueueBindingSpecification,
    QueueInfo,
)
from ..management import Management
from ..qpid.proton._message import Message
from ..qpid.proton.utils import BlockingConnection
from ..queues import (
    ClassicQueueSpecification,
    QuorumQueueSpecification,
    StreamSpecification,
)

logger = logging.getLogger(__name__)


class AsyncManagement:
    """Asyncio-compatible facade around the Management class"""

    def __init__(
        self,
        conn: BlockingConnection,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        connection_lock: Optional[asyncio.Lock] = None,
    ):
        self._management = Management(conn)
        self._loop = loop
        self._connection_lock = connection_lock or asyncio.Lock()

    def _set_remove_callback(
        self, callback: Optional[Callable[[AsyncManagement], None]]
    ) -> None:
        self._remove_callback = callback

    async def open(self) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(None, self._management.open)

    async def close(self) -> None:
        try:
            async with self._connection_lock:
                await self._event_loop.run_in_executor(None, self._management.close)
        except Exception as e:
            logger.error(f"Error closing management: {e}")
            raise
        finally:
            if self._remove_callback is not None:
                self._remove_callback(self)

    async def request(
        self,
        body: Any,
        path: str,
        method: str,
        expected_response_codes: list[int],
    ) -> Message:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None,
                self._management.request,
                body,
                path,
                method,
                expected_response_codes,
            )

    async def declare_exchange(
        self,
        exchange_specification: Union[
            ExchangeSpecification, ExchangeCustomSpecification
        ],
    ) -> Union[ExchangeSpecification, ExchangeCustomSpecification]:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None,
                self._management.declare_exchange,
                exchange_specification,
            )

    async def declare_queue(
        self,
        queue_specification: Union[
            ClassicQueueSpecification, QuorumQueueSpecification, StreamSpecification
        ],
    ) -> Union[
        ClassicQueueSpecification, QuorumQueueSpecification, StreamSpecification
    ]:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None,
                self._management.declare_queue,
                queue_specification,
            )

    async def delete_exchange(self, name: str) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(
                None,
                self._management.delete_exchange,
                name,
            )

    async def delete_queue(self, name: str) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(
                None,
                self._management.delete_queue,
                name,
            )

    async def bind(
        self,
        bind_specification: Union[
            ExchangeToQueueBindingSpecification, ExchangeToExchangeBindingSpecification
        ],
    ) -> str:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None,
                self._management.bind,
                bind_specification,
            )

    async def unbind(
        self,
        bind_specification: Union[
            str,
            ExchangeToQueueBindingSpecification,
            ExchangeToExchangeBindingSpecification,
        ],
    ) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(
                None,
                self._management.unbind,
                bind_specification,
            )

    async def purge_queue(self, name: str) -> int:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None,
                self._management.purge_queue,
                name,
            )

    async def queue_info(self, name: str) -> QueueInfo:
        async with self._connection_lock:
            return await self._event_loop.run_in_executor(
                None,
                self._management.queue_info,
                name,
            )

    async def refresh_token(self, token: str) -> None:
        async with self._connection_lock:
            await self._event_loop.run_in_executor(
                None,
                self._management.refresh_token,
                token,
            )

    async def __aenter__(self) -> AsyncManagement:
        await self.open()
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
