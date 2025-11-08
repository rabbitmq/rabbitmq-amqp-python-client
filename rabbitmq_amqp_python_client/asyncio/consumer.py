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
    """
    Asyncio-compatible facade around Consumer.

    This class wraps the synchronous Consumer to provide an async interface.
    All blocking operations are executed in threads to avoid blocking the event loop.

    Note:
        The underlying Proton BlockingConnection is NOT thread-safe. A lock must
        be provided by the caller (AsyncConnection) to serialize all operations.

    Attributes:
        _consumer (Optional[Consumer]): The underlying synchronous Consumer
        _conn (BlockingConnection): The shared blocking connection
        _addr (str): The address to consume from
        _handler (Optional[AMQPMessagingHandler]): Optional message handling callback
        _stream_options (Optional[ConsumerOptions]): Configuration for stream consumption
        _credit (Optional[int]): Flow control credit value
        _connection_lock (asyncio.Lock): Lock for coordinating access to the shared connection
        _remove_callback (Optional[Callable[[AsyncConsumer], None]]): Callback on close
        _opened (bool): Indicates if the consumer is opened
    """

    def __init__(
        self,
        conn: BlockingConnection,
        addr: str,
        handler: Optional[AMQPMessagingHandler] = None,
        stream_options: Optional[ConsumerOptions] = None,
        credit: Optional[int] = None,
        *,
        connection_lock: asyncio.Lock,
    ):
        """
        Initialize AsyncConsumer.

        Args:
            conn: The blocking connection to use
            addr: The address to consume from
            handler: Optional message handler for processing received messages
            stream_options: Optional configuration for stream-based consumption
            credit: Optional credit value for flow control
            connection_lock: Lock for coordinating access to the shared connection.
                           Must be created by the caller (AsyncConnection).
        """
        self._conn = conn
        self._addr = addr
        self._handler = handler
        self._stream_options = stream_options
        self._credit = credit
        self._consumer: Optional[Consumer] = None
        self._connection_lock = connection_lock
        self._remove_callback: Optional[Callable[[AsyncConsumer], None]] = None
        self._opened = False

    def _set_remove_callback(
        self, callback: Optional[Callable[["AsyncConsumer"], None]]
    ) -> None:
        """Set callback to be called when consumer is closed."""
        self._remove_callback = callback

    async def open(self) -> None:
        """
        Open the consumer in an async context.

        Creates the underlying Consumer instance. This should be called
        before using the consumer, either explicitly or via async context manager.
        """
        if self._opened:
            return

        # Create consumer in thread to avoid blocking event loop
        self._consumer = await asyncio.to_thread(
            Consumer,
            self._conn,
            self._addr,
            self._handler,
            self._stream_options,
            self._credit,
        )
        self._opened = True
        logger.debug(f"AsyncConsumer opened for address: {self._addr}")

    async def consume(
        self, timeout: Union[None, Literal[False], float] = False
    ) -> Message:
        """
        Consume a message from the queue.

        Args:
            timeout: The time to wait for a message.
                    None: Defaults to 60s
                    float: Wait for specified number of seconds

        Returns:
            Message: The received message

        Raises:
            RuntimeError: If consumer is not opened

        Note:
            The return type might be None if no message is available and timeout occurs,
            but this is handled by the cast to Message.
        """
        if not self._opened or self._consumer is None:
            raise RuntimeError(
                "Consumer is not opened. Call open() or use async context manager."
            )

        async with self._connection_lock:
            return await asyncio.to_thread(self._consumer.consume, timeout)

    async def close(self) -> None:
        """
        Close the consumer connection.

        Closes the receiver if it exists and cleans up resources.
        """
        if not self._opened or self._consumer is None:
            return

        try:
            async with self._connection_lock:
                await asyncio.to_thread(self._consumer.close)

            logger.debug(f"AsyncConsumer closed for address: {self._addr}")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}", exc_info=True)
            raise
        finally:
            self._opened = False
            self._consumer = None
            if self._remove_callback is not None:
                callback = self._remove_callback
                self._remove_callback = None  # Prevent double-call
                callback(self)

    async def run(self) -> None:
        """
        Run the consumer in continuous mode.

        Starts the consumer's container to process messages continuously.

        Raises:
            RuntimeError: If consumer is not opened
        """
        if not self._opened or self._consumer is None:
            raise RuntimeError(
                "Consumer is not opened. Call open() or use async context manager."
            )

        async with self._connection_lock:
            await asyncio.to_thread(self._consumer.run)

    async def stop(self) -> None:
        """
        Stop the consumer's continuous processing.

        Stops the consumer's container, halting message processing.
        This should be called to cleanly stop a consumer that was started with run().

        Raises:
            RuntimeError: If consumer is not opened
        """
        if not self._opened or self._consumer is None:
            raise RuntimeError(
                "Consumer is not opened. Call open() or use async context manager."
            )

        async with self._connection_lock:
            await asyncio.to_thread(self._consumer.stop)

    async def __aenter__(self) -> AsyncConsumer:
        """Async context manager entry."""
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        """Async context manager exit."""
        await self.close()

    @property
    def address(self) -> str:
        """Get the current consumer address."""
        return self._addr

    @property
    def handler(self) -> Optional[AMQPMessagingHandler]:
        """Get the current message handler."""
        return self._handler
