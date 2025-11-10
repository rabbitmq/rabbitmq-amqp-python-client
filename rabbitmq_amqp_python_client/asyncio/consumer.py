from __future__ import annotations

import asyncio
import logging
import threading
from types import TracebackType
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    Type,
    Union,
)

from ..amqp_consumer_handler import AMQPMessagingHandler
from ..consumer import Consumer
from ..entities import ConsumerOptions
from ..qpid.proton._exceptions import Timeout
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
        _running (bool): Indicates if the consumer is running
        _run_task (Optional[asyncio.Task[Any]]): The task running the consumer loop
        _stop_event (threading.Event): Event to signal stopping the consumer loop
        _poll_timeout (float): Timeout for polling in the run loop (i.e. the maximum time to block
                               waiting for messages before checking for stop signal)
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
        poll_timeout: float = 0.5,
    ):
        """
        Initialize AsyncConsumer.

        Args:
            conn (BlockingConnection): The blocking connection to use
            addr (str): The address to consume from
            handler (Optional[AMQPMessagingHandler]): Optional message handler for processing received messages
            stream_options (Optional[ConsumerOptions]): Optional configuration for stream-based consumption
            credit (Optional[int]): Optional credit value for flow control
            connection_lock (asyncio.Lock): Lock for coordinating access to the shared connection.
                            Must be created by the caller (AsyncConnection).
            poll_timeout (float): Timeout for polling in the run loop (i.e. the maximum time to block
                            waiting for messages before checking for stop signal)
        """
        self._conn = conn
        self._addr = addr
        self._handler = handler
        self._stream_options = stream_options
        self._credit = credit
        self._consumer: Optional[Consumer] = None
        self._connection_lock = connection_lock
        self._poll_timeout = poll_timeout
        self._remove_callback: Optional[Callable[[AsyncConsumer], None]] = None
        self._opened = False
        self._running = False
        self._run_task: Optional[asyncio.Task[Any]] = None
        self._stop_event = threading.Event()

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

        self._consumer = await asyncio.to_thread(
            Consumer,
            self._conn,
            self._addr,
            self._handler,
            self._stream_options,
            self._credit,
        )
        self._opened = True
        self._stop_event.clear()
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
            # Signal to stop the run loop if running
            await self.stop_processing()

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
                self._remove_callback = None
                callback(self)

    async def run(self) -> None:
        """
        Run the consumer in continuous mode.

        Starts the consumer's container to process messages continuously
        until until stop_processing() is called.

        Raises:
            RuntimeError: If consumer is not opened
        """
        if not self._opened or self._consumer is None:
            raise RuntimeError(
                "Consumer is not opened. Call open() or use async context manager."
            )

        self._running = True
        self._run_task = asyncio.current_task()
        self._stop_event.clear()

        logger.debug(f"Starting consumer run loop for address: {self._addr}")

        def _blocking_run() -> None:
            """
            Run consumer with periodic stop checks using container processing.

            This method will poll the container for work with a timeout (`_poll_timeout`),
            allowing it to check periodically if a stop has been requested via `_stop_event`.
            """
            try:
                if self._consumer is None:
                    raise RuntimeError("Consumer is not initialized")
                if self._consumer._receiver is None:
                    raise RuntimeError("Consumer receiver is not initialized")

                container = self._consumer._receiver.container
                if container is None:
                    raise RuntimeError("Container is not available from receiver")

                # Process messages with periodic timeout checks
                while not self._stop_event.is_set():
                    try:
                        container.do_work(self._poll_timeout)

                    except Timeout:
                        if self._stop_event.is_set():
                            logger.debug("Stop event set, exiting run loop")
                            break
                        continue
                    except Exception as e:
                        if self._stop_event.is_set():
                            logger.debug(f"Exception during shutdown (expected): {e}")
                            break
                        logger.error(
                            f"Unexpected exception in container.do_work: {e}",
                            exc_info=True,
                        )
                        raise

            except Exception as e:
                if not self._stop_event.is_set():
                    logger.error(f"Error in blocking run: {e}", exc_info=True)
                    raise
                else:
                    logger.debug(f"Consumer run interrupted (expected): {e}")

        try:
            await asyncio.to_thread(_blocking_run)
        except asyncio.CancelledError:
            logger.debug("Consumer run task cancelled")
            self._stop_event.set()
            raise
        except Exception as e:
            logger.error(f"Error in consumer run: {e}", exc_info=True)
            raise
        finally:
            self._running = False
            self._run_task = None
            logger.debug(f"Consumer run loop stopped for address: {self._addr}")

    async def stop_processing(self) -> None:
        """
        Stop the consumer's continuous processing loop.

        This stops the run() method gracefully without closing the receiver
        or stopping the container. Use this when you want to stop consuming
        messages but keep the connection and other components alive.
        """
        if not self._running:
            logger.debug("Consumer not running")
            return

        logger.debug(f"Stopping consumer processing for address: {self._addr}")

        self._running = False
        self._stop_event.set()

        # Wait for the run task to complete
        if self._run_task and not self._run_task.done():
            try:
                await asyncio.wait_for(self._run_task, timeout=5.0)
                logger.debug("Run task completed")
            except asyncio.TimeoutError:
                logger.warning("Run task did not complete within timeout")

        logger.debug(f"Consumer processing stopped for address: {self._addr}")

    async def stop(self) -> None:
        """
        Stop the consumer's continuous processing.

        Stops the consumer's container, halting message processing.

        Note:
            This will stop the container which may affect other components
            (publishers, management, etc.) sharing the same connection.
            In most cases, you should use `stop_processing()` instead.

        This method is provided for compatibility with the sync Consumer API.
        """
        if not self._opened or self._consumer is None:
            logger.debug("Consumer not opened, nothing to stop")
            return

        logger.debug(f"Stopping underlying consumer for address: {self._addr}")

        # First stop processing if still running
        await self.stop_processing()

        try:
            await asyncio.to_thread(self._consumer.stop)
            logger.debug("Underlying consumer stopped")
        except Exception as e:
            logger.warning(f"Error stopping underlying consumer: {e}")

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

    @property
    def is_running(self) -> bool:
        """Check if the consumer is currently running."""
        return self._running
