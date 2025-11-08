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
    """
    Asyncio-compatible facade for the Publisher class.

    This class wraps the synchronous Publisher to provide an async interface.
    All blocking operations are executed in threads to avoid blocking the event loop.

    Note:
        The underlying Proton BlockingConnection is NOT thread-safe. A lock must
        be provided by the caller (AsyncConnection) to serialize all operations.

    Attributes:
        _publisher (Optional[Publisher]): The underlying synchronous Publisher
        _conn (BlockingConnection): The shared blocking connection
        _addr (str): The default address for publishing
        _connection_lock (asyncio.Lock): Lock for coordinating access to the shared connection
        _remove_callback (Optional[Callable[[AsyncPublisher], None]]): Callback on close
        _opened (bool): Indicates if the publisher is opened
    """

    def __init__(
        self,
        conn: BlockingConnection,
        addr: str = "",
        *,
        connection_lock: asyncio.Lock,
    ) -> None:
        """
        Initialize AsyncPublisher.

        Args:
            conn: The blocking connection to use
            addr: Optional default address for publishing
            connection_lock: Lock for coordinating access to the shared connection.
                           Must be created by the caller (AsyncConnection).

        Note:
            The underlying Publisher is NOT created here. Call open() explicitly
            or use the async context manager.
        """
        self._conn = conn
        self._addr = addr
        self._publisher: Optional[Publisher] = None
        self._connection_lock = connection_lock
        self._remove_callback: Optional[Callable[["AsyncPublisher"], None]] = None
        self._opened = False

    def _set_remove_callback(
        self, callback: Optional[Callable[["AsyncPublisher"], None]]
    ) -> None:
        """Set callback to be called when publisher is closed."""
        self._remove_callback = callback

    async def open(self) -> None:
        """
        Open the publisher in an async context.

        Creates the underlying Publisher instance. This should be called
        before using the publisher, either explicitly or via async context manager.
        """
        if self._opened:
            return

        # Create publisher in thread to avoid blocking event loop
        # Note: We don't need the lock here because Publisher.__init__ doesn't
        # send any network traffic, it just initializes local state
        self._publisher = await asyncio.to_thread(Publisher, self._conn, self._addr)
        self._opened = True
        logger.debug(f"AsyncPublisher opened for address: {self._addr}")

    async def publish(self, message: Message) -> Delivery:
        """
        Publish a message to RabbitMQ.

        The message can be sent to either the publisher's default address or
        to an address specified in the message itself, but not both.

        Args:
            message: The message to publish

        Returns:
            Delivery: The delivery confirmation from RabbitMQ

        Raises:
            RuntimeError: If publisher is not opened
            ValidationCodeException: If address is specified in both message and publisher
            ArgumentOutOfRangeException: If message address format is invalid
        """
        if not self._opened or self._publisher is None:
            raise RuntimeError(
                "Publisher is not opened. Call open() or use async context manager."
            )

        async with self._connection_lock:
            return await asyncio.to_thread(self._publisher.publish, message)

    async def close(self) -> None:
        """
        Close the publisher connection.

        Closes the sender if it exists and cleans up resources.
        """
        if not self._opened or self._publisher is None:
            return

        try:
            async with self._connection_lock:
                await asyncio.to_thread(self._publisher.close)

            logger.debug(f"AsyncPublisher closed for address: {self._addr}")
        except Exception as e:
            logger.error(f"Error closing publisher: {e}", exc_info=True)
            raise
        finally:
            self._opened = False
            self._publisher = None
            if self._remove_callback is not None:
                callback = self._remove_callback
                self._remove_callback = None  # Prevent double-call
                callback(self)

    async def __aenter__(self) -> "AsyncPublisher":
        """Async context manager entry."""
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Async context manager exit."""
        await self.close()

    @property
    def is_open(self) -> bool:
        """Check if publisher is open and ready to send messages."""
        return self._opened and self._publisher is not None and self._publisher.is_open

    @property
    def address(self) -> str:
        """Get the current publisher address."""
        return self._addr
