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
    """
    Asyncio-compatible facade around the Management class

    This class provides methods for declaring and managing exchanges, queues,
    and bindings in RabbitMQ. It uses a blocking connection to communicate
    with the RabbitMQ management interface.

    Note:
        The underlying Proton BlockingConnection is NOT thread-safe. A lock must
        be provided by the caller (AsyncConnection) to serialize all operations.

    Attributes:
        _management (Management): The underlying synchronous Management instance
        _conn (BlockingConnection): The shared blocking connection
        _connection_lock (asyncio.Lock): Lock for coordinating access to the shared connection
        _remove_callback (Optional[Callable[[AsyncManagement], None]]): Callback on close
        _opened (bool): Indicates if the management interface is open
    """

    def __init__(
        self,
        conn: BlockingConnection,
        *,
        connection_lock: asyncio.Lock,
    ):
        """
        Initialize AsyncManagement.

        Args:
            conn: The blocking connection to use
            connection_lock: Lock for coordinating access to the shared connection.
                           Must be created by the caller (AsyncConnection).
        """
        self._conn = conn
        self._management = Management(conn)
        self._connection_lock = connection_lock
        self._remove_callback: Optional[Callable[[AsyncManagement], None]] = None
        self._opened = False

    def _check_is_open(self) -> None:
        """
        Check if the management interface is open.

        Raises:
            RuntimeError: If the management interface is not open
        """
        if not self._opened:
            raise RuntimeError("Management interface is not open")

    def _set_remove_callback(
        self, callback: Optional[Callable[[AsyncManagement], None]]
    ) -> None:
        """Set callback to be called when management is closed."""
        self._remove_callback = callback

    async def open(self) -> None:
        """
        Open the management connection by creating sender and receiver.

        Creates sender and receiver if they don't exist, using the management
        node address defined in CommonValues.
        """
        if self._opened:
            return

        async with self._connection_lock:
            await asyncio.to_thread(self._management.open)

        self._opened = True
        logger.debug("AsyncManagement opened")

    async def close(self) -> None:
        """
        Close the management connection.

        Closes both sender and receiver if they exist.
        """
        if not self._opened:
            return

        try:
            async with self._connection_lock:
                await asyncio.to_thread(self._management.close)

            logger.debug("AsyncManagement closed")
        except Exception as e:
            logger.error(f"Error closing management: {e}", exc_info=True)
            raise
        finally:
            self._opened = False
            if self._remove_callback is not None:
                callback = self._remove_callback
                self._remove_callback = None  # Prevent multiple calls
                callback(self)

    async def request(
        self,
        body: Any,
        path: str,
        method: str,
        expected_response_codes: list[int],
    ) -> Message:
        """
        Send a management request with a new UUID.

        Args:
            body: The request body to send
            path: The management API path
            method: The HTTP method to use
            expected_response_codes: List of acceptable response codes

        Returns:
            Message: The response message from the server

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If response code is not in expected_response_codes
        """
        self._check_is_open()
        async with self._connection_lock:
            return await asyncio.to_thread(
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
        """
        Declare a new exchange in RabbitMQ.

        Args:
            exchange_specification: The specification for the exchange to create

        Returns:
            The same specification object that was passed in

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If exchange already exists or other validation fails
        """
        self._check_is_open()
        async with self._connection_lock:
            return await asyncio.to_thread(
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
        """
        Declare a new queue in RabbitMQ.

        Supports declaration of classic queues, quorum queues, and streams.

        Args:
            queue_specification: The specification for the queue to create

        Returns:
            The same specification object that was passed in

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If queue already exists or other validation fails
        """
        self._check_is_open()
        async with self._connection_lock:
            return await asyncio.to_thread(
                self._management.declare_queue,
                queue_specification,
            )

    async def delete_exchange(self, name: str) -> None:
        """
        Delete an exchange.

        Args:
            name: The name of the exchange to delete

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If exchange doesn't exist or deletion fails
        """
        self._check_is_open()
        async with self._connection_lock:
            await asyncio.to_thread(
                self._management.delete_exchange,
                name,
            )

    async def delete_queue(self, name: str) -> None:
        """
        Delete a queue.

        Args:
            name: The name of the queue to delete

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If queue doesn't exist or deletion fails
        """
        self._check_is_open()
        async with self._connection_lock:
            await asyncio.to_thread(
                self._management.delete_queue,
                name,
            )

    async def bind(
        self,
        bind_specification: Union[
            ExchangeToQueueBindingSpecification, ExchangeToExchangeBindingSpecification
        ],
    ) -> str:
        """
        Create a binding between exchanges or between an exchange and a queue.

        Args:
            bind_specification: The specification for the binding to create

        Returns:
            str: The binding path created

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If binding creation fails
        """
        self._check_is_open()
        async with self._connection_lock:
            return await asyncio.to_thread(
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
        """
        Remove a binding between exchanges or between an exchange and a queue.

        Args:
            bind_specification: Either a binding path string or a binding specification

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If unbinding fails
        """
        self._check_is_open()
        async with self._connection_lock:
            await asyncio.to_thread(
                self._management.unbind,
                bind_specification,
            )

    async def purge_queue(self, name: str) -> int:
        """
        Purge all messages from a queue.

        Args:
            name: The name of the queue to purge

        Returns:
            int: The number of messages that were purged

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If queue doesn't exist or purge fails
        """
        self._check_is_open()
        async with self._connection_lock:
            return await asyncio.to_thread(
                self._management.purge_queue,
                name,
            )

    async def queue_info(self, name: str) -> QueueInfo:
        """
        Get information about a queue.

        Args:
            name: The name of the queue to get information about

        Returns:
            QueueInfo: Object containing queue information

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If queue doesn't exist or other errors occur
        """
        self._check_is_open()
        async with self._connection_lock:
            return await asyncio.to_thread(
                self._management.queue_info,
                name,
            )

    async def refresh_token(self, token: str) -> None:
        """
        Refresh the oauth token

        Args:
            token: the oauth token to refresh

        Raises:
            RuntimeError: If management interface is not open
            ValidationCodeException: If oauth is not enabled
        """
        self._check_is_open()
        async with self._connection_lock:
            await asyncio.to_thread(
                self._management.refresh_token,
                token,
            )

    async def __aenter__(self) -> AsyncManagement:
        """Async context manager entry."""
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[object],
    ) -> None:
        """Async context manager exit."""
        await self.close()

    @property
    def is_open(self) -> bool:
        """Check if the management interface is open."""
        return self._opened
