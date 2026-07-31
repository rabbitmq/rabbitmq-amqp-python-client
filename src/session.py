"""AMQP 1.0 sessions: ``begin``/``end``, link registry and transfer-id bookkeeping.

A :class:`Session` owns one connection channel, allocates link handles, tracks
the session's transfer-id windows, and dispatches every inbound performative to
the :class:`~.link.Link` it belongs to. Frames are dispatched on the
connection's frame-reader thread, so nothing here may block waiting on a caller
thread: inbound handling only updates state and notifies conditions.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from .exceptions import AMQPError, AMQPTimeoutError, ProtocolError
from .link import Link, LinkRole
from .logging_utils import get_logger
from .wire import (
    Attach,
    Begin,
    Detach,
    Disposition,
    End,
    Error,
    Flow,
    Performative,
    Transfer,
)

if TYPE_CHECKING:
    from .connection import Connection

#: Default transfer-id window advertised in ``begin``/``flow``. Large enough
#: that this client never throttles on session-level flow control.
DEFAULT_WINDOW = 0x7FFFFFFF

#: Highest link handle the spec allows.
MAX_HANDLE = 0xFFFFFFFF

DEFAULT_BEGIN_TIMEOUT_SECONDS = 5.0
DEFAULT_END_TIMEOUT_SECONDS = 5.0

_logger = get_logger("session")


class Session:
    """One AMQP 1.0 session, living on a single connection channel.

    Example:
        >>> session = Session()
        >>> session.begin(connection)
        >>> session.end()
    """

    def __init__(
        self,
        *,
        incoming_window: int = DEFAULT_WINDOW,
        outgoing_window: int = DEFAULT_WINDOW,
        begin_timeout: float = DEFAULT_BEGIN_TIMEOUT_SECONDS,
        end_timeout: float = DEFAULT_END_TIMEOUT_SECONDS,
    ) -> None:
        """Create an unopened session.

        Args:
            incoming_window: ``begin.incoming-window`` this endpoint advertises.
            outgoing_window: ``begin.outgoing-window`` this endpoint advertises.
            begin_timeout: Seconds to wait for the peer's ``begin``.
            end_timeout: Seconds to wait for the peer's ``end``.
        """
        self._logger = _logger
        self._incoming_window = incoming_window
        self._outgoing_window = outgoing_window
        self._begin_timeout = begin_timeout
        self._end_timeout = end_timeout

        self._cond = threading.Condition()
        self._outgoing_lock = threading.RLock()

        self._connection: Connection | None = None
        self._channel: int | None = None
        self._remote_begin: Begin | None = None
        self._remote_end: End | None = None
        self._failure: BaseException | None = None
        self._ended = False

        self._next_outgoing_id = 0
        self._next_incoming_id: int | None = None
        self._received_transfer = False
        self._remote_incoming_window = 0
        self._remote_outgoing_window = 0
        self._incoming_used = 0
        self._handle_max = MAX_HANDLE

        self._links: dict[int, Link] = {}
        self._links_by_name: dict[str, Link] = {}
        self._links_by_remote_handle: dict[int, Link] = {}

    # --- public surface -------------------------------------------------

    @property
    def channel(self) -> int | None:
        """The connection channel this session occupies, or ``None`` before ``begin``."""
        return self._channel

    @property
    def connection(self) -> Connection:
        """The connection this session belongs to.

        Raises:
            ProtocolError: If the session has not begun yet.
        """
        if self._connection is None:
            raise ProtocolError("the session has not begun yet")
        return self._connection

    @property
    def is_open(self) -> bool:
        """Whether ``begin`` completed and the session has not ended."""
        return self._remote_begin is not None and not self._ended and self._failure is None

    @property
    def handle_max(self) -> int:
        """Effective ``handle-max``: the minimum of both endpoints' values."""
        return self._handle_max

    @property
    def next_outgoing_id(self) -> int:
        """The transfer-id the next outgoing delivery will use."""
        return self._next_outgoing_id

    @property
    def remote_begin(self) -> Begin | None:
        """The peer's ``begin`` performative, available once the session is open."""
        return self._remote_begin

    def begin(self, connection: Connection) -> None:
        """Allocate a channel, send ``begin`` and wait for the peer's ``begin``.

        Args:
            connection: Connection to open the session on.

        Raises:
            ProtocolError: If this session already begun, or the connection is
                not open.
            AMQPTimeoutError: If the peer does not reply within ``begin_timeout``.
        """
        with self._cond:
            if self._channel is not None:
                raise ProtocolError("this session has already begun")
        channel = connection.allocate_channel(self)
        with self._cond:
            self._connection = connection
            self._channel = channel
        begin = Begin(
            next_outgoing_id=self._next_outgoing_id,
            incoming_window=self._incoming_window,
            outgoing_window=self._outgoing_window,
        )
        try:
            connection.send_frame(channel, begin)
            self._wait_for(
                lambda: self._remote_begin is not None,
                self._begin_timeout,
                f"the broker's begin on channel {channel}",
            )
        except BaseException:
            connection.release_channel(channel)
            with self._cond:
                self._channel = None
            raise
        self._logger.debug("session begun on channel %d", channel)

    def end(self, error: Error | None = None) -> None:
        """Send ``end``, wait briefly for the peer's ``end``, and unregister.

        Idempotent: ending an already-ended or never-begun session is a no-op. A
        missing reply is logged, not raised, so teardown always completes.

        Args:
            error: Optional ``error`` to put on the outgoing ``end``.
        """
        with self._cond:
            if self._ended or self._channel is None or self._connection is None:
                return
            self._ended = True
            connection, channel = self._connection, self._channel
        self._detach_links_locally(ProtocolError("the session was ended"))
        try:
            connection.send_frame(channel, End(error=error))
            self._wait_for(lambda: self._remote_end is not None, self._end_timeout, "the broker's end")
        except Exception as failure:
            self._logger.debug("ignoring error while ending session on channel %d: %s", channel, failure)
        finally:
            connection.release_channel(channel)
        self._logger.debug("session on channel %d ended", channel)

    def send_frame(self, performative: Performative, payload: bytes = b"") -> None:
        """Send one performative on this session's channel.

        Args:
            performative: Performative to send.
            payload: Raw bytes appended after the performative.

        Raises:
            ProtocolError: If the session is not open, or the socket fails.
        """
        connection, channel = self._require_open()
        connection.send_frame(channel, performative, payload)

    def allocate_handle(self, link: Link) -> int:
        """Reserve the lowest free handle for ``link`` and register it by name.

        Args:
            link: Link being attached.

        Returns:
            The handle reserved.

        Raises:
            AMQPError: If every handle up to ``handle-max`` is in use.
        """
        with self._cond:
            candidate = 0
            while candidate in self._links:
                candidate += 1
            if candidate > self._handle_max:
                raise AMQPError(f"every link handle up to handle-max {self._handle_max} is already in use")
            self._links[candidate] = link
            self._links_by_name[link.name] = link
            return candidate

    def unregister_link(self, link: Link) -> None:
        """Forget ``link``, freeing its handle for reuse."""
        with self._cond:
            for handle, registered in list(self._links.items()):
                if registered is link:
                    del self._links[handle]
            if self._links_by_name.get(link.name) is link:
                del self._links_by_name[link.name]
            for remote_handle, registered in list(self._links_by_remote_handle.items()):
                if registered is link:
                    del self._links_by_remote_handle[remote_handle]

    def send_flow(
        self,
        *,
        handle: int | None = None,
        delivery_count: int | None = None,
        link_credit: int | None = None,
        drain: bool = False,
        echo: bool = False,
    ) -> None:
        """Send a ``flow``, filling in the session-level fields.

        Args:
            handle: Link the flow refers to; ``None`` for a session-only flow.
            delivery_count: Sender's delivery-count, required with ``handle``.
            link_credit: Credit granted to the sender, required with ``handle``.
            drain: Ask the sender to use up its credit and stop.
            echo: Ask the peer to reply with its own ``flow``.

        Raises:
            ProtocolError: If the session is not open.
        """
        connection, channel = self._require_open()
        with self._cond:
            flow = Flow(
                incoming_window=self._incoming_window,
                next_outgoing_id=self._next_outgoing_id,
                outgoing_window=self._outgoing_window,
                next_incoming_id=self._next_incoming_id,
                handle=handle,
                delivery_count=delivery_count,
                link_credit=link_credit,
                drain=drain,
                echo=echo,
            )
            self._incoming_used = 0
        connection.send_frame(channel, flow)

    def send_delivery(
        self,
        *,
        handle: int,
        delivery_tag: bytes,
        payload: bytes,
        settled: bool,
        max_fragment: int,
        on_delivery_id: Callable[[int], None] | None = None,
    ) -> int:
        """Write one delivery as one or more ``transfer`` frames.

        The whole delivery is written while holding the session's outgoing lock,
        so concurrent senders can neither interleave the fragments of a
        multi-frame delivery nor assign transfer-ids out of order.

        Args:
            handle: Link handle to send on.
            delivery_tag: Link-scoped delivery tag, on the first frame only.
            payload: Encoded message bytes.
            settled: Whether the sender considers the delivery already settled.
            max_fragment: Largest payload slice one frame may carry.
            on_delivery_id: Called with the assigned delivery-id before the
                first frame is written, so a caller can register a disposition
                waiter that cannot miss an early reply.

        Returns:
            The delivery-id assigned to this delivery.

        Raises:
            ProtocolError: If the session is not open, or the socket fails.
        """
        connection, channel = self._require_open()
        fragments = _split(payload, max_fragment)
        with self._outgoing_lock:
            delivery_id = self._next_outgoing_id
            if on_delivery_id is not None:
                on_delivery_id(delivery_id)
            for index, fragment in enumerate(fragments):
                more = index < len(fragments) - 1
                if index == 0:
                    transfer = Transfer(
                        handle=handle,
                        delivery_id=delivery_id,
                        delivery_tag=delivery_tag,
                        settled=settled,
                        more=more,
                    )
                else:
                    transfer = Transfer(handle=handle, more=more)
                connection.send_frame(channel, transfer, fragment)
            self._next_outgoing_id += 1
        return delivery_id

    # --- inbound dispatch ----------------------------------------------

    def handle_frame(self, performative: Performative, payload: bytes) -> None:
        """Dispatch one inbound performative addressed to this session's channel.

        Args:
            performative: The decoded performative.
            payload: Raw payload bytes, non-empty only for ``transfer``.
        """
        if isinstance(performative, Begin):
            self._on_begin(performative)
        elif isinstance(performative, End):
            self._on_end(performative)
        elif isinstance(performative, Flow):
            self._on_flow(performative)
        elif isinstance(performative, Transfer):
            self._on_transfer(performative, payload)
        elif isinstance(performative, Disposition):
            self._on_disposition(performative)
        elif isinstance(performative, Attach):
            self._on_attach(performative)
        elif isinstance(performative, Detach):
            self._on_detach(performative)
        else:
            self._logger.warning("dropping unexpected %s on channel %s", type(performative).__name__, self._channel)

    def transport_lost(self, error: BaseException) -> None:
        """Fail this session and every link on it because the connection died.

        Args:
            error: What killed the connection.
        """
        with self._cond:
            if self._failure is None:
                self._failure = error
            links = list(self._links.values())
            self._cond.notify_all()
        for link in links:
            link.transport_lost(error)

    # --- inbound handlers ----------------------------------------------

    def _on_begin(self, performative: Begin) -> None:
        with self._cond:
            self._remote_begin = performative
            self._next_incoming_id = performative.next_outgoing_id
            self._remote_incoming_window = performative.incoming_window
            self._remote_outgoing_window = performative.outgoing_window
            self._handle_max = min(self._handle_max, performative.handle_max)
            self._cond.notify_all()

    def _on_end(self, performative: End) -> None:
        with self._cond:
            self._remote_end = performative
            locally_initiated = self._ended
            self._ended = True
            connection, channel = self._connection, self._channel
            self._cond.notify_all()
        if locally_initiated:
            return
        failure = _error_failure(performative.error, "the broker ended the session")
        self._logger.warning("session on channel %s ended by the broker: %s", channel, failure)
        with self._cond:
            if self._failure is None:
                self._failure = failure
            self._cond.notify_all()
        self._detach_links_locally(failure)
        if connection is None or channel is None:
            return
        try:
            connection.send_frame(channel, End())
        except AMQPError as error:
            self._logger.debug("could not echo the broker's end: %s", error)
        connection.release_channel(channel)

    def _on_flow(self, performative: Flow) -> None:
        with self._cond:
            self._remote_incoming_window = performative.incoming_window
            self._remote_outgoing_window = performative.outgoing_window
            if not self._received_transfer:
                # Until a transfer arrives, the peer's own next-outgoing-id is
                # the only thing that tells us which id to expect next.
                self._next_incoming_id = performative.next_outgoing_id
        if performative.handle is not None:
            link = self._link_for_handle(performative.handle)
            if link is None:
                self._logger.warning("dropping flow for unknown link handle %d", performative.handle)
                return
            link.handle_frame(performative, b"")
            return
        if performative.echo:
            self.send_flow()

    def _on_transfer(self, performative: Transfer, payload: bytes) -> None:
        with self._cond:
            if performative.delivery_id is not None:
                self._next_incoming_id = performative.delivery_id + 1
                self._received_transfer = True
            self._incoming_used += 1
            replenish = self._incoming_used >= max(1, self._incoming_window // 2)
        link = self._link_for_handle(performative.handle)
        if link is None:
            self._logger.warning("dropping transfer for unknown link handle %d", performative.handle)
        else:
            link.handle_frame(performative, payload)
        if replenish:
            self._replenish_incoming_window()

    def _on_disposition(self, performative: Disposition) -> None:
        target_role = LinkRole.SENDER if performative.role else LinkRole.RECEIVER
        with self._cond:
            links = [link for link in self._links.values() if link.role is target_role]
        for link in links:
            link.handle_frame(performative, b"")

    def _on_attach(self, performative: Attach) -> None:
        with self._cond:
            link = self._links_by_name.get(performative.name)
            if link is not None:
                self._links_by_remote_handle[performative.handle] = link
        if link is None:
            self._logger.warning("dropping attach for unknown link %r", performative.name)
            return
        link.handle_frame(performative, b"")

    def _on_detach(self, performative: Detach) -> None:
        link = self._link_for_handle(performative.handle)
        if link is None:
            self._logger.warning("dropping detach for unknown link handle %d", performative.handle)
            return
        link.handle_frame(performative, b"")

    # --- helpers --------------------------------------------------------

    def _link_for_handle(self, handle: int) -> Link | None:
        """Resolve a handle the peer used, falling back to our own numbering."""
        with self._cond:
            link = self._links_by_remote_handle.get(handle)
            if link is not None:
                return link
            return self._links.get(handle)

    def _replenish_incoming_window(self) -> None:
        """Re-advertise the incoming window once half of it has been consumed."""
        try:
            self.send_flow()
        except AMQPError as error:
            self._logger.debug("could not replenish the session window: %s", error)

    def _detach_links_locally(self, error: BaseException) -> None:
        """Mark every link on this session detached, without any ``detach`` frame."""
        with self._cond:
            links = list(self._links.values())
            self._links.clear()
            self._links_by_name.clear()
            self._links_by_remote_handle.clear()
        for link in links:
            link.session_ended(error)

    def _require_open(self) -> tuple[Connection, int]:
        """Return the connection and channel, refusing a session that is not open."""
        with self._cond:
            if self._failure is not None:
                raise ProtocolError(f"the session is no longer usable: {self._failure}")
            if self._connection is None or self._channel is None or self._ended:
                raise ProtocolError("the session is not open")
            return self._connection, self._channel

    def _wait_for(self, done: Callable[[], bool], timeout: float, description: str) -> None:
        """Block until ``done()``, the session fails, or ``timeout`` elapses.

        Raises:
            AMQPTimeoutError: If ``timeout`` elapses first.
            AMQPError: Whatever failure killed the session or its connection.
        """
        with self._cond:
            completed = self._cond.wait_for(lambda: done() or self._failure is not None, timeout)
            failure = self._failure
        if failure is not None:
            raise failure
        if not completed:
            raise AMQPTimeoutError(f"timed out after {timeout:g}s waiting for {description}")


def _split(payload: bytes, max_fragment: int) -> list[bytes]:
    """Slice ``payload`` into at most ``max_fragment``-sized pieces, never empty."""
    step = max(1, max_fragment)
    if len(payload) <= step:
        return [payload]
    return [payload[start : start + step] for start in range(0, len(payload), step)]


def _error_failure(error: Error | None, prefix: str) -> BaseException:
    """Turn a wire ``error`` into an exception describing it."""
    if error is None:
        return ProtocolError(prefix)
    detail = f"{prefix}: {error.condition}"
    if error.description:
        detail = f"{detail}: {error.description}"
    return ProtocolError(detail)


__all__ = [
    "DEFAULT_WINDOW",
    "MAX_HANDLE",
    "Session",
]
