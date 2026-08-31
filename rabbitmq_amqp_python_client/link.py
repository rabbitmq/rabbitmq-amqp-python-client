"""AMQP 1.0 links: ``attach``/``detach``, credit-based flow, transfers and settlement.

Three classes live here:

* :class:`Link` — the shared ``attach``/``detach`` handshake and frame routing.
* :class:`SenderLink` — credit accounting plus ``transfer``, with a
  :class:`PendingDelivery` handle a caller blocks on for the ``disposition``.
* :class:`ReceiverLink` — ``flow`` credit grants, multi-frame delivery
  reassembly into a queue, and per-delivery settlement.

Inbound frames arrive on the connection's frame-reader thread. Handlers here
only mutate state, notify conditions, and put items on queues; they never block
on a caller thread. The one exception is the optional ``flow``-properties hook,
which is documented as having to return promptly.
"""

from __future__ import annotations

import queue
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple

from .exceptions import AMQPError, AMQPTimeoutError, ProtocolError
from .logging_utils import get_logger
from .wire import (
    FRAME_HEADER_SIZE,
    Accepted,
    Attach,
    DeliveryState,
    Detach,
    Disposition,
    Error,
    Flow,
    Message,
    Performative,
    Source,
    Target,
    Transfer,
)

if TYPE_CHECKING:
    from .session import Session

DEFAULT_ATTACH_TIMEOUT_SECONDS = 5.0
DEFAULT_DETACH_TIMEOUT_SECONDS = 5.0
DEFAULT_CREDIT_TIMEOUT_SECONDS = 30.0

#: How long a refused ``attach`` waits for the ``detach`` that carries the reason.
REFUSAL_DETACH_TIMEOUT_SECONDS = 1.0

#: Slice a blocking :meth:`ReceiverLink.receive` waits before re-checking for failure.
RECEIVE_POLL_INTERVAL_SECONDS = 0.1

#: How many unobserved inbound ``flow.properties`` maps a receiver keeps.
FLOW_PROPERTIES_BUFFER = 32

#: Highest value a ``delivery-id`` can take, used to size a ``transfer`` header.
MAX_DELIVERY_ID = 0xFFFFFFFF

LINK_NAME_PREFIX = "link"

_logger = get_logger("link")


class LinkRole(Enum):
    """Direction of a link, matching ``attach.role`` on the wire."""

    SENDER = False
    RECEIVER = True


class Delivery(NamedTuple):
    """One fully reassembled inbound delivery.

    Attributes:
        delivery_id: Session-scoped delivery-id from the first ``transfer``.
        message: The decoded message.
        settled: Whether the sender already settled it, so no ``disposition`` is
            expected from us.
    """

    delivery_id: int
    message: Message
    settled: bool


@dataclass(frozen=True)
class LinkRefusal:
    """Why the peer refused a link.

    Attributes:
        link_name: Name of the refused link.
        remote_attach: The peer's ``attach``, or ``None`` when it answered with
            only a ``detach``.
        error: The ``error`` from the peer's ``detach``, when it sent one.
    """

    link_name: str
    remote_attach: Attach | None
    error: Error | None

    def describe(self) -> str:
        """Return a one-line human-readable reason."""
        if self.error is None:
            return f"the broker refused link {self.link_name!r} without giving a reason"
        detail = f"the broker refused link {self.link_name!r}: {self.error.condition}"
        if self.error.description:
            detail = f"{detail}: {self.error.description}"
        return detail


#: Builds the exception :meth:`Link.attach` raises when the peer refuses a link,
#: so callers can surface their own domain error (``PublisherError``,
#: ``ConsumerError``, ...) without this layer knowing about them.
RefusalErrorFactory = Callable[[LinkRefusal], Exception]


@dataclass
class PendingDelivery:
    """A handle that resolves with the outcome of one outgoing delivery.

    Register one with :meth:`SenderLink.register_pending` *before* sending, then
    block on :meth:`wait` for the ``disposition`` the broker sends back.

    Attributes:
        delivery_tag: The tag the delivery was sent with.
        delivery_id: The transfer-id assigned when the delivery was written.
        state: The outcome the broker reported, once resolved.
        settled: Whether the broker settled the delivery.
    """

    delivery_tag: bytes
    delivery_id: int | None = None
    state: DeliveryState | None = None
    settled: bool = False
    _resolved: threading.Event = field(default_factory=threading.Event, repr=False)
    _failure: BaseException | None = field(default=None, repr=False)

    @property
    def is_resolved(self) -> bool:
        """Whether an outcome (or a failure) is already available."""
        return self._resolved.is_set()

    def resolve(self, state: DeliveryState | None, settled: bool) -> None:
        """Record the reported outcome and wake every waiter."""
        self.state = state
        self.settled = settled
        self._resolved.set()

    def fail(self, error: BaseException) -> None:
        """Record that no outcome will ever arrive and wake every waiter."""
        self._failure = error
        self._resolved.set()

    def wait(self, timeout: float | None = None) -> DeliveryState:
        """Block until the broker reports this delivery's outcome.

        Args:
            timeout: Seconds to wait; ``None`` waits indefinitely.

        Returns:
            The delivery state the broker reported.

        Raises:
            AMQPTimeoutError: If ``timeout`` elapses first.
            ProtocolError: If the ``disposition`` carried no delivery state.
            AMQPError: Whatever failure made the outcome unreachable.
        """
        if not self._resolved.wait(timeout):
            raise AMQPTimeoutError(f"no disposition for delivery {self.delivery_tag!r} within {timeout:g}s")
        if self._failure is not None:
            raise self._failure
        if self.state is None:
            raise ProtocolError(f"the disposition for delivery {self.delivery_tag!r} carried no delivery state")
        return self.state


class Link:
    """Base link: the ``attach``/``detach`` handshake and inbound frame routing.

    Subclasses add the role-specific behaviour; ``Link`` itself is not attached
    to any role and is not meant to be instantiated directly.
    """

    ROLE: ClassVar[LinkRole]

    def __init__(
        self,
        name: str | None = None,
        *,
        attach_timeout: float = DEFAULT_ATTACH_TIMEOUT_SECONDS,
        detach_timeout: float = DEFAULT_DETACH_TIMEOUT_SECONDS,
    ) -> None:
        """Create an unattached link.

        Args:
            name: Link name shared with the peer's ``attach``; generated when omitted.
            attach_timeout: Seconds to wait for the peer's ``attach``.
            detach_timeout: Seconds to wait for the peer's ``detach``.
        """
        self.name = name if name else f"{LINK_NAME_PREFIX}-{uuid.uuid4().hex}"
        self._logger = _logger
        self._attach_timeout = attach_timeout
        self._detach_timeout = detach_timeout
        self._cond = threading.Condition()
        self._session: Session | None = None
        self._handle: int | None = None
        self._remote_handle: int | None = None
        self._remote_attach: Attach | None = None
        self._remote_detach: Detach | None = None
        self._attached = False
        self._detached = False
        self._refused = False
        self._failure: BaseException | None = None

    # --- public surface -------------------------------------------------

    @property
    def role(self) -> LinkRole:
        """This link's direction."""
        return self.ROLE

    @property
    def handle(self) -> int:
        """The session-local handle this link was attached with.

        Raises:
            ProtocolError: If the link has not been attached.
        """
        if self._handle is None:
            raise ProtocolError(f"link {self.name!r} has not been attached")
        return self._handle

    @property
    def is_attached(self) -> bool:
        """Whether the ``attach`` handshake completed and no ``detach`` followed."""
        return self._attached and not self._detached and self._failure is None

    @property
    def refused(self) -> bool:
        """Whether the peer refused this link's ``attach``."""
        return self._refused

    @property
    def remote_attach(self) -> Attach | None:
        """The peer's ``attach``, available once it replied."""
        return self._remote_attach

    @property
    def session(self) -> Session:
        """The session this link is attached to.

        Raises:
            ProtocolError: If the link has not been attached.
        """
        if self._session is None:
            raise ProtocolError(f"link {self.name!r} has not been attached")
        return self._session

    def attach(
        self,
        session: Session,
        source: Source | None = None,
        target: Target | None = None,
        *,
        on_refused: RefusalErrorFactory | None = None,
        **attach_fields: Any,
    ) -> None:
        """Send ``attach`` and wait for the peer's reply.

        A peer that refuses the link answers with an ``attach`` whose relevant
        terminus is ``null`` (``target`` for a sender, ``source`` for a
        receiver), usually followed by a ``detach`` carrying the reason. Both
        shapes — and a bare ``detach`` — are treated as a refusal: the link is
        torn down and the exception ``on_refused`` builds is raised, so a caller
        can surface ``PublisherError``/``ConsumerError`` instead of the default
        :class:`~.exceptions.ProtocolError`.

        Args:
            session: Open session to attach on.
            source: ``attach.source`` terminus.
            target: ``attach.target`` terminus.
            on_refused: Builds the exception to raise when the peer refuses.
            **attach_fields: Extra ``attach`` fields, e.g. ``snd_settle_mode``,
                ``rcv_settle_mode``, ``properties``, ``desired_capabilities``.

        Raises:
            ProtocolError: If the link is already attached, or the peer refused
                it and no ``on_refused`` factory was given.
            AMQPTimeoutError: If the peer does not reply within ``attach_timeout``.
        """
        with self._cond:
            if self._handle is not None:
                raise ProtocolError(f"link {self.name!r} has already been attached")
        handle = session.allocate_handle(self)
        with self._cond:
            self._session = session
            self._handle = handle
        performative = Attach(
            name=self.name,
            handle=handle,
            role=self.ROLE.value,
            source=source,
            target=target,
            initial_delivery_count=self._initial_delivery_count(),
            **attach_fields,
        )
        try:
            session.send_frame(performative)
            self._wait_for(
                lambda: self._remote_attach is not None or self._remote_detach is not None,
                self._attach_timeout,
                f"the broker's attach for link {self.name!r}",
            )
        except BaseException:
            session.unregister_link(self)
            raise
        if self._is_refusal():
            raise self._refusal_error(on_refused)
        with self._cond:
            self._attached = True
        self._on_attached()
        self._logger.debug("link %r attached with handle %d", self.name, handle)

    def detach(self, error: Error | None = None) -> None:
        """Send ``detach``, wait briefly for the peer's, and unregister.

        Idempotent, and never raises for a missing or late reply — teardown
        always completes.

        Args:
            error: Optional ``error`` to put on the outgoing ``detach``.
        """
        with self._cond:
            if self._detached or self._session is None or self._handle is None:
                return
            self._detached = True
            session, handle = self._session, self._handle
        try:
            session.send_frame(Detach(handle=handle, closed=True, error=error))
            self._wait_for(
                lambda: self._remote_detach is not None,
                self._detach_timeout,
                f"the broker's detach for link {self.name!r}",
            )
        except Exception as failure:  # teardown must not fail on a missing reply
            self._logger.debug("ignoring error while detaching link %r: %s", self.name, failure)
        finally:
            session.unregister_link(self)
        self._logger.debug("link %r detached", self.name)

    # --- inbound dispatch ----------------------------------------------

    def handle_frame(self, performative: Performative, payload: bytes) -> None:
        """Dispatch one inbound performative addressed to this link.

        Args:
            performative: The decoded performative.
            payload: Raw payload bytes, non-empty only for ``transfer``.
        """
        if isinstance(performative, Attach):
            self._on_remote_attach(performative)
        elif isinstance(performative, Detach):
            self._on_remote_detach(performative)
        elif isinstance(performative, Flow):
            self._on_flow(performative)
        elif isinstance(performative, Transfer):
            self._on_transfer(performative, payload)
        elif isinstance(performative, Disposition):
            self._on_disposition(performative)
        else:
            self._logger.warning("dropping unexpected %s on link %r", type(performative).__name__, self.name)

    def transport_lost(self, error: BaseException) -> None:
        """Fail this link because the connection died.

        Args:
            error: What killed the connection.
        """
        self._fail(error)

    def session_ended(self, error: BaseException) -> None:
        """Fail this link because its session ended, which detaches it implicitly.

        Args:
            error: Why the session ended.
        """
        with self._cond:
            self._detached = True
        self._fail(error)

    # --- hooks for subclasses -------------------------------------------

    def _initial_delivery_count(self) -> int | None:
        """``attach.initial-delivery-count``; mandatory for a sender, unused otherwise."""
        return None

    def _on_attached(self) -> None:
        """Called once the ``attach`` handshake completed successfully."""

    def _on_flow(self, performative: Flow) -> None:
        """Handle an inbound link ``flow``."""
        self._logger.debug("ignoring flow on link %r", self.name)

    def _on_transfer(self, performative: Transfer, payload: bytes) -> None:
        """Handle an inbound ``transfer``."""
        self._logger.warning("dropping transfer on link %r, which cannot receive", self.name)

    def _on_disposition(self, performative: Disposition) -> None:
        """Handle an inbound ``disposition``."""
        self._logger.debug("ignoring disposition on link %r", self.name)

    # --- internals ------------------------------------------------------

    def _on_remote_attach(self, performative: Attach) -> None:
        with self._cond:
            self._remote_attach = performative
            self._remote_handle = performative.handle
            self._cond.notify_all()

    def _on_remote_detach(self, performative: Detach) -> None:
        with self._cond:
            self._remote_detach = performative
            locally_initiated = self._detached
            self._detached = True
            session = self._session
            handle = self._handle
            self._cond.notify_all()
        if locally_initiated or not self._attached:
            return
        failure = _detach_failure(self.name, performative.error)
        self._fail(failure)
        self._logger.warning("link %r detached by the broker: %s", self.name, failure)
        if session is None or handle is None:
            return
        try:
            session.send_frame(Detach(handle=handle, closed=True))
        except AMQPError as error:
            self._logger.debug("could not echo the broker's detach: %s", error)
        session.unregister_link(self)

    def _is_refusal(self) -> bool:
        """Whether the peer's reply means it refused this link."""
        with self._cond:
            attach = self._remote_attach
            if attach is None:
                self._refused = True
                return True
            refused = attach.target is None if self.ROLE is LinkRole.SENDER else attach.source is None
            self._refused = refused
            return refused

    def _refusal_error(self, on_refused: RefusalErrorFactory | None) -> Exception:
        """Tear the refused link down and build the exception to raise for it."""
        with self._cond:
            self._cond.wait_for(lambda: self._remote_detach is not None, REFUSAL_DETACH_TIMEOUT_SECONDS)
            detach = self._remote_detach
            attach = self._remote_attach
            session, handle = self._session, self._handle
            self._detached = True
        if session is not None and handle is not None:
            try:
                if detach is None:
                    session.send_frame(Detach(handle=handle, closed=True))
            except AMQPError as error:
                self._logger.debug("could not detach refused link %r: %s", self.name, error)
            session.unregister_link(self)
        refusal = LinkRefusal(link_name=self.name, remote_attach=attach, error=detach.error if detach else None)
        if on_refused is None:
            return ProtocolError(refusal.describe())
        return on_refused(refusal)

    def _fail(self, error: BaseException) -> None:
        """Record a terminal failure and wake every waiter."""
        with self._cond:
            if self._failure is None:
                self._failure = error
            self._cond.notify_all()

    def _require_attached(self) -> Session:
        """Return the session, refusing a link that is not usable.

        Raises:
            ProtocolError: If the link is not attached.
            AMQPError: Whatever failure killed the link.
        """
        with self._cond:
            if self._failure is not None:
                raise self._failure
            if not self._attached or self._detached or self._session is None:
                raise ProtocolError(f"link {self.name!r} is not attached")
            return self._session

    def _wait_for(self, done: Callable[[], bool], timeout: float, description: str) -> None:
        """Block until ``done()``, the link fails, or ``timeout`` elapses.

        Raises:
            AMQPTimeoutError: If ``timeout`` elapses first.
            AMQPError: Whatever failure killed the link, its session or the connection.
        """
        with self._cond:
            completed = self._cond.wait_for(lambda: done() or self._failure is not None, timeout)
            failure = self._failure
        if failure is not None:
            raise failure
        if not completed:
            raise AMQPTimeoutError(f"timed out after {timeout:g}s waiting for {description}")


class SenderLink(Link):
    """A link this endpoint sends messages on.

    Example:
        >>> sender = SenderLink()
        >>> sender.attach(session, target=Target(address="/queues/q"))
        >>> pending = sender.register_pending(b"tag-1")
        >>> sender.send_transfer(b"tag-1", Message("hello"))
        >>> outcome = pending.wait(timeout=10)
    """

    ROLE = LinkRole.SENDER

    def __init__(
        self,
        name: str | None = None,
        *,
        credit_timeout: float = DEFAULT_CREDIT_TIMEOUT_SECONDS,
        attach_timeout: float = DEFAULT_ATTACH_TIMEOUT_SECONDS,
        detach_timeout: float = DEFAULT_DETACH_TIMEOUT_SECONDS,
    ) -> None:
        """Create an unattached sender link.

        Args:
            name: Link name; generated when omitted.
            credit_timeout: Seconds :meth:`send_transfer` waits for link credit.
            attach_timeout: Seconds to wait for the peer's ``attach``.
            detach_timeout: Seconds to wait for the peer's ``detach``.
        """
        super().__init__(name, attach_timeout=attach_timeout, detach_timeout=detach_timeout)
        self._credit_timeout = credit_timeout
        self._link_credit = 0
        self._delivery_count = 0
        self._pending_by_tag: dict[bytes, PendingDelivery] = {}
        self._pending_by_id: dict[int, PendingDelivery] = {}

    @property
    def link_credit(self) -> int:
        """Credit currently granted by the receiver."""
        return self._link_credit

    @property
    def delivery_count(self) -> int:
        """Number of deliveries sent on this link."""
        return self._delivery_count

    def register_pending(self, delivery_tag: bytes) -> PendingDelivery:
        """Register a waiter for the ``disposition`` of an upcoming delivery.

        Call this before :meth:`send_transfer` so the waiter cannot miss a
        ``disposition`` that arrives while the sender is still returning.

        Args:
            delivery_tag: Tag the delivery will be sent with.

        Returns:
            The handle to block on; see :meth:`PendingDelivery.wait`.
        """
        pending = PendingDelivery(delivery_tag=delivery_tag)
        with self._cond:
            self._pending_by_tag[delivery_tag] = pending
        return pending

    def cancel_pending(self, pending: PendingDelivery) -> None:
        """Forget a registered waiter, so a late ``disposition`` resolves nothing.

        Called by a caller that gave up on the outcome, e.g. after its own
        timeout elapsed or the transfer itself failed.

        Args:
            pending: The waiter returned by :meth:`register_pending`.
        """
        with self._cond:
            if self._pending_by_tag.get(pending.delivery_tag) is pending:
                del self._pending_by_tag[pending.delivery_tag]
            delivery_id = pending.delivery_id
            if delivery_id is not None and self._pending_by_id.get(delivery_id) is pending:
                del self._pending_by_id[delivery_id]

    def send_transfer(
        self,
        delivery_tag: bytes,
        message: Message,
        settled: bool = False,
        timeout: float | None = None,
    ) -> int:
        """Block for link credit, then send ``message`` as one delivery.

        The encoded message is split across as many ``transfer`` frames as the
        connection's negotiated ``max-frame-size`` requires.

        Args:
            delivery_tag: Link-scoped delivery tag.
            message: Message to encode and send.
            settled: Send presettled, so the broker reports no ``disposition``.
                Any registered :class:`PendingDelivery` resolves immediately as
                ``accepted``, since no outcome will ever arrive.
            timeout: Seconds to wait for credit; defaults to ``credit_timeout``.

        Returns:
            The delivery-id assigned to this delivery.

        Raises:
            ProtocolError: If the link is not attached, or the socket fails.
            AMQPTimeoutError: If no credit is granted in time.
        """
        session = self._require_attached()
        payload = message.encode()
        self._await_credit(timeout if timeout is not None else self._credit_timeout)
        max_fragment = self._max_fragment(session, delivery_tag, settled)
        delivery_id = session.send_delivery(
            handle=self.handle,
            delivery_tag=delivery_tag,
            payload=payload,
            settled=settled,
            max_fragment=max_fragment,
            on_delivery_id=self._bind_delivery_id(delivery_tag),
        )
        with self._cond:
            self._link_credit = max(0, self._link_credit - 1)
            self._delivery_count += 1
            pending = self._pending_by_tag.get(delivery_tag) if settled else None
            if pending is not None:
                self._pending_by_tag.pop(delivery_tag, None)
                self._pending_by_id.pop(delivery_id, None)
        if pending is not None:
            pending.resolve(Accepted(), True)
        return delivery_id

    def transport_lost(self, error: BaseException) -> None:
        """Fail the link and every unresolved delivery on it."""
        super().transport_lost(error)
        self._fail_pending(error)

    def session_ended(self, error: BaseException) -> None:
        """Fail the link and every unresolved delivery on it."""
        super().session_ended(error)
        self._fail_pending(error)

    def _initial_delivery_count(self) -> int | None:
        return 0

    def _on_flow(self, performative: Flow) -> None:
        """Recompute available credit from the receiver's view of the link."""
        with self._cond:
            if performative.link_credit is None:
                return
            if performative.delivery_count is not None:
                credit = performative.delivery_count + performative.link_credit - self._delivery_count
            else:
                credit = performative.link_credit
            self._link_credit = max(0, credit)
            self._cond.notify_all()

    def _on_disposition(self, performative: Disposition) -> None:
        """Resolve every pending delivery inside the reported id range."""
        first = performative.first
        last = performative.last if performative.last is not None else performative.first
        with self._cond:
            matched = [pending for delivery_id, pending in self._pending_by_id.items() if first <= delivery_id <= last]
            for pending in matched:
                if pending.delivery_id is not None:
                    self._pending_by_id.pop(pending.delivery_id, None)
                self._pending_by_tag.pop(pending.delivery_tag, None)
        for pending in matched:
            pending.resolve(performative.state, performative.settled)

    def _await_credit(self, timeout: float) -> None:
        """Block until the receiver has granted at least one credit."""
        with self._cond:
            granted = self._cond.wait_for(lambda: self._link_credit > 0 or self._failure is not None, timeout)
            failure = self._failure
        if failure is not None:
            raise failure
        if not granted:
            raise AMQPTimeoutError(f"link {self.name!r} received no credit within {timeout:g}s")

    def _bind_delivery_id(self, delivery_tag: bytes) -> Callable[[int], None]:
        """Return a hook that indexes a registered waiter by its delivery-id."""

        def bind(delivery_id: int) -> None:
            with self._cond:
                pending = self._pending_by_tag.get(delivery_tag)
                if pending is None:
                    return
                pending.delivery_id = delivery_id
                self._pending_by_id[delivery_id] = pending

        return bind

    def _max_fragment(self, session: Session, delivery_tag: bytes, settled: bool) -> int:
        """Largest payload slice that fits in one frame alongside the performative."""
        probe = Transfer(
            handle=self.handle,
            delivery_id=MAX_DELIVERY_ID,
            delivery_tag=delivery_tag,
            settled=settled,
            more=True,
        )
        overhead = FRAME_HEADER_SIZE + len(probe.encode())
        return max(1, session.connection.max_frame_size - overhead)

    def _fail_pending(self, error: BaseException) -> None:
        """Fail every unresolved delivery waiter."""
        with self._cond:
            pending = list(self._pending_by_tag.values()) + list(self._pending_by_id.values())
            self._pending_by_tag.clear()
            self._pending_by_id.clear()
        for waiter in pending:
            waiter.fail(error)


class ReceiverLink(Link):
    """A link this endpoint receives messages on.

    Example:
        >>> receiver = ReceiverLink()
        >>> receiver.attach(session, source=Source(address="/queues/q"))
        >>> receiver.flow(10)
        >>> delivery = receiver.receive(timeout=5)
        >>> if delivery is not None:
        ...     receiver.settle(delivery.delivery_id, Accepted())
    """

    ROLE = LinkRole.RECEIVER

    def __init__(
        self,
        name: str | None = None,
        *,
        flow_properties_buffer: int = FLOW_PROPERTIES_BUFFER,
        attach_timeout: float = DEFAULT_ATTACH_TIMEOUT_SECONDS,
        detach_timeout: float = DEFAULT_DETACH_TIMEOUT_SECONDS,
    ) -> None:
        """Create an unattached receiver link.

        Args:
            name: Link name; generated when omitted.
            flow_properties_buffer: How many inbound ``flow.properties`` maps to
                keep while no handler is registered.
            attach_timeout: Seconds to wait for the peer's ``attach``.
            detach_timeout: Seconds to wait for the peer's ``detach``.
        """
        super().__init__(name, attach_timeout=attach_timeout, detach_timeout=detach_timeout)
        self._deliveries: queue.Queue[Delivery] = queue.Queue()
        self._credit = 0
        self._delivery_count = 0
        self._available = 0
        self._partial: bytearray | None = None
        self._partial_first: Transfer | None = None
        self._flow_properties: deque[dict[Any, Any]] = deque(maxlen=max(1, flow_properties_buffer))
        self._flow_handler: Callable[[dict[Any, Any]], None] | None = None

    @property
    def credit(self) -> int:
        """Credit currently granted to the sender and not yet consumed."""
        return self._credit

    @property
    def delivery_count(self) -> int:
        """Number of complete deliveries received on this link."""
        return self._delivery_count

    @property
    def available(self) -> int:
        """Messages the sender last reported as ready to send."""
        return self._available

    def flow(self, link_credit: int, drain: bool = False) -> None:
        """Grant ``link_credit`` to the sender.

        Args:
            link_credit: Total credit the sender may use from now on.
            drain: Ask the sender to consume all credit and then stop.

        Raises:
            ProtocolError: If the link is not attached, or the socket fails.
        """
        session = self._require_attached()
        with self._cond:
            self._credit = link_credit
            delivery_count = self._delivery_count
        session.send_flow(
            handle=self.handle,
            delivery_count=delivery_count,
            link_credit=link_credit,
            drain=drain,
        )

    def receive(self, timeout: float | None = None) -> Delivery | None:
        """Block until the next delivery is fully reassembled.

        Args:
            timeout: Seconds to wait; ``None`` waits until a delivery arrives or
                the link fails.

        Returns:
            The next delivery, or ``None`` when ``timeout`` elapsed first.

        Raises:
            AMQPError: Whatever failure killed the link, its session or the
                connection, once every already-received delivery is drained.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                return self._deliveries.get_nowait()
            except queue.Empty:
                pass
            with self._cond:
                failure = self._failure
            if failure is not None:
                raise failure
            remaining = RECEIVE_POLL_INTERVAL_SECONDS
            if deadline is not None:
                remaining = min(remaining, deadline - time.monotonic())
                if remaining <= 0:
                    return None
            try:
                return self._deliveries.get(timeout=remaining)
            except queue.Empty:
                continue

    def settle(self, delivery_id: int, state: DeliveryState) -> None:
        """Settle one delivery with ``state``.

        Args:
            delivery_id: Delivery-id reported by :meth:`receive`.
            state: Outcome to report, e.g. ``Accepted()`` or ``Rejected(...)``.

        Raises:
            ProtocolError: If the link is not attached, or the socket fails.
        """
        session = self._require_attached()
        session.send_frame(
            Disposition(
                role=LinkRole.RECEIVER.value,
                first=delivery_id,
                last=delivery_id,
                settled=True,
                state=state,
            )
        )

    def on_flow_properties(self, handler: Callable[[dict[Any, Any]], None] | None) -> None:
        """Observe the ``properties`` map of every ``flow`` seen on this link.

        Maps that arrived before a handler was registered are buffered (the last
        ``flow_properties_buffer`` of them) and replayed here, so a handler
        registered after ``attach`` still sees the broker's first ``flow``.

        The handler runs on the connection's frame-reader thread and must return
        promptly; exceptions it raises are logged and swallowed.

        Args:
            handler: Callable receiving each ``flow.properties`` map, or ``None``
                to stop observing and resume buffering.
        """
        with self._cond:
            self._flow_handler = handler
            buffered = list(self._flow_properties)
            if handler is not None:
                self._flow_properties.clear()
        if handler is None:
            return
        for properties in buffered:
            self._invoke_flow_handler(handler, properties)

    def _on_flow(self, performative: Flow) -> None:
        """Track the sender's view of the link and surface ``flow.properties``."""
        with self._cond:
            if performative.available is not None:
                self._available = performative.available
            if performative.link_credit is not None:
                self._credit = performative.link_credit
            properties = performative.properties
            handler = self._flow_handler
            if properties and handler is None:
                self._flow_properties.append(properties)
            self._cond.notify_all()
        if properties and handler is not None:
            self._invoke_flow_handler(handler, properties)

    def _on_transfer(self, performative: Transfer, payload: bytes) -> None:
        """Reassemble a possibly multi-frame delivery and queue it."""
        with self._cond:
            if performative.aborted:
                self._partial = None
                self._partial_first = None
                self._logger.debug("discarding aborted delivery on link %r", self.name)
                return
            if self._partial_first is None:
                self._partial_first = performative
                self._partial = bytearray()
            if self._partial is None:
                self._partial = bytearray()
            self._partial.extend(payload)
            if performative.more:
                return
            first = self._partial_first
            data = bytes(self._partial)
            self._partial = None
            self._partial_first = None
            self._credit = max(0, self._credit - 1)
            self._delivery_count += 1
        try:
            message = Message.decode(data)
        except AMQPError as error:
            self._logger.error("dropping undecodable delivery on link %r: %s", self.name, error)
            return
        delivery_id = first.delivery_id if first.delivery_id is not None else -1
        self._deliveries.put(Delivery(delivery_id=delivery_id, message=message, settled=bool(first.settled)))

    def _invoke_flow_handler(self, handler: Callable[[dict[Any, Any]], None], properties: dict[Any, Any]) -> None:
        """Call a user flow handler without letting it break the reader thread."""
        try:
            handler(properties)
        except Exception:
            self._logger.exception("flow-properties handler raised for link %r", self.name)


def _detach_failure(link_name: str, error: Error | None) -> BaseException:
    """Turn the ``error`` of an inbound ``detach`` into an exception."""
    if error is None:
        return ProtocolError(f"the broker detached link {link_name!r}")
    detail = f"the broker detached link {link_name!r}: {error.condition}"
    if error.description:
        detail = f"{detail}: {error.description}"
    return ProtocolError(detail)


__all__ = [
    "DEFAULT_CREDIT_TIMEOUT_SECONDS",
    "FLOW_PROPERTIES_BUFFER",
    "Delivery",
    "Link",
    "LinkRefusal",
    "LinkRole",
    "PendingDelivery",
    "ReceiverLink",
    "RefusalErrorFactory",
    "SenderLink",
]
