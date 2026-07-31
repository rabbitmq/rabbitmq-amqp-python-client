"""Publishers: one sender link per publisher, on the connection's shared pub/sub session.

This module implements ``step_020_publishers.md`` together with
``step_070_rejection-reason.md``, which share the same :class:`Outcome` shape.

Three layers live here:

* :class:`PublisherBuilder` — obtained from
  :meth:`~.connection.Connection.publisher_builder`, resolves the target address
  (§2.1) and attaches the link.
* :class:`Publisher` — one ``sender`` link, whose :meth:`Publisher.publish`
  transfers a message unsettled and blocks for the broker's ``disposition``.
* :class:`Outcome` / :class:`RejectionDetails` — the three modelled outcomes
  (§4) plus the structured rejection metadata RabbitMQ 4.3+ attaches to a
  ``Rejected`` one.

A ``Rejected`` or ``Released`` outcome is a normal result, not an error:
:class:`~.exceptions.PublisherError` is reserved for failures of the publish
attempt itself.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from .constants import (
    EXCHANGE_ADDRESS_TEMPLATE,
    EXCHANGE_ADDRESS_WITH_KEY_TEMPLATE,
    QUEUE_ADDRESS_TEMPLATE,
)
from .exceptions import InvalidAddressError, PublisherError
from .link import LinkRefusal, SenderLink
from .logging_utils import get_logger
from .management import ExchangeSpecification, QueueSpecification, encode_path_segment
from .wire import (
    EXPIRY_POLICY_LINK_DETACH,
    EXPIRY_POLICY_SESSION_END,
    RCV_SETTLE_MODE_FIRST,
    SND_SETTLE_MODE_UNSETTLED,
    TERMINUS_DURABILITY_NONE,
    Accepted,
    DeliveryState,
    Error,
    Message,
    Rejected,
    Released,
    Source,
    Target,
)

if TYPE_CHECKING:
    from .connection import Connection
    from .session import Session

#: Default bound on a whole :meth:`Publisher.publish` call (credit wait,
#: transfer and disposition wait combined).
DEFAULT_PUBLISH_TIMEOUT_SECONDS = 30.0

#: Prefix of a generated sender-link name, so a publisher is recognisable in
#: broker-side link listings.
PUBLISHER_LINK_PREFIX = "publisher"

#: ``Rejected.error.info`` keys carrying the structured rejection reason
#: (step_070 §2).
REJECTION_REASON_KEY = "reason"
REJECTION_QUEUE_KEY = "queue"

_logger = get_logger("publisher")


class OutcomeState(str, Enum):
    """The three publish outcomes this client models (step_020 §4)."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    RELEASED = "released"


@dataclass(frozen=True)
class RejectionDetails:
    """Structured reason a queue rejected a message (step_070 §3).

    Both fields are read independently from ``Rejected.error.info``: a key that
    is missing, or present with a non-string value, degrades that field to
    ``None`` rather than discarding the whole object.

    Attributes:
        reason: Broker-supplied explanation, e.g. the overflow condition.
        rejected_by_queue: Name of the queue that rejected the message.
    """

    reason: str | None = None
    rejected_by_queue: str | None = None


@dataclass(frozen=True)
class Outcome:
    """What the broker did with one published message (step_020 §4).

    Attributes:
        state: Which of the three modelled outcomes the broker reported.
        error: The raw AMQP ``error`` from a ``rejected`` outcome, when the
            broker sent one.
        rejection_details: Structured rejection metadata, ``None`` unless
            ``state`` is ``REJECTED`` and ``error.info`` carries at least one of
            ``reason``/``queue``.
    """

    state: OutcomeState
    error: Error | None = None
    rejection_details: RejectionDetails | None = None


@dataclass(frozen=True)
class PublishResult:
    """One published message paired with its outcome.

    Bundling the two lets a caller with many publishes in flight tell which
    result belongs to which message without its own correlation table.

    Attributes:
        message: The message that was published.
        outcome: What the broker reported for it.
    """

    message: Message
    outcome: Outcome


def queue_address(name: str) -> str:
    """Return the AMQP node address of a queue: ``/queues/{name}``.

    Args:
        name: Queue name, percent-encoded here.

    Returns:
        The node address, usable as a ``Target.address`` or a
        ``Message.properties.to``.
    """
    return QUEUE_ADDRESS_TEMPLATE.format(name=encode_path_segment(name))


def exchange_address(name: str, key: str | None = None) -> str:
    """Return the AMQP node address of an exchange, with an optional routing key.

    Args:
        name: Exchange name, percent-encoded here.
        key: Routing key, percent-encoded independently when given.

    Returns:
        ``/exchanges/{name}`` or ``/exchanges/{name}/{key}``.
    """
    if key is None:
        return EXCHANGE_ADDRESS_TEMPLATE.format(name=encode_path_segment(name))
    return EXCHANGE_ADDRESS_WITH_KEY_TEMPLATE.format(
        name=encode_path_segment(name),
        key=encode_path_segment(key),
    )


def rejection_details_from_error(error: Error | None) -> RejectionDetails | None:
    """Read step_070 §2's structured fields out of a ``rejected`` outcome's ``error``.

    Purely diagnostic, so it never raises: an absent ``error``, an absent
    ``info`` map, or an ``info`` map carrying neither key yields ``None``, and a
    key whose value is not a string yields ``None`` for that field alone.

    Args:
        error: The ``error`` of the ``rejected`` outcome, if it carried one.

    Returns:
        The structured details, or ``None`` when the broker sent none.
    """
    if error is None or not isinstance(error.info, Mapping):
        return None
    info = {str(key): value for key, value in error.info.items()}
    if REJECTION_REASON_KEY not in info and REJECTION_QUEUE_KEY not in info:
        return None
    return RejectionDetails(
        reason=_string_or_none(info.get(REJECTION_REASON_KEY)),
        rejected_by_queue=_string_or_none(info.get(REJECTION_QUEUE_KEY)),
    )


def outcome_from_delivery_state(state: DeliveryState) -> Outcome:
    """Map a ``disposition``'s delivery-state to a publish outcome (step_020 §4).

    Args:
        state: The delivery state the broker settled the transfer with.

    Returns:
        The matching outcome, carrying :class:`RejectionDetails` for a
        ``rejected`` state that supplies them.

    Raises:
        PublisherError: If the state is one this client does not model — a
        ``modified`` outcome is a protocol error here, not a fourth bucket.
    """
    if isinstance(state, Accepted):
        return Outcome(state=OutcomeState.ACCEPTED)
    if isinstance(state, Released):
        return Outcome(state=OutcomeState.RELEASED)
    if isinstance(state, Rejected):
        return Outcome(
            state=OutcomeState.REJECTED,
            error=state.error,
            rejection_details=rejection_details_from_error(state.error),
        )
    raise PublisherError(
        f"the broker settled the delivery with {type(state).__name__}, which this client does not model"
    )


def _string_or_none(value: Any) -> str | None:
    """Return ``value`` when it is a string, ``None`` for anything else."""
    return value if isinstance(value, str) else None


def _publisher_source(address: str | None) -> Source:
    """Build the ``source`` terminus of a publisher's sender link (step_020 §3.1)."""
    return Source(
        address=address,
        expiry_policy=EXPIRY_POLICY_LINK_DETACH,
        timeout=0,
        dynamic=False,
    )


def _publisher_target(address: str | None) -> Target:
    """Build the ``target`` terminus of a publisher's sender link (step_020 §3.1)."""
    return Target(
        address=address,
        durable=TERMINUS_DURABILITY_NONE,
        expiry_policy=EXPIRY_POLICY_SESSION_END,
        dynamic=False,
    )


class Publisher:
    """One sender link, and the publish calls that ride on it.

    Callers obtain a publisher from :meth:`PublisherBuilder.build`, never by
    constructing one. :meth:`publish` is safe to call from several threads at
    once: each delivery carries its own tag and is correlated back to its caller
    by delivery-id.

    Example:
        >>> publisher = connection.publisher_builder().queue("orders").build()
        >>> result = publisher.publish(Message("hello"))
        >>> result.outcome.state is OutcomeState.ACCEPTED
        True
        >>> publisher.close()
    """

    def __init__(
        self,
        connection: Connection,
        session: Session,
        address: str | None,
        *,
        publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    ) -> None:
        """Create an unattached publisher; :meth:`open` attaches its link.

        Args:
            connection: Connection tracking this publisher.
            session: The connection's shared pub/sub session to attach on.
            address: Resolved node address, or ``None`` for an anonymous
                publisher whose messages each carry their own ``to``.
            publish_timeout: Default bound on a whole :meth:`publish` call.
        """
        self._connection = connection
        self._session = session
        self._address = address
        self._publish_timeout = publish_timeout
        self._logger = _logger
        self._lock = threading.Lock()
        self._closed = False
        self._link = SenderLink(f"{PUBLISHER_LINK_PREFIX}-{uuid.uuid4().hex}")

    # --- public surface -------------------------------------------------

    @property
    def id(self) -> str:
        """The link name, unique within the connection, used as the tracking key."""
        return self._link.name

    @property
    def address(self) -> str | None:
        """The node address this publisher targets, ``None`` when anonymous."""
        return self._address

    @property
    def is_anonymous(self) -> bool:
        """Whether every message must supply its own ``properties.to``."""
        return self._address is None

    @property
    def is_open(self) -> bool:
        """Whether the publisher has not been closed and its link is attached."""
        with self._lock:
            if self._closed:
                return False
        return self._link.is_attached

    @property
    def publish_timeout(self) -> float:
        """Default bound, in seconds, on a whole :meth:`publish` call."""
        return self._publish_timeout

    def open(self) -> None:
        """Attach the sender link and register with the connection (step_020 §3.1).

        Called by :meth:`PublisherBuilder.build`; a refused ``attach`` leaves
        nothing half-open behind.

        Raises:
            PublisherError: If the broker refuses the address.
            AMQPTimeoutError: If the broker does not answer ``attach``.
        """
        self._attach_link()
        self._connection._register_publisher(self)
        self._logger.debug("publisher %r attached to %r", self.id, self._address)

    def publish(self, message: Message, timeout: float | None = None) -> PublishResult:
        """Send ``message`` unsettled and block until the broker settles it.

        Args:
            message: The message to publish. An anonymous publisher requires it
                to set ``properties.to``.
            timeout: Seconds bounding the whole call — credit wait, transfer and
                disposition wait combined; defaults to :attr:`publish_timeout`.

        Returns:
            The message paired with the outcome the broker reported. A
            ``rejected``/``released`` outcome is reported here, not raised.

        Raises:
            PublisherError: If the publisher is closed, if an anonymous publish
                carries no ``properties.to``, or if the broker settles with a
                state this client does not model.
            AMQPTimeoutError: If no credit is granted, or no ``disposition``
                arrives, within ``timeout``; the pending registration is dropped.
            AMQPError: Whatever failure killed the link, its session or the
                connection.
        """
        self._require_open()
        self._require_destination(message)
        limit = self._publish_timeout if timeout is None else timeout
        deadline = time.monotonic() + limit
        delivery_tag = uuid.uuid4().bytes
        pending = self._link.register_pending(delivery_tag)
        try:
            self._link.send_transfer(delivery_tag, message, settled=False, timeout=limit)
            state = pending.wait(max(0.0, deadline - time.monotonic()))
        except BaseException:
            self._link.cancel_pending(pending)
            raise
        return PublishResult(message=message, outcome=outcome_from_delivery_state(state))

    def close(self) -> None:
        """Detach the sender link and stop tracking this publisher (step_020 §3.4).

        Idempotent, and best-effort: a detach that times out or fails is logged
        rather than raised. The shared pub/sub session is left open — only
        :meth:`~.connection.Connection.close` ends it.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            self._link.detach()
        except Exception as error:  # teardown continues even if the link misbehaves
            self._logger.warning("ignoring error while detaching publisher %r: %s", self.id, error)
        self._connection._unregister_publisher(self)
        self._logger.debug("publisher %r closed", self.id)

    # --- internals ------------------------------------------------------

    def _attach_link(self) -> None:
        """Send this publisher's ``attach`` and wait for the broker's (step_020 §3.1)."""
        self._link.attach(
            self._session,
            source=_publisher_source(self._address),
            target=_publisher_target(self._address),
            on_refused=_refusal_error,
            snd_settle_mode=SND_SETTLE_MODE_UNSETTLED,
            rcv_settle_mode=RCV_SETTLE_MODE_FIRST,
        )

    def _reattach(self, session: Session) -> None:
        """Attach a fresh sender link on ``session`` after a reconnect (step_040 §3.3).

        The publisher keeps its identity: the new link carries the same name, so
        :attr:`id` — and the connection's registry key — do not move, and a
        caller holding this object never has to rebuild it. A publisher that was
        closed before the reconnect is left alone.

        Args:
            session: The connection's freshly re-opened pub/sub session.

        Raises:
            PublisherError: If the broker refuses the address, e.g. because the
                target queue is gone.
            AMQPTimeoutError: If the broker does not answer ``attach``.
        """
        name = self.id
        with self._lock:
            if self._closed:
                return
            self._session = session
            self._link = SenderLink(name)
        self._attach_link()
        self._logger.debug("publisher %r re-attached to %r", self.id, self._address)

    def _require_open(self) -> None:
        """Refuse a publish on a closed publisher.

        Raises:
            PublisherError: If :meth:`close` already ran.
        """
        with self._lock:
            if self._closed:
                raise PublisherError(f"publisher {self.id!r} is closed")

    def _require_destination(self, message: Message) -> None:
        """Refuse an anonymous publish that names no destination (step_020 §3.3).

        Raises:
            PublisherError: If this publisher has no address and ``message``
                carries no ``properties.to``.
        """
        if self._address is not None:
            return
        properties = message.properties
        if properties is None or not properties.to:
            raise PublisherError(
                f"publisher {self.id!r} is anonymous, so every message must set properties.to to a node address"
            )


class PublisherBuilder:
    """Chainable builder for one :class:`Publisher` (step_020 §2).

    A fresh builder comes from every
    :meth:`~.connection.Connection.publisher_builder` call and is not reusable
    after :meth:`build`. Setting neither :meth:`queue` nor :meth:`exchange`
    builds an **anonymous** publisher, whose messages each carry their own
    ``properties.to``.

    Example:
        >>> publisher = connection.publisher_builder().exchange("events").key("order.created").build()
    """

    def __init__(self, connection: Connection, *, publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS) -> None:
        """Create a builder bound to ``connection``.

        Args:
            connection: Connection whose shared pub/sub session hosts the link.
            publish_timeout: Default bound on the built publisher's
                :meth:`Publisher.publish` calls.
        """
        self._connection = connection
        self._publish_timeout = publish_timeout
        self._queue: str | None = None
        self._exchange: str | None = None
        self._key: str | None = None

    # --- setters --------------------------------------------------------

    def queue(self, queue: str | QueueSpecification) -> PublisherBuilder:
        """Target the publisher at a queue; mutually exclusive with :meth:`exchange`.

        Args:
            queue: Queue name, or a :class:`~.management.QueueSpecification`
                whose name is read. A specification is never declared here.

        Returns:
            This builder.

        Raises:
            InvalidAddressError: If the name is empty.
        """
        self._queue = _require_address_name(
            "queue", queue.queue_name if isinstance(queue, QueueSpecification) else queue
        )
        return self

    def exchange(self, exchange: str | ExchangeSpecification) -> PublisherBuilder:
        """Target the publisher at an exchange; mutually exclusive with :meth:`queue`.

        Args:
            exchange: Exchange name, or an
                :class:`~.management.ExchangeSpecification` whose name is read.

        Returns:
            This builder.

        Raises:
            InvalidAddressError: If the name is empty.
        """
        name = exchange.exchange_name if isinstance(exchange, ExchangeSpecification) else exchange
        self._exchange = _require_address_name("exchange", name)
        return self

    def key(self, routing_key: str) -> PublisherBuilder:
        """Fix the routing key appended to an :meth:`exchange` target.

        Args:
            routing_key: The routing key; meaningless, and refused by
                :meth:`build`, without :meth:`exchange`.

        Returns:
            This builder.
        """
        self._key = routing_key
        return self

    # --- terminal operations --------------------------------------------

    def address(self) -> str | None:
        """Resolve the target node address (step_020 §2.1).

        Returns:
            ``/queues/{name}``, ``/exchanges/{name}``,
            ``/exchanges/{name}/{key}``, or ``None`` for an anonymous publisher.

        Raises:
            InvalidAddressError: If both a queue and an exchange were set, or a
                routing key was set without an exchange.
        """
        if self._queue is not None and self._exchange is not None:
            raise InvalidAddressError("exchange and queue cannot be set together")
        if self._key is not None and self._exchange is None:
            raise InvalidAddressError("exchange or queue must be set: a routing key alone is not an address")
        if self._queue is not None:
            return queue_address(self._queue)
        if self._exchange is not None:
            return exchange_address(self._exchange, self._key)
        return None

    def build(self) -> Publisher:
        """Resolve the address, attach the sender link, and return the publisher.

        Opens the connection's shared pub/sub session when this is the first
        publisher (or consumer) built on it, and reuses it afterwards.

        Returns:
            The ready-to-use publisher.

        Raises:
            InvalidAddressError: If the address configuration is inconsistent.
            PublisherError: If the broker refuses the address.
            AMQPTimeoutError: If the broker does not answer ``begin``/``attach``.
        """
        address = self.address()
        session = self._connection._pub_sub_session()
        publisher = Publisher(self._connection, session, address, publish_timeout=self._publish_timeout)
        publisher.open()
        return publisher


def _require_address_name(kind: str, name: str) -> str:
    """Return ``name``, refusing an empty one.

    Raises:
        InvalidAddressError: If ``name`` is empty.
    """
    if not name:
        raise InvalidAddressError(f"a non-empty {kind} name is required")
    return name


def _refusal_error(refusal: LinkRefusal) -> PublisherError:
    """Turn a refused ``attach`` into the error :meth:`Publisher.open` raises."""
    return PublisherError(refusal.describe())


__all__ = [
    "DEFAULT_PUBLISH_TIMEOUT_SECONDS",
    "PUBLISHER_LINK_PREFIX",
    "REJECTION_QUEUE_KEY",
    "REJECTION_REASON_KEY",
    "Outcome",
    "OutcomeState",
    "PublishResult",
    "Publisher",
    "PublisherBuilder",
    "RejectionDetails",
    "exchange_address",
    "outcome_from_delivery_state",
    "queue_address",
    "rejection_details_from_error",
]
