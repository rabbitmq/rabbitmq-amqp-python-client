"""Consumers: one receiver link per consumer, on the connection's shared pub/sub session.

This module implements ``step_030_consumers.md`` together with
``step_060_consumer_strategy.md``, which widens the plain unsettled/presettled
attach into the three-way :class:`ConsumerSettleStrategy` — the presettled case
is the same attach/delivery path with ``snd-settle-mode`` flipped and no
``disposition`` ever sent, and ``DirectReplyTo`` additionally attaches to
RabbitMQ's direct-reply-to pseudo-queue instead of a caller-supplied one —
``step_090_quorum-queue-notifications.md``, which reads one extra property off
the ``flow`` frames that path already processes, and
``step_080_stream-filtering.md``, which only adds entries to the receiver's
``source.filter`` map.

Six layers live here:

* :class:`ConsumerBuilder` — obtained from
  :meth:`~.connection.Connection.consumer_builder`, resolves the queue address
  (§2.1), attaches the link and grants the initial credit.
* :class:`QuorumConsumerOptions` — the quorum-queue-only sub-builder, a view over
  the same :class:`ConsumerBuilder` (step_090 §2).
* :class:`StreamOptions` and :class:`StreamFilterOptions` — the stream-only
  sub-builders, views over that same builder, which between them fill in the
  ``source.filter`` map (step_080 §1-§3).
* :class:`Consumer` — one ``receiver`` link plus the dedicated delivery-loop
  thread that hands every delivery to the caller's handler.
* :class:`Context` — the per-delivery settlement handle that handler is given,
  good for exactly one of ``accept``/``discard``/``requeue``.

The delivery loop never runs on the connection's frame-reader thread, so a slow
handler throttles only its own consumer. Credit is tied to settlement rather
than to delivery, which is what bounds the number of unsettled deliveries in
flight to ``initial_credits`` (§3.3).
"""

from __future__ import annotations

import re
import threading
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

from .constants import (
    AMQP_APPLICATION_PROPERTIES_FILTER,
    AMQP_PROPERTIES_FILTER,
    AMQP_SQL_FILTER,
    DIRECT_REPLY_TO_CAPABILITY,
    RABBITMQ_ACTIVE_PROPERTY,
    SQL_FILTER_NAME,
    STREAM_FILTER_VALUES_FILTER,
    STREAM_MATCH_UNFILTERED_FILTER,
    STREAM_OFFSET_ANNOTATION,
    STREAM_OFFSET_SPEC_FILTER,
)
from .exceptions import (
    AMQPError,
    ConsumerError,
    InvalidAddressError,
    ProtocolError,
    ValidationError,
)
from .link import Delivery, LinkRefusal, ReceiverLink
from .logging_utils import get_logger
from .management import QueueSpecification
from .publisher import queue_address
from .wire import (
    EXPIRY_POLICY_LINK_DETACH,
    RCV_SETTLE_MODE_FIRST,
    SND_SETTLE_MODE_SETTLED,
    SND_SETTLE_MODE_UNSETTLED,
    Accepted,
    DeliveryState,
    Described,
    Long,
    Message,
    Modified,
    Rejected,
    Released,
    Source,
    Symbol,
    Timestamp,
)

if TYPE_CHECKING:
    from .connection import Connection
    from .session import Session

#: Link credit granted at attach, and topped back up to as deliveries settle.
DEFAULT_INITIAL_CREDITS = 100

#: Prefix of a generated receiver-link name, so a consumer is recognisable in
#: broker-side link listings.
CONSUMER_LINK_PREFIX = "consumer"

#: Slice the delivery loop blocks for before re-checking whether it must stop.
DELIVERY_POLL_INTERVAL_SECONDS = 0.2

#: How long :meth:`Consumer.close` waits for the delivery loop to stop.
DELIVERY_LOOP_JOIN_TIMEOUT_SECONDS = 5.0

#: Prefix every annotation key a caller attaches to an outcome must carry (§4).
ANNOTATION_KEY_PREFIX = "x-"

#: What a relative-interval offset must look like (step_080 §1.1): a count
#: followed by one of years, months, days, hours, minutes or seconds.
STREAM_INTERVAL_PATTERN = re.compile(r"^[0-9]+[YMDhms]$")

#: The one ``amqp:properties-filter`` field this client writes (step_080 §3).
SUBJECT_FILTER_FIELD = "subject"

_logger = get_logger("consumer")

#: Called once per delivery with the delivery's :class:`Context` and message.
#: Exceptions escaping it are logged by the delivery loop and never propagate.
MessageHandler = Callable[["Context", Message], None]

#: Called as ``handler(consumer, is_active)`` every time the broker reports
#: whether this consumer's link is the active one of a single-active-consumer
#: quorum queue (step_090 §3). Exceptions escaping it are logged and never
#: propagate.
SingleActiveConsumerStateHandler = Callable[["Consumer", bool], None]


def parse_active_flag(value: Any) -> bool:
    """Read one ``rabbitmq:active`` ``flow`` property value (step_090 §1 point 2).

    Args:
        value: What the broker put at ``rabbitmq:active``. RabbitMQ may encode it
            as an AMQP ``boolean`` or as any of the AMQP integer types, all of
            which decode to a Python ``bool`` or ``int`` here.

    Returns:
        A ``boolean`` as-is; for an integer, whether it is non-zero — ``0`` means
        standby, anything else means active.

    Raises:
        ProtocolError: If the value is of neither kind, e.g. a string: no
            active/standby status can be read from it.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    raise ProtocolError(
        f"the {RABBITMQ_ACTIVE_PROPERTY!r} flow property carried an unusable {type(value).__name__}: {value!r}"
    )


class ConsumerSettleStrategy(Enum):
    """How deliveries on a :class:`Consumer` are settled (step_060_consumer_strategy.md §1)."""

    #: Default: every delivery is settled via :class:`Context`
    #: (step_030_consumers.md §4). ``snd-settle-mode = unsettled`` at attach.
    EXPLICIT_SETTLE = "explicit_settle"
    #: The broker settles every delivery itself before the ``transfer`` is even
    #: sent; every :class:`Context` method raises :class:`~.exceptions.ConsumerError`
    #: (§3.2). ``snd-settle-mode = settled`` at attach.
    PRESETTLED = "presettled"
    #: Attaches to RabbitMQ's direct-reply-to pseudo-queue instead of a
    #: caller-supplied one, implicitly presettled (§3.3). Mutually exclusive
    #: with :meth:`ConsumerBuilder.queue` and with
    #: :meth:`QuorumConsumerOptions.single_active_consumer_state_changed` (§5).
    DIRECT_REPLY_TO = "direct_reply_to"


class StreamOffsetSpecification(Enum):
    """A named position a stream consumer can start reading from (step_080 §1.1)."""

    #: The oldest message the stream still retains.
    FIRST = "first"
    #: The most recently published chunk.
    LAST = "last"
    #: Whatever is published after the attach; nothing retained is replayed.
    NEXT = "next"


#: Everything :meth:`StreamOptions.offset` accepts: a named position, an
#: absolute offset, a point in time, or a relative interval such as ``"7D"``.
StreamOffset = StreamOffsetSpecification | int | datetime | str


@dataclass
class StreamConfiguration:
    """What :class:`StreamOptions` and :class:`StreamFilterOptions` collected.

    Held by the :class:`ConsumerBuilder` both sub-builders are views over, and
    turned into the receiver's ``source.filter`` map by :func:`stream_filter_set`
    at :meth:`ConsumerBuilder.build` time. Every field left unset stays out of
    that map, which is how the broker's own defaults are asked for.

    Attributes:
        offset: Where to start reading (step_080 §1.1).
        filter_values: Bloom-filter values this consumer wants, OR-combined by
            the broker (step_080 §2.2).
        match_unfiltered: Whether messages carrying no ``x-stream-filter-value``
            annotation are delivered as well (step_080 §2.2).
        subject: Pattern the broker matches against ``properties.subject``
            (step_080 §3).
        properties: Patterns the broker matches against application properties,
            one entry per key (step_080 §3).
        sql: A broker-evaluated boolean expression (step_080 §3, RabbitMQ 4.2+).
    """

    offset: StreamOffset | None = None
    filter_values: tuple[str, ...] | None = None
    match_unfiltered: bool | None = None
    subject: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    sql: str | None = None


def stream_offset_filter_value(offset: StreamOffset) -> Any:
    """Turn one :meth:`StreamOptions.offset` argument into its filter value (step_080 §1.1).

    Args:
        offset: A :class:`StreamOffsetSpecification`, an absolute offset, a
            :class:`~datetime.datetime` (a naive one is read as local time), or a
            relative-interval string.

    Returns:
        The value to describe under ``rabbitmq:stream-offset-spec``: the plain
        name of a named position, an AMQP ``long``, an AMQP ``timestamp`` in
        milliseconds, or the interval string unmodified.

    Raises:
        ConsumerError: If a string does not match :data:`STREAM_INTERVAL_PATTERN`,
            or an absolute offset is negative. Both would be silently ignored by
            the broker, so they are refused before anything is attached (§4).
    """
    if isinstance(offset, StreamOffsetSpecification):
        return offset.value
    if isinstance(offset, datetime):
        return Timestamp(int(offset.timestamp() * 1000))
    if isinstance(offset, int):
        if offset < 0:
            raise ConsumerError(f"an absolute stream offset must be >= 0, got {offset}")
        return Long(offset)
    if not STREAM_INTERVAL_PATTERN.match(offset):
        raise ConsumerError(
            f"{offset!r} is not a stream offset interval: it must match {STREAM_INTERVAL_PATTERN.pattern} "
            "(for example '7D' or '12h')"
        )
    return offset


def _described(name: str, value: Any) -> Described:
    """Describe one filter-set value, as every ``source.filter`` entry must be.

    Args:
        name: Descriptor naming the kind of filter, which is also the name the
            entry has in the filter set for all but the SQL filter.
        value: The value that descriptor annotates.

    Returns:
        The described value to put in the map.
    """
    return Described(Symbol(name), value)


def stream_filter_set(configuration: StreamConfiguration) -> dict[str, Any] | None:
    """Build the receiver's ``source.filter`` map from ``configuration`` (step_080 §1-§3).

    Args:
        configuration: What the stream sub-builders collected.

    Returns:
        One entry per configured filter, all of which the broker ANDs together,
        or ``None`` when nothing was configured — an absent filter set asks for
        the broker's defaults.

    Raises:
        ConsumerError: If the offset specification is invalid (§1.1).
    """
    filter_set: dict[str, Any] = {}
    if configuration.offset is not None:
        filter_set[STREAM_OFFSET_SPEC_FILTER] = _described(
            STREAM_OFFSET_SPEC_FILTER, stream_offset_filter_value(configuration.offset)
        )
    if configuration.filter_values is not None:
        filter_set[STREAM_FILTER_VALUES_FILTER] = _described(
            STREAM_FILTER_VALUES_FILTER, list(configuration.filter_values)
        )
    if configuration.match_unfiltered is not None:
        filter_set[STREAM_MATCH_UNFILTERED_FILTER] = _described(
            STREAM_MATCH_UNFILTERED_FILTER, configuration.match_unfiltered
        )
    if configuration.subject is not None:
        # The properties filter is a map keyed by AMQP field name, and the broker
        # only accepts those keys as symbols.
        filter_set[AMQP_PROPERTIES_FILTER] = _described(
            AMQP_PROPERTIES_FILTER, {Symbol(SUBJECT_FILTER_FIELD): configuration.subject}
        )
    if configuration.properties:
        filter_set[AMQP_APPLICATION_PROPERTIES_FILTER] = _described(
            AMQP_APPLICATION_PROPERTIES_FILTER, dict(configuration.properties)
        )
    if configuration.sql is not None:
        filter_set[SQL_FILTER_NAME] = _described(AMQP_SQL_FILTER, configuration.sql)
    return filter_set or None


def stream_offset_of(message: Message) -> int | None:
    """Read the offset RabbitMQ annotates a stream delivery with.

    Args:
        message: A received message.

    Returns:
        The ``x-stream-offset`` message annotation, or ``None`` for a delivery
        that carries none — which is every delivery from a queue that is not a
        stream.
    """
    annotations = message.message_annotations
    if annotations is None:
        return None
    value = annotations.value.get(STREAM_OFFSET_ANNOTATION)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _consumer_source(address: str | None, filter_set: Mapping[str, Any] | None = None) -> Source:
    """Build the ``source`` terminus of a consumer's receiver link (step_030 §3.1).

    Args:
        address: The resolved ``/queues/{name}`` node address. Typed as
            optional only because :attr:`Consumer._address` is shared with
            ``ConsumerSettleStrategy.DIRECT_REPLY_TO`` (§3.3), which never calls
            this function — every actual caller has already required a
            non-``None`` queue.
        filter_set: Stream offset/filter entries to attach with (step_080 §1),
            or ``None`` for a consumer that asked for none.

    Returns:
        The terminus to put on ``attach.source``.
    """
    return Source(
        address=address,
        expiry_policy=EXPIRY_POLICY_LINK_DETACH,
        timeout=0,
        dynamic=False,
        filter=None if filter_set is None else dict(filter_set),
    )


def _direct_reply_to_source() -> Source:
    """Build the dynamic ``source`` terminus for a direct-reply-to consumer (step_060_consumer_strategy.md §3.3).

    Returns:
        The terminus to put on ``attach.source``: no caller-supplied address,
        ``dynamic = true`` so the broker generates one, and the capability that
        marks this as a direct-reply-to attach.
    """
    return Source(
        address=None,
        expiry_policy=EXPIRY_POLICY_LINK_DETACH,
        timeout=0,
        dynamic=True,
        capabilities=[DIRECT_REPLY_TO_CAPABILITY],
    )


def _refusal_error(refusal: LinkRefusal) -> ConsumerError:
    """Turn a refused ``attach`` into the error :meth:`Consumer.open` raises."""
    return ConsumerError(refusal.describe())


def _validated_annotations(annotations: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``annotations`` as a plain dict, refusing keys the broker would not accept.

    Args:
        annotations: Annotations to merge into the message's own, whose keys must
            all start with ``x-`` (§4).

    Returns:
        The same mapping as a dict with string keys.

    Raises:
        ValidationError: If any key does not start with ``x-``. Raised before
            anything is sent, so the delivery stays unsettled and settleable.
    """
    refused = sorted(str(key) for key in annotations if not str(key).startswith(ANNOTATION_KEY_PREFIX))
    if refused:
        raise ValidationError(
            f"annotation keys must start with {ANNOTATION_KEY_PREFIX!r}, which these do not: {', '.join(refused)}"
        )
    return {str(key): value for key, value in annotations.items()}


class Context:
    """Settles the one delivery it was built for (step_030 §4).

    Handed to :data:`MessageHandler` once per delivery. Exactly one of
    :meth:`accept`, :meth:`discard` and :meth:`requeue` may be called, at most
    once; every later call raises :class:`~.exceptions.ConsumerError`. A context
    for a presettled delivery raises from all three, since the broker settled
    that delivery before it ever reached this client
    (step_060_consumer_strategy.md §3.2/§3.3).

    Example:
        >>> def handler(context, message):
        ...     if message.body_as_string() == "poison":
        ...         context.discard({"x-reason": "unparseable"})
        ...     else:
        ...         context.accept()
    """

    def __init__(self, consumer: Consumer, delivery_id: int, *, presettled: bool) -> None:
        """Bind a context to one delivery.

        Args:
            consumer: The consumer that received the delivery.
            delivery_id: Session-scoped delivery-id to settle.
            presettled: Whether the broker already settled this delivery.
        """
        self._consumer = consumer
        self._delivery_id = delivery_id
        self._presettled = presettled
        self._lock = threading.Lock()
        self._settled = False

    @property
    def delivery_id(self) -> int:
        """The delivery-id this context settles."""
        return self._delivery_id

    @property
    def is_presettled(self) -> bool:
        """Whether the broker settled this delivery itself, leaving nothing to do."""
        return self._presettled

    @property
    def is_settled(self) -> bool:
        """Whether one of the settlement methods already ran."""
        with self._lock:
            return self._settled

    def accept(self) -> None:
        """Settle the delivery as ``accepted``: it was processed successfully.

        Raises:
            ConsumerError: If the delivery is already settled, was presettled, or
                the consumer is closed.
        """
        self._settle(Accepted())

    def discard(self, annotations: Mapping[str, Any] | None = None) -> None:
        """Settle the delivery as unprocessable, so the broker drops or dead-letters it.

        Args:
            annotations: When given, the delivery is settled as
                ``modified{delivery-failed=true, undeliverable-here=true}``
                carrying these annotations instead of as ``rejected``. Every key
                must start with ``x-``.

        Raises:
            ValidationError: If an annotation key does not start with ``x-``;
                nothing is sent, so the delivery stays settleable.
            ConsumerError: If the delivery is already settled, was presettled, or
                the consumer is closed.
        """
        if annotations is None:
            self._settle(Rejected())
            return
        self._settle(
            Modified(
                delivery_failed=True,
                undeliverable_here=True,
                message_annotations=_validated_annotations(annotations),
            )
        )

    def requeue(self, annotations: Mapping[str, Any] | None = None, delivery_failed: bool = False) -> None:
        """Return the delivery to the queue, so the broker may redeliver it.

        Args:
            annotations: When given, the delivery is settled as
                ``modified{undeliverable-here=false}`` carrying these annotations
                instead of as ``released``. Every key must start with ``x-``.
            delivery_failed: Whether this counts as a failed delivery attempt,
                incrementing the message's delivery-count. Setting it also
                selects the ``modified`` outcome, which is the only one that can
                carry the flag.

        Raises:
            ValidationError: If an annotation key does not start with ``x-``;
                nothing is sent, so the delivery stays settleable.
            ConsumerError: If the delivery is already settled, was presettled, or
                the consumer is closed.
        """
        if annotations is None and not delivery_failed:
            self._settle(Released())
            return
        self._settle(
            Modified(
                delivery_failed=delivery_failed,
                undeliverable_here=False,
                message_annotations=None if annotations is None else _validated_annotations(annotations),
            )
        )

    def _settle(self, state: DeliveryState) -> None:
        """Claim the one settlement this context allows, then send it."""
        if self._presettled:
            raise ConsumerError(
                f"delivery {self._delivery_id} arrived presettled, so it cannot be settled by this client"
            )
        with self._lock:
            if self._settled:
                raise ConsumerError(f"delivery {self._delivery_id} has already been settled")
            self._settled = True
        self._consumer._settle(self._delivery_id, state)


class Consumer:
    """One receiver link, and the delivery loop that drains it (step_030 §3).

    Callers obtain a consumer from :meth:`ConsumerBuilder.build`, never by
    constructing one. Deliveries are dispatched on a dedicated thread, so the
    handler may block for as long as it likes without affecting anything else on
    the connection — it only stops new deliveries once ``initial_credits``
    deliveries are unsettled.

    Example:
        >>> consumer = connection.consumer_builder().queue("orders").message_handler(handler).build()
        >>> consumer.pause()
        >>> consumer.unpause()
        >>> consumer.close()
    """

    def __init__(
        self,
        connection: Connection,
        session: Session,
        queue: str | None,
        handler: MessageHandler,
        *,
        initial_credits: int = DEFAULT_INITIAL_CREDITS,
        settle_strategy: ConsumerSettleStrategy = ConsumerSettleStrategy.EXPLICIT_SETTLE,
        single_active_consumer_handler: SingleActiveConsumerStateHandler | None = None,
        stream_filter: Mapping[str, Any] | None = None,
    ) -> None:
        """Create an unattached consumer; :meth:`open` attaches its link.

        Args:
            connection: Connection tracking this consumer.
            session: The connection's shared pub/sub session to attach on.
            queue: Name of the queue to consume from, or ``None`` for
                ``ConsumerSettleStrategy.DIRECT_REPLY_TO``, whose address is
                broker-generated and only known once ``open()`` completes
                (step_060_consumer_strategy.md §3.3 point 2).
            handler: Callback invoked once per delivery.
            initial_credits: Link credit granted at attach and kept topped up.
            settle_strategy: How deliveries are settled
                (step_060_consumer_strategy.md §1); anything but
                ``EXPLICIT_SETTLE`` makes every :class:`Context` method raise.
            single_active_consumer_handler: Callback invoked with every
                active/standby status the broker reports for this link
                (step_090 §3). Without one, the status is never watched for.
            stream_filter: Already-built ``source.filter`` entries to attach with
                (step_080 §1), as :func:`stream_filter_set` returns them.
        """
        self._connection = connection
        self._session = session
        self._queue = queue
        self._address = None if queue is None else queue_address(queue)
        self._handler = handler
        self._initial_credits = initial_credits
        self._settle_strategy = settle_strategy
        self._single_active_consumer_handler = single_active_consumer_handler
        self._stream_filter = None if stream_filter is None else dict(stream_filter)
        self._last_stream_offset: int | None = None
        self._logger = _logger
        self._lock = threading.RLock()
        self._closed = False
        self._paused = False
        self._unsettled = 0
        self._stopped = threading.Event()
        self._name = f"{CONSUMER_LINK_PREFIX}-{uuid.uuid4().hex}"
        self._link = ReceiverLink(self._name)
        self._delivery_loop: threading.Thread | None = None
        self._states: Queue[bool] = Queue()
        self._notification_loop: threading.Thread | None = None

    # --- public surface -------------------------------------------------

    @property
    def id(self) -> str:
        """The link name, unique within the connection, used as the tracking key."""
        return self._name

    @property
    def queue(self) -> str | None:
        """The queue this consumer is attached to (step_030_consumers.md §3.6).

        For ``ConsumerSettleStrategy.DIRECT_REPLY_TO`` this is not a caller-
        supplied name but the broker-generated pseudo-queue address read back
        from the ``attach`` reply (step_060_consumer_strategy.md §3.3 point 2) —
        ``None`` until :meth:`open`/``ConsumerBuilder.build()`` has completed.
        """
        return self._queue

    @property
    def address(self) -> str | None:
        """The resolved ``/queues/{name}`` node address of that queue.

        Same caveat as :attr:`queue` for ``ConsumerSettleStrategy.DIRECT_REPLY_TO``.
        """
        return self._address

    @property
    def initial_credits(self) -> int:
        """Link credit this consumer keeps outstanding while it is not paused."""
        return self._initial_credits

    @property
    def settle_strategy(self) -> ConsumerSettleStrategy:
        """Which settlement strategy this consumer was built with (step_060_consumer_strategy.md §1)."""
        return self._settle_strategy

    @property
    def _presettled(self) -> bool:
        """Whether the broker settles every delivery itself, leaving nothing for `Context` to do."""
        return self._settle_strategy is not ConsumerSettleStrategy.EXPLICIT_SETTLE

    @property
    def is_presettled(self) -> bool:
        """Whether the broker settles every delivery itself (step_060_consumer_strategy.md §1)."""
        return self._presettled

    @property
    def is_open(self) -> bool:
        """Whether the consumer has not been closed and its link is attached."""
        with self._lock:
            if self._closed:
                return False
        return self._link.is_attached

    @property
    def is_paused(self) -> bool:
        """Whether credit is currently held at zero by :meth:`pause`."""
        with self._lock:
            return self._paused

    @property
    def unsettled_message_count(self) -> int:
        """Deliveries handed to the handler and not yet settled; always ``0`` when presettled (§3.6)."""
        with self._lock:
            return self._unsettled

    @property
    def last_stream_offset(self) -> int | None:
        """Highest ``x-stream-offset`` seen so far, or ``None`` outside a stream.

        This is what a re-attach after a reconnect resumes one past, so a caller
        watching it sees exactly where recovery would restart.
        """
        with self._lock:
            return self._last_stream_offset

    def open(self) -> None:
        """Attach the receiver link, grant credit and start the delivery loop (§3.1).

        Called by :meth:`ConsumerBuilder.build`; a refused ``attach`` leaves
        nothing half-open behind.

        Raises:
            ConsumerError: If the broker refuses the queue.
            AMQPTimeoutError: If the broker does not answer ``attach``.
        """
        self._attach_link()
        self._start_delivery_loop()
        self._start_notification_loop()
        self._connection._register_consumer(self)
        self._logger.debug("consumer %r attached to queue %r", self.id, self._queue)

    def pause(self) -> None:
        """Hold the link at zero credit, so the broker sends nothing new (§3.4).

        A no-op when already paused. Deliveries the broker had already put on the
        wire still arrive and are still dispatched.

        Raises:
            ConsumerError: If the consumer is closed.
        """
        with self._lock:
            self._require_open()
            if self._paused:
                return
            self._paused = True
            self._link.flow(0)
        self._logger.debug("consumer %r paused", self.id)

    def unpause(self) -> None:
        """Restore the outstanding credit to ``initial_credits`` (§3.4).

        A no-op when not paused.

        Raises:
            ConsumerError: If the consumer is closed.
        """
        with self._lock:
            self._require_open()
            if not self._paused:
                return
            self._paused = False
            self._link.flow(self._initial_credits)
        self._logger.debug("consumer %r unpaused", self.id)

    def close(self) -> None:
        """Stop the delivery loop, detach the receiver link and stop being tracked (§3.5).

        Idempotent, and best-effort: a detach that times out or fails is logged
        rather than raised. No delivery is dispatched to the handler once this has
        been called, even one that was already on the wire. The shared pub/sub
        session is left open — only :meth:`~.connection.Connection.close` ends it.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._stopped.set()
        try:
            self._link.detach()
        except Exception as error:  # teardown continues even if the link misbehaves
            self._logger.warning("ignoring error while detaching consumer %r: %s", self.id, error)
        self._join_loops()
        self._connection._unregister_consumer(self)
        self._logger.debug("consumer %r closed", self.id)

    # --- internals ------------------------------------------------------

    def _attach_link(self) -> None:
        """Attach the receiver link and grant its initial credit (§3.1 steps 4-6).

        A consumer that was paused before a reconnect is re-attached with zero
        credit, so :meth:`pause` survives the recovery it did not ask to end, and
        a stream consumer resumes one past the last offset it saw rather than
        replaying from its original offset specification (see
        :meth:`_effective_stream_filter`).
        """
        source = (
            _direct_reply_to_source()
            if self._settle_strategy is ConsumerSettleStrategy.DIRECT_REPLY_TO
            else _consumer_source(self._address, self._effective_stream_filter())
        )
        self._link.attach(
            self._session,
            source=source,
            target=None,
            on_refused=_refusal_error,
            snd_settle_mode=(
                SND_SETTLE_MODE_UNSETTLED
                if self._settle_strategy is ConsumerSettleStrategy.EXPLICIT_SETTLE
                else SND_SETTLE_MODE_SETTLED
            ),
            rcv_settle_mode=RCV_SETTLE_MODE_FIRST,
        )
        if self._settle_strategy is ConsumerSettleStrategy.DIRECT_REPLY_TO:
            self._resolve_direct_reply_to_address()
        if self._single_active_consumer_handler is not None:
            # Registered before the initial flow — and again on every fresh link a
            # reconnect brings — because the broker's first flow can already carry
            # the status. The link replays whatever it buffered before this call,
            # which is what closes step_090 §3's race.
            self._link.on_flow_properties(self._observe_flow_properties)
        try:
            self._link.flow(0 if self._paused else self._initial_credits)
        except BaseException:
            self._link.detach()
            raise

    def _resolve_direct_reply_to_address(self) -> None:
        """Read back the broker-generated pseudo-queue address (step_060_consumer_strategy.md §3.3 point 2).

        Raises:
            ConsumerError: If the broker's attach reply carried no usable address.
        """
        remote_attach = self._link.remote_attach
        address = remote_attach.source.address if remote_attach and remote_attach.source else None
        if not address:
            raise ConsumerError(
                f"consumer {self.id!r} used ConsumerSettleStrategy.DIRECT_REPLY_TO but the broker returned no address"
            )
        with self._lock:
            self._queue = address
            self._address = address

    def _effective_stream_filter(self) -> dict[str, Any] | None:
        """Return the filter set to attach with, resumed past what was consumed.

        The first attach uses exactly what the builder configured. Every later one
        — only a reconnect re-attaches (step_040 §3.3) — replaces the offset
        specification with the absolute offset after the last one delivered, so
        recovery does not replay from ``first`` (or from a fixed offset, or skip to
        ``next``) what the handler has already seen. step_080 §5 leaves this
        unspecified; RabbitMQ annotating every stream delivery with
        ``x-stream-offset`` is what makes it possible without any caller
        bookkeeping. A consumer that has been delivered nothing, or that consumes
        from a queue that is not a stream, is re-attached unchanged.

        Returns:
            The filter entries for ``attach.source``, or ``None`` when there are
            none.
        """
        with self._lock:
            last_offset = self._last_stream_offset
        if last_offset is None:
            return None if self._stream_filter is None else dict(self._stream_filter)
        resumed = dict(self._stream_filter or {})
        resumed[STREAM_OFFSET_SPEC_FILTER] = _described(STREAM_OFFSET_SPEC_FILTER, Long(last_offset + 1))
        return resumed

    def _track_stream_offset(self, message: Message) -> None:
        """Remember the highest stream offset delivered, for a later re-attach."""
        offset = stream_offset_of(message)
        if offset is None:
            return
        with self._lock:
            if self._last_stream_offset is None or offset > self._last_stream_offset:
                self._last_stream_offset = offset

    def _start_delivery_loop(self) -> None:
        """Start the thread that drains the receiver link (§3.2)."""
        self._delivery_loop = threading.Thread(
            target=self._run_delivery_loop,
            name=f"amqp-{self._name}",
            daemon=True,
        )
        self._delivery_loop.start()

    def _start_notification_loop(self) -> None:
        """Start the thread that reports active/standby changes (step_090 §3).

        Only a consumer that registered a handler gets this thread; an ordinary
        one costs nothing.
        """
        if self._single_active_consumer_handler is None:
            return
        self._notification_loop = threading.Thread(
            target=self._run_notification_loop,
            name=f"amqp-{self._name}-sac",
            daemon=True,
        )
        self._notification_loop.start()

    def _reattach(self, session: Session) -> None:
        """Attach a fresh receiver link on ``session`` after a reconnect (step_040 §3.3).

        The consumer keeps its identity and its whole configuration — queue,
        handler, credits, settle strategy and paused flags all live on this
        object — so a caller holding it never has to rebuild it. The delivery
        loop that was draining the dead link is stopped and replaced, since the
        old one would otherwise race the new one for the same deliveries. A
        consumer that was closed before the reconnect is left alone.

        Args:
            session: The connection's freshly re-opened pub/sub session.

        Raises:
            ConsumerError: If the broker refuses the queue, e.g. because it is
                gone.
            AMQPTimeoutError: If the broker does not answer ``attach``.
        """
        with self._lock:
            if self._closed:
                return
        self._stopped.set()
        self._join_loops()
        with self._lock:
            self._session = session
            self._link = ReceiverLink(self._name)
            self._unsettled = 0
            if self._settle_strategy is ConsumerSettleStrategy.DIRECT_REPLY_TO:
                # The pseudo-queue is session-scoped (step_060_consumer_strategy.md
                # §3.3 point 5): it dies with the old session, and the fresh attach
                # below gets a brand new broker-generated address, not the old one.
                self._queue = None
                self._address = None
        self._attach_link()
        self._stopped.clear()
        self._start_delivery_loop()
        self._start_notification_loop()
        self._logger.debug("consumer %r re-attached to queue %r", self.id, self._queue)

    def _run_delivery_loop(self) -> None:
        """Hand every delivery to the handler until the consumer or the link stops (§3.2)."""
        while not self._stopped.is_set():
            try:
                delivery = self._link.receive(timeout=DELIVERY_POLL_INTERVAL_SECONDS)
            except AMQPError as error:
                self._logger.debug("consumer %r stopped receiving: %s", self.id, error)
                return
            # Re-check after waking: a delivery that arrived during teardown must
            # not reach the handler (§3.5).
            if delivery is None or self._stopped.is_set():
                continue
            self._dispatch(delivery)
        self._logger.debug("the delivery loop of consumer %r stopped", self.id)

    def _run_notification_loop(self) -> None:
        """Hand every observed active/standby status to the caller's handler (step_090 §3)."""
        while not self._stopped.is_set():
            try:
                is_active = self._states.get(timeout=DELIVERY_POLL_INTERVAL_SECONDS)
            except Empty:
                continue
            self._notify(is_active)
        self._logger.debug("the notification loop of consumer %r stopped", self.id)

    def _observe_flow_properties(self, properties: Mapping[Any, Any]) -> None:
        """Queue the ``rabbitmq:active`` status carried by one ``flow`` (step_090 §1).

        Runs on the connection's frame-reader thread, so it only parses and
        enqueues; the caller's handler is invoked on this consumer's own
        notification thread instead, exactly as a :data:`MessageHandler` is.

        Args:
            properties: The ``properties`` map of an inbound ``flow``. One without
                ``rabbitmq:active`` — every ``flow`` from a classic or stream
                queue, and most from a quorum one — is dropped.
        """
        if RABBITMQ_ACTIVE_PROPERTY not in properties:
            return
        try:
            is_active = parse_active_flag(properties[RABBITMQ_ACTIVE_PROPERTY])
        except ProtocolError as error:  # an unreadable status is dropped, not raised at the reader
            self._logger.warning("consumer %r ignoring an unusable active flag: %s", self.id, error)
            return
        self._states.put(is_active)

    def _notify(self, is_active: bool) -> None:
        """Report one active/standby status, absorbing whatever the handler raises."""
        handler = self._single_active_consumer_handler
        with self._lock:
            if self._closed or handler is None:
                return
        try:
            handler(self, is_active)
        except Exception:  # a bad handler must not stop the next notification (step_090 §4)
            self._logger.exception("the single-active-consumer handler of consumer %r raised", self.id)

    def _dispatch(self, delivery: Delivery) -> None:
        """Invoke the handler for one delivery, absorbing whatever it raises."""
        context = Context(self, delivery.delivery_id, presettled=self._presettled)
        self._track_stream_offset(delivery.message)
        with self._lock:
            if self._presettled:
                # step_060_consumer_strategy.md §3.2/§3.3: no settlement will ever
                # follow, so the credit is reclaimed at handoff rather than after
                # the handler returns.
                self._replenish_credit()
            else:
                self._unsettled += 1
        try:
            self._handler(context, delivery.message)
        except Exception:  # a bad handler invocation must not stop delivery (§3.2)
            self._logger.exception("the message handler of consumer %r raised", self.id)

    def _settle(self, delivery_id: int, state: DeliveryState) -> None:
        """Send the ``disposition`` for one delivery and replenish its credit (§3.3).

        Raises:
            ConsumerError: If the consumer is closed.
            AMQPError: Whatever failure killed the link, its session or the
                connection.
        """
        with self._lock:
            self._require_open()
            self._link.settle(delivery_id, state)
            self._unsettled = max(0, self._unsettled - 1)
            self._replenish_credit()

    def _replenish_credit(self) -> None:
        """Grant one more credit, keeping the outstanding total at ``initial_credits``.

        A ``flow`` carries the receiver's delivery-count, so re-granting
        ``initial_credits`` against a count that has advanced by one delivery is
        exactly the ``+1`` §3.3 asks for. A paused consumer grants nothing —
        :meth:`unpause` restores the credit in one go instead. Must be called
        with the consumer's lock held.
        """
        if self._paused or self._closed:
            return
        try:
            self._link.flow(self._initial_credits)
        except AMQPError as error:  # the settlement itself succeeded; only credit is lost
            self._logger.warning("consumer %r could not replenish link credit: %s", self.id, error)

    def _join_loops(self) -> None:
        """Wait for both of this consumer's loops to notice they must stop."""
        self._join_loop(self._delivery_loop, "delivery loop")
        self._join_loop(self._notification_loop, "notification loop")

    def _join_loop(self, thread: threading.Thread | None, description: str) -> None:
        """Wait for one loop thread to stop, unless it never started or we are it.

        Args:
            thread: The loop thread, or ``None`` when it was never started.
            description: How to name the loop in the warning a stuck one gets.
        """
        if thread is None or thread is threading.current_thread():
            return
        thread.join(DELIVERY_LOOP_JOIN_TIMEOUT_SECONDS)
        if thread.is_alive():
            self._logger.warning(
                "the %s of consumer %r did not stop within %.1fs",
                description,
                self.id,
                DELIVERY_LOOP_JOIN_TIMEOUT_SECONDS,
            )

    def _require_open(self) -> None:
        """Refuse an operation on a closed consumer (§5).

        Raises:
            ConsumerError: If :meth:`close` already ran.
        """
        with self._lock:
            if self._closed:
                raise ConsumerError(f"consumer {self.id!r} is closed")


class ConsumerBuilder:
    """Chainable builder for one :class:`Consumer` (step_030 §2).

    A fresh builder comes from every
    :meth:`~.connection.Connection.consumer_builder` call and is not reusable
    after :meth:`build`. Both :meth:`queue` and :meth:`message_handler` are
    mandatory — unless :meth:`settle_strategy` is set to
    :attr:`ConsumerSettleStrategy.DIRECT_REPLY_TO`, in which case :meth:`queue`
    must **not** be called (step_060_consumer_strategy.md §2, §5).

    Example:
        >>> consumer = (
        ...     connection.consumer_builder()
        ...     .queue("orders")
        ...     .initial_credits(20)
        ...     .message_handler(lambda context, message: context.accept())
        ...     .build()
        ... )
    """

    def __init__(self, connection: Connection) -> None:
        """Create a builder bound to ``connection``.

        Args:
            connection: Connection whose shared pub/sub session hosts the link.
        """
        self._connection = connection
        self._queue: str | None = None
        self._handler: MessageHandler | None = None
        self._initial_credits = DEFAULT_INITIAL_CREDITS
        self._settle_strategy = ConsumerSettleStrategy.EXPLICIT_SETTLE
        self._single_active_consumer_handler: SingleActiveConsumerStateHandler | None = None
        self._stream = StreamConfiguration()

    # --- setters --------------------------------------------------------

    def queue(self, queue: str | QueueSpecification) -> ConsumerBuilder:
        """Set the queue to consume from; mandatory.

        Args:
            queue: Queue name, or a :class:`~.management.QueueSpecification`
                whose name is read. A specification is never declared here.

        Returns:
            This builder.

        Raises:
            InvalidAddressError: If the name is empty.
        """
        name = queue.queue_name if isinstance(queue, QueueSpecification) else queue
        if not name:
            raise InvalidAddressError("a non-empty queue name is required")
        self._queue = name
        return self

    def message_handler(self, handler: MessageHandler) -> ConsumerBuilder:
        """Set the callback invoked once per delivery; mandatory.

        Args:
            handler: Called as ``handler(context, message)`` on the consumer's
                own delivery thread. It should settle the delivery through the
                context unless the consumer is presettled, and it must catch its
                own exceptions: anything escaping it is only logged (§3.2).

        Returns:
            This builder.
        """
        self._handler = handler
        return self

    def initial_credits(self, credits: int) -> ConsumerBuilder:
        """Set the link credit granted at attach, and kept topped up to (§3.3).

        Args:
            credits: Credit to grant; also the ceiling on how many deliveries can
                be unsettled at once. Defaults to
                :data:`DEFAULT_INITIAL_CREDITS`.

        Returns:
            This builder.

        Raises:
            ValidationError: If ``credits`` is not > 0 — a consumer that grants
                no credit would never receive anything; use
                :meth:`Consumer.pause` for that.
        """
        if credits <= 0:
            raise ValidationError(f"initial credits must be > 0, got {credits}")
        self._initial_credits = credits
        return self

    def settle_strategy(self, strategy: ConsumerSettleStrategy) -> ConsumerBuilder:
        """Select how deliveries on the built consumer are settled (step_060_consumer_strategy.md §1-§2).

        Args:
            strategy: ``EXPLICIT_SETTLE`` (default): every delivery is settled via
                :class:`Context`. ``PRESETTLED``: the broker settles every
                delivery itself; every :class:`Context` method raises
                :class:`~.exceptions.ConsumerError` (§3.2). ``DIRECT_REPLY_TO``:
                attaches to RabbitMQ's direct-reply-to pseudo-queue instead of a
                named queue — mutually exclusive with :meth:`queue` and with
                :meth:`QuorumConsumerOptions.single_active_consumer_state_changed`
                (§3.3, §5).

        Returns:
            This builder.
        """
        self._settle_strategy = strategy
        return self

    # --- type-specific sub-builders -------------------------------------

    def quorum(self) -> QuorumConsumerOptions:
        """Return the options only a quorum queue offers (step_090 §2).

        Returns:
            A view over this builder; :meth:`QuorumConsumerOptions.builder`
            returns here to resume chaining.

        Raises:
            ConsumerError: If no queue has been set yet — these options describe
                how to consume from an already-chosen queue.
        """
        if self._queue is None:
            raise ConsumerError("quorum consumer options need a queue: call queue() before quorum()")
        return QuorumConsumerOptions(self)

    def stream(self) -> StreamOptions:
        """Return the options only a stream offers (step_080 §1).

        Returns:
            A view over this builder; :meth:`StreamOptions.builder` returns here
            to resume chaining. Calling it twice hands out two views over the same
            configuration, so a filter set can be built up in several steps.

        Raises:
            ConsumerError: If no queue has been set yet — these options describe
                how to consume from an already-chosen queue.
        """
        if self._queue is None:
            raise ConsumerError("stream consumer options need a queue: call queue() before stream()")
        return StreamOptions(self)

    # --- terminal operations --------------------------------------------

    def build(self) -> Consumer:
        """Attach the receiver link, start the delivery loop, return the consumer.

        Opens the connection's shared pub/sub session when this is the first
        consumer (or publisher) built on it, and reuses it afterwards.

        Returns:
            The consumer, already receiving.

        Raises:
            ConsumerError: If no queue or no message handler was set (unless
                ``settle_strategy(DIRECT_REPLY_TO)``, which forbids a queue), if
                ``DIRECT_REPLY_TO`` is combined with ``queue(...)`` or with
                ``quorum().single_active_consumer_state_changed(...)``
                (step_060_consumer_strategy.md §5), if the stream offset
                specification is invalid (step_080 §1.1), or if the broker
                refuses the queue.
            AMQPTimeoutError: If the broker does not answer ``begin``/``attach``.
        """
        strategy = self._settle_strategy
        queue = self._queue
        if strategy is ConsumerSettleStrategy.DIRECT_REPLY_TO:
            if queue is not None:
                raise ConsumerError(
                    "settle_strategy(DIRECT_REPLY_TO) cannot be combined with queue(): "
                    "the pseudo-queue's address is broker-generated, not caller-supplied"
                )
            if self._single_active_consumer_handler is not None:
                raise ConsumerError(
                    "settle_strategy(DIRECT_REPLY_TO) cannot be combined with "
                    "quorum().single_active_consumer_state_changed(): the pseudo-queue is never "
                    "single-active-consumer-enabled"
                )
        elif queue is None:
            raise ConsumerError("a consumer needs a queue: call queue() before build()")
        handler = self._handler
        if handler is None:
            raise ConsumerError("a consumer needs a message handler: call message_handler() before build()")
        # Built before the session is opened, so an invalid offset specification
        # costs no I/O at all (step_080 §4).
        stream_filter = stream_filter_set(self._stream)
        session = self._connection._pub_sub_session()
        consumer = Consumer(
            self._connection,
            session,
            queue,
            handler,
            initial_credits=self._initial_credits,
            settle_strategy=strategy,
            single_active_consumer_handler=self._single_active_consumer_handler,
            stream_filter=stream_filter,
        )
        consumer.open()
        return consumer


class QuorumConsumerOptions:
    """Quorum-queue-only consumer options, as a view over the parent builder (step_090 §2).

    Reached through :meth:`ConsumerBuilder.quorum` once the queue is set. Nothing
    here changes the ``attach``: the broker sends ``rabbitmq:active`` on its own
    for a queue declared with ``single_active_consumer(True)``, and these options
    only decide whether the client routes it anywhere. Using them against a
    classic or stream queue is harmless — no such ``flow`` ever arrives, so the
    handler is simply never called.

    Example:
        >>> consumer = (
        ...     connection.consumer_builder()
        ...     .queue("orders")
        ...     .quorum()
        ...     .single_active_consumer_state_changed(lambda consumer, is_active: print(is_active))
        ...     .builder()
        ...     .message_handler(lambda context, message: context.accept())
        ...     .build()
        ... )
    """

    def __init__(self, parent: ConsumerBuilder) -> None:
        """Wrap ``parent``; every setter here writes to that same builder."""
        self._parent = parent

    def builder(self) -> ConsumerBuilder:
        """Return the parent builder, to resume chaining or call ``build()``."""
        return self._parent

    def single_active_consumer_state_changed(self, handler: SingleActiveConsumerStateHandler) -> QuorumConsumerOptions:
        """Register the callback that learns whether this consumer is active (step_090 §2.1).

        Args:
            handler: Called as ``handler(consumer, is_active)`` on the consumer's
                own notification thread, once per ``rabbitmq:active`` property the
                broker sends — starting with the status the link is given at
                attach, which is reported even when it arrives before
                :meth:`ConsumerBuilder.build` returns. It must catch its own
                exceptions: anything escaping it is only logged (step_090 §4).
                Calling this again replaces the previously registered handler.

        Returns:
            This view.
        """
        self._parent._single_active_consumer_handler = handler
        return self


class StreamOptions:
    """Stream-only consumer options, as a view over the parent builder (step_080 §1).

    Reached through :meth:`ConsumerBuilder.stream` once the queue is set. Every
    setter here only adds an entry to the receiver link's ``source.filter`` map,
    which is sent with the one ``attach`` :meth:`ConsumerBuilder.build` performs;
    the broker requires all of those entries to match for a message to be
    delivered, so the bloom filter (§2) and the AMQP filter expressions (§3) may
    legitimately be combined. Using these options against a queue that is not a
    stream is not validated here — what the broker makes of them is its own
    business (§5).

    Example:
        >>> consumer = (
        ...     connection.consumer_builder()
        ...     .queue("events")
        ...     .stream()
        ...     .offset(StreamOffsetSpecification.FIRST)
        ...     .filter_values("emea")
        ...     .filter_match_unfiltered(True)
        ...     .builder()
        ...     .message_handler(lambda context, message: context.accept())
        ...     .build()
        ... )
    """

    def __init__(self, parent: ConsumerBuilder) -> None:
        """Wrap ``parent``; every setter here writes to that same builder."""
        self._parent = parent

    def builder(self) -> ConsumerBuilder:
        """Return the parent builder, to resume chaining or call ``build()``."""
        return self._parent

    def offset(self, offset: StreamOffset) -> StreamOptions:
        """Set where in the stream to start reading (step_080 §1.1).

        Args:
            offset: A :class:`StreamOffsetSpecification` naming a position, an
                absolute stream offset, a :class:`~datetime.datetime` to start at
                (a naive one is read as local time), or a relative interval such
                as ``"7D"`` or ``"12h"``. Calling this again overwrites the
                previous value; never calling it leaves the choice to the broker,
                which starts at :attr:`StreamOffsetSpecification.NEXT`.

        Returns:
            This view.
        """
        self._parent._stream.offset = offset
        return self

    def filter_values(self, *values: str) -> StreamOptions:
        """Ask for messages tagged with any of ``values`` (step_080 §2.2).

        The broker uses each stream segment's bloom filter to skip segments that
        cannot match, so this is cheap but approximate: a non-matching message may
        still be delivered, but a matching one is never held back. Callers needing
        exactness must re-check each delivery themselves, or use :meth:`filter`
        instead, which the broker evaluates precisely.

        Args:
            *values: Values a publisher put in a message's
                ``x-stream-filter-value`` annotation (step_080 §2.1),
                OR-combined by the broker. Calling this again replaces the whole
                set; calling it with no values at all asks for the empty set,
                which matches no tagged message.

        Returns:
            This view.
        """
        self._parent._stream.filter_values = values
        return self

    def filter_match_unfiltered(self, match_unfiltered: bool = True) -> StreamOptions:
        """Also receive messages carrying no filter value at all (step_080 §2.2).

        Args:
            match_unfiltered: ``True`` delivers untagged messages in addition to
                whatever :meth:`filter_values` matches; ``False``, the broker's
                default, excludes them. Setting it without any
                :meth:`filter_values` is a legitimate no-op filter that delivers
                everything, and is not refused here.

        Returns:
            This view.
        """
        self._parent._stream.match_unfiltered = match_unfiltered
        return self

    def filter(self) -> StreamFilterOptions:
        """Return the AMQP filter-expression options (step_080 §3, RabbitMQ 4.1+).

        Returns:
            A view over the same configuration;
            :meth:`StreamFilterOptions.stream` returns here.
        """
        return StreamFilterOptions(self)


class StreamFilterOptions:
    """AMQP filter expressions for a stream consumer (step_080 §3).

    Reached through :meth:`StreamOptions.filter`, and a view over the same
    :class:`ConsumerBuilder`. Unlike the bloom filter, these are evaluated exactly
    by the broker against the message's own ``properties`` and
    ``application-properties`` sections, so nothing has to be tagged when
    publishing. :meth:`sql` needs RabbitMQ 4.2+. The broker ANDs every filter
    entry together, so :meth:`sql` can be combined with :meth:`subject`/
    :meth:`property` — unusual, and mostly useful for testing/demonstration,
    but not refused.

    Example:
        >>> consumer = (
        ...     connection.consumer_builder()
        ...     .queue("events")
        ...     .stream()
        ...     .offset(StreamOffsetSpecification.FIRST)
        ...     .filter()
        ...     .subject("orders")
        ...     .property("region", "emea")
        ...     .stream()
        ...     .builder()
        ...     .message_handler(lambda context, message: context.accept())
        ...     .build()
        ... )
    """

    def __init__(self, parent: StreamOptions) -> None:
        """Wrap ``parent``; every setter here writes to the same configuration."""
        self._parent = parent
        self._configuration = parent.builder()._stream

    def stream(self) -> StreamOptions:
        """Return the parent stream options, to resume chaining or call ``builder()``."""
        return self._parent

    def subject(self, subject: str) -> StreamFilterOptions:
        """Match the message's ``properties.subject`` against ``subject``.

        Args:
            subject: The value to match. ``amqp:properties-filter`` has only this
                one meaningful field, so calling this again overwrites the
                previous value rather than adding anything.

        Returns:
            This view.
        """
        self._configuration.subject = subject
        return self

    def property(self, key: str, value: Any) -> StreamFilterOptions:
        """Match one of the message's application properties.

        Args:
            key: Application-property name to match on.
            value: The value it must have. Calling this with a new key adds
                another entry to ``amqp:application-properties-filter``, all of
                which must match; calling it with a key already set overwrites
                just that entry.

        Returns:
            This view.
        """
        self._configuration.properties[key] = value
        return self

    def sql(self, expression: str) -> StreamFilterOptions:
        """Have the broker evaluate one boolean expression per message (RabbitMQ 4.2+).

        Args:
            expression: An AMQP SQL filter expression over ``properties.*`` and
                application properties, e.g.
                ``"properties.subject LIKE 'orders%' AND region = 'emea'"``.
                Calling this again overwrites the previous expression. A broker
                older than 4.2 does not know this filter and, rather than
                refusing the attach, ignores it — the consumer then receives
                everything.

        Returns:
            This view.
        """
        self._configuration.sql = expression
        return self


__all__ = [
    "ANNOTATION_KEY_PREFIX",
    "CONSUMER_LINK_PREFIX",
    "DEFAULT_INITIAL_CREDITS",
    "STREAM_INTERVAL_PATTERN",
    "SUBJECT_FILTER_FIELD",
    "Consumer",
    "ConsumerBuilder",
    "ConsumerSettleStrategy",
    "Context",
    "MessageHandler",
    "QuorumConsumerOptions",
    "SingleActiveConsumerStateHandler",
    "StreamConfiguration",
    "StreamFilterOptions",
    "StreamOffset",
    "StreamOffsetSpecification",
    "StreamOptions",
    "parse_active_flag",
    "stream_filter_set",
    "stream_offset_filter_value",
    "stream_offset_of",
]
