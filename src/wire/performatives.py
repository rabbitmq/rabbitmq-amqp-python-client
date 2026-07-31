"""AMQP 1.0 performatives and the ``source``/``target`` terminus types.

Every performative is a described list (§2.7) whose fields are positional. The
``encode()`` methods below return only the described-list body: the enclosing
frame header is added by :mod:`.frames`. Fields equal to their spec default are
passed as ``None`` to :func:`~.encoding.encode_described_list`, which drops a
trailing run of them and writes ``null`` for the rest — this is legal because a
``null`` field and an omitted field both mean "use the default".
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from ..exceptions import ProtocolError
from .delivery_state import DeliveryState, Error, decode_delivery_state
from .encoding import (
    Decoder,
    Described,
    as_dict,
    as_symbol_list,
    descriptor_code,
    encode_binary,
    encode_boolean,
    encode_described_list,
    encode_map_of_encoded,
    encode_string,
    encode_symbol,
    encode_symbol_array,
    encode_symbol_map,
    encode_ubyte,
    encode_uint,
    encode_ulong,
    encode_ushort,
    field_at,
    read_described_list,
)

DESCRIPTOR_OPEN = 0x10
DESCRIPTOR_BEGIN = 0x11
DESCRIPTOR_ATTACH = 0x12
DESCRIPTOR_FLOW = 0x13
DESCRIPTOR_TRANSFER = 0x14
DESCRIPTOR_DISPOSITION = 0x15
DESCRIPTOR_DETACH = 0x16
DESCRIPTOR_END = 0x17
DESCRIPTOR_CLOSE = 0x18
DESCRIPTOR_SOURCE = 0x28
DESCRIPTOR_TARGET = 0x29

SYMBOLIC_DESCRIPTORS: dict[str, int] = {
    "amqp:open:list": DESCRIPTOR_OPEN,
    "amqp:begin:list": DESCRIPTOR_BEGIN,
    "amqp:attach:list": DESCRIPTOR_ATTACH,
    "amqp:flow:list": DESCRIPTOR_FLOW,
    "amqp:transfer:list": DESCRIPTOR_TRANSFER,
    "amqp:disposition:list": DESCRIPTOR_DISPOSITION,
    "amqp:detach:list": DESCRIPTOR_DETACH,
    "amqp:end:list": DESCRIPTOR_END,
    "amqp:close:list": DESCRIPTOR_CLOSE,
    "amqp:source:list": DESCRIPTOR_SOURCE,
    "amqp:target:list": DESCRIPTOR_TARGET,
}

MAX_UINT = 0xFFFFFFFF
MAX_USHORT = 0xFFFF

ROLE_SENDER = False
ROLE_RECEIVER = True

SND_SETTLE_MODE_UNSETTLED = 0
SND_SETTLE_MODE_SETTLED = 1
SND_SETTLE_MODE_MIXED = 2

RCV_SETTLE_MODE_FIRST = 0
RCV_SETTLE_MODE_SECOND = 1

TERMINUS_DURABILITY_NONE = 0
TERMINUS_DURABILITY_CONFIGURATION = 1
TERMINUS_DURABILITY_UNSETTLED_STATE = 2

EXPIRY_POLICY_LINK_DETACH = "link-detach"
EXPIRY_POLICY_SESSION_END = "session-end"
EXPIRY_POLICY_CONNECTION_CLOSE = "connection-close"
EXPIRY_POLICY_NEVER = "never"

DISTRIBUTION_MODE_MOVE = "move"
DISTRIBUTION_MODE_COPY = "copy"


def _encode_string_field(value: str | None) -> bytes | None:
    return None if value is None else encode_string(value)


@dataclass
class Source:
    """The ``source`` terminus of a link (descriptor ``0x28``).

    Attributes:
        address: Node the source refers to.
        durable: Terminus durability (0 none, 1 configuration, 2 unsettled-state).
        expiry_policy: When the terminus expires.
        timeout: Expiry timeout in seconds.
        dynamic: Whether the peer should create a node on demand.
        dynamic_node_properties: Properties for the dynamically created node.
        distribution_mode: ``move`` or ``copy`` for message distribution.
        filter: Filter set, e.g. RabbitMQ stream offset/filter specifications.
        default_outcome: Outcome applied to unsettled deliveries by default.
        outcomes: Outcome symbols the sender supports.
        capabilities: Extension capabilities of the terminus.
    """

    address: str | None = None
    durable: int = TERMINUS_DURABILITY_NONE
    expiry_policy: str = EXPIRY_POLICY_SESSION_END
    timeout: int = 0
    dynamic: bool = False
    dynamic_node_properties: dict[Any, Any] | None = None
    distribution_mode: str | None = None
    filter: dict[Any, Any] | None = None
    default_outcome: DeliveryState | None = None
    outcomes: list[str] | None = None
    capabilities: list[str] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_SOURCE

    def encode(self) -> bytes:
        """Encode the terminus as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                _encode_string_field(self.address),
                None if self.durable == TERMINUS_DURABILITY_NONE else encode_uint(self.durable),
                None if self.expiry_policy == EXPIRY_POLICY_SESSION_END else encode_symbol(self.expiry_policy),
                None if self.timeout == 0 else encode_uint(self.timeout),
                encode_boolean(True) if self.dynamic else None,
                None if self.dynamic_node_properties is None else encode_symbol_map(self.dynamic_node_properties),
                None if self.distribution_mode is None else encode_symbol(self.distribution_mode),
                None if self.filter is None else encode_symbol_map(self.filter),
                _encode_outcome(self.default_outcome),
                None if self.outcomes is None else encode_symbol_array(self.outcomes),
                None if self.capabilities is None else encode_symbol_array(self.capabilities),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Source:
        """Build a source from the decoded fields of its described list."""
        raw_default_outcome = field_at(values, 8)
        return cls(
            address=_optional_str(field_at(values, 0)),
            durable=int(field_at(values, 1, TERMINUS_DURABILITY_NONE)),
            expiry_policy=str(field_at(values, 2, EXPIRY_POLICY_SESSION_END)),
            timeout=int(field_at(values, 3, 0)),
            dynamic=bool(field_at(values, 4, False)),
            dynamic_node_properties=as_dict(field_at(values, 5)),
            distribution_mode=_optional_str(field_at(values, 6)),
            filter=as_dict(field_at(values, 7)),
            default_outcome=None if raw_default_outcome is None else decode_delivery_state(raw_default_outcome),
            outcomes=as_symbol_list(field_at(values, 9)),
            capabilities=as_symbol_list(field_at(values, 10)),
        )

    @classmethod
    def decode(cls, data: bytes | Described) -> Source:
        """Decode a source from encoded bytes or an already-decoded described value."""
        return cls.from_fields(_terminus_fields(DESCRIPTOR_SOURCE, data))


@dataclass
class Target:
    """The ``target`` terminus of a link (descriptor ``0x29``).

    Attributes:
        address: Node the target refers to.
        durable: Terminus durability (0 none, 1 configuration, 2 unsettled-state).
        expiry_policy: When the terminus expires.
        timeout: Expiry timeout in seconds.
        dynamic: Whether the peer should create a node on demand.
        dynamic_node_properties: Properties for the dynamically created node.
        capabilities: Extension capabilities of the terminus.
    """

    address: str | None = None
    durable: int = TERMINUS_DURABILITY_NONE
    expiry_policy: str = EXPIRY_POLICY_SESSION_END
    timeout: int = 0
    dynamic: bool = False
    dynamic_node_properties: dict[Any, Any] | None = None
    capabilities: list[str] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_TARGET

    def encode(self) -> bytes:
        """Encode the terminus as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                _encode_string_field(self.address),
                None if self.durable == TERMINUS_DURABILITY_NONE else encode_uint(self.durable),
                None if self.expiry_policy == EXPIRY_POLICY_SESSION_END else encode_symbol(self.expiry_policy),
                None if self.timeout == 0 else encode_uint(self.timeout),
                encode_boolean(True) if self.dynamic else None,
                None if self.dynamic_node_properties is None else encode_symbol_map(self.dynamic_node_properties),
                None if self.capabilities is None else encode_symbol_array(self.capabilities),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Target:
        """Build a target from the decoded fields of its described list."""
        return cls(
            address=_optional_str(field_at(values, 0)),
            durable=int(field_at(values, 1, TERMINUS_DURABILITY_NONE)),
            expiry_policy=str(field_at(values, 2, EXPIRY_POLICY_SESSION_END)),
            timeout=int(field_at(values, 3, 0)),
            dynamic=bool(field_at(values, 4, False)),
            dynamic_node_properties=as_dict(field_at(values, 5)),
            capabilities=as_symbol_list(field_at(values, 6)),
        )

    @classmethod
    def decode(cls, data: bytes | Described) -> Target:
        """Decode a target from encoded bytes or an already-decoded described value."""
        return cls.from_fields(_terminus_fields(DESCRIPTOR_TARGET, data))


@dataclass
class Open:
    """The ``open`` performative (descriptor ``0x10``): connection negotiation.

    Attributes:
        container_id: Unique name of this container.
        hostname: Host the peer should route this connection to.
        max_frame_size: Largest frame this peer will accept.
        channel_max: Highest channel number this peer will accept.
        idle_time_out: Inactivity period, in milliseconds, after which this peer
            considers the connection dead.
        outgoing_locales: Locales this peer sends descriptions in.
        incoming_locales: Locales this peer accepts descriptions in.
        offered_capabilities: Extension capabilities this peer supports.
        desired_capabilities: Extension capabilities this peer wants to use.
        properties: Informational connection properties.
    """

    container_id: str
    hostname: str | None = None
    max_frame_size: int = MAX_UINT
    channel_max: int = MAX_USHORT
    idle_time_out: int | None = None
    outgoing_locales: list[str] | None = None
    incoming_locales: list[str] | None = None
    offered_capabilities: list[str] | None = None
    desired_capabilities: list[str] | None = None
    properties: dict[Any, Any] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_OPEN

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                _encode_string_field(self.container_id),
                _encode_string_field(self.hostname),
                None if self.max_frame_size == MAX_UINT else encode_uint(self.max_frame_size),
                None if self.channel_max == MAX_USHORT else encode_ushort(self.channel_max),
                None if self.idle_time_out is None else encode_uint(self.idle_time_out),
                None if self.outgoing_locales is None else encode_symbol_array(self.outgoing_locales),
                None if self.incoming_locales is None else encode_symbol_array(self.incoming_locales),
                None if self.offered_capabilities is None else encode_symbol_array(self.offered_capabilities),
                None if self.desired_capabilities is None else encode_symbol_array(self.desired_capabilities),
                None if self.properties is None else encode_symbol_map(self.properties),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Open:
        """Build the performative from the decoded fields of its described list."""
        container_id = field_at(values, 0)
        if container_id is None:
            raise ProtocolError("open is missing its mandatory container-id field")
        return cls(
            container_id=str(container_id),
            hostname=_optional_str(field_at(values, 1)),
            max_frame_size=int(field_at(values, 2, MAX_UINT)),
            channel_max=int(field_at(values, 3, MAX_USHORT)),
            idle_time_out=_optional_int(field_at(values, 4)),
            outgoing_locales=as_symbol_list(field_at(values, 5)),
            incoming_locales=as_symbol_list(field_at(values, 6)),
            offered_capabilities=as_symbol_list(field_at(values, 7)),
            desired_capabilities=as_symbol_list(field_at(values, 8)),
            properties=as_dict(field_at(values, 9)),
        )

    @classmethod
    def decode(cls, data: bytes) -> Open:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_OPEN, data))


@dataclass
class Begin:
    """The ``begin`` performative (descriptor ``0x11``): session negotiation.

    Attributes:
        next_outgoing_id: First transfer-id this endpoint will assign.
        incoming_window: Transfer-ids this endpoint can currently receive.
        outgoing_window: Transfer-ids this endpoint can currently send.
        remote_channel: Channel of the ``begin`` being replied to; ``None`` when initiating.
        handle_max: Highest link handle this endpoint will accept.
        offered_capabilities: Extension capabilities this endpoint supports.
        desired_capabilities: Extension capabilities this endpoint wants to use.
        properties: Informational session properties.
    """

    next_outgoing_id: int
    incoming_window: int
    outgoing_window: int
    remote_channel: int | None = None
    handle_max: int = MAX_UINT
    offered_capabilities: list[str] | None = None
    desired_capabilities: list[str] | None = None
    properties: dict[Any, Any] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_BEGIN

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                None if self.remote_channel is None else encode_ushort(self.remote_channel),
                encode_uint(self.next_outgoing_id),
                encode_uint(self.incoming_window),
                encode_uint(self.outgoing_window),
                None if self.handle_max == MAX_UINT else encode_uint(self.handle_max),
                None if self.offered_capabilities is None else encode_symbol_array(self.offered_capabilities),
                None if self.desired_capabilities is None else encode_symbol_array(self.desired_capabilities),
                None if self.properties is None else encode_symbol_map(self.properties),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Begin:
        """Build the performative from the decoded fields of its described list."""
        return cls(
            remote_channel=_optional_int(field_at(values, 0)),
            next_outgoing_id=int(field_at(values, 1, 0)),
            incoming_window=int(field_at(values, 2, 0)),
            outgoing_window=int(field_at(values, 3, 0)),
            handle_max=int(field_at(values, 4, MAX_UINT)),
            offered_capabilities=as_symbol_list(field_at(values, 5)),
            desired_capabilities=as_symbol_list(field_at(values, 6)),
            properties=as_dict(field_at(values, 7)),
        )

    @classmethod
    def decode(cls, data: bytes) -> Begin:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_BEGIN, data))


@dataclass
class Attach:
    """The ``attach`` performative (descriptor ``0x12``): link negotiation.

    Attributes:
        name: Link name, shared by both peers' ``attach``.
        handle: Session-local link handle chosen by the sender of this ``attach``.
        role: ``False`` for sender, ``True`` for receiver.
        snd_settle_mode: 0 unsettled, 1 settled, 2 mixed.
        rcv_settle_mode: 0 first, 1 second.
        source: Origin terminus, or ``None`` when refusing that end.
        target: Destination terminus, or ``None`` when refusing that end.
        unsettled: Delivery-tag to delivery-state map, for link recovery.
        incomplete_unsettled: Whether ``unsettled`` is a partial map.
        initial_delivery_count: Mandatory when ``role`` is sender.
        max_message_size: Largest message accepted; ``None``/0 means no limit.
        offered_capabilities: Extension capabilities this endpoint supports.
        desired_capabilities: Extension capabilities this endpoint wants to use.
        properties: Informational link properties.
    """

    name: str
    handle: int
    role: bool
    snd_settle_mode: int = SND_SETTLE_MODE_MIXED
    rcv_settle_mode: int = RCV_SETTLE_MODE_FIRST
    source: Source | None = None
    target: Target | None = None
    unsettled: dict[bytes, DeliveryState] | None = None
    incomplete_unsettled: bool = False
    initial_delivery_count: int | None = None
    max_message_size: int | None = None
    offered_capabilities: list[str] | None = None
    desired_capabilities: list[str] | None = None
    properties: dict[Any, Any] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_ATTACH

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                _encode_string_field(self.name),
                encode_uint(self.handle),
                encode_boolean(self.role),
                None if self.snd_settle_mode == SND_SETTLE_MODE_MIXED else encode_ubyte(self.snd_settle_mode),
                None if self.rcv_settle_mode == RCV_SETTLE_MODE_FIRST else encode_ubyte(self.rcv_settle_mode),
                None if self.source is None else self.source.encode(),
                None if self.target is None else self.target.encode(),
                None if self.unsettled is None else _encode_unsettled(self.unsettled),
                encode_boolean(True) if self.incomplete_unsettled else None,
                None if self.initial_delivery_count is None else encode_uint(self.initial_delivery_count),
                None if self.max_message_size is None else encode_ulong(self.max_message_size),
                None if self.offered_capabilities is None else encode_symbol_array(self.offered_capabilities),
                None if self.desired_capabilities is None else encode_symbol_array(self.desired_capabilities),
                None if self.properties is None else encode_symbol_map(self.properties),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Attach:
        """Build the performative from the decoded fields of its described list."""
        name = field_at(values, 0)
        if name is None:
            raise ProtocolError("attach is missing its mandatory name field")
        raw_source = field_at(values, 5)
        raw_target = field_at(values, 6)
        return cls(
            name=str(name),
            handle=int(field_at(values, 1, 0)),
            role=bool(field_at(values, 2, ROLE_SENDER)),
            snd_settle_mode=int(field_at(values, 3, SND_SETTLE_MODE_MIXED)),
            rcv_settle_mode=int(field_at(values, 4, RCV_SETTLE_MODE_FIRST)),
            source=None if raw_source is None else Source.decode(raw_source),
            target=None if raw_target is None else Target.decode(raw_target),
            unsettled=_decode_unsettled(field_at(values, 7)),
            incomplete_unsettled=bool(field_at(values, 8, False)),
            initial_delivery_count=_optional_int(field_at(values, 9)),
            max_message_size=_optional_int(field_at(values, 10)),
            offered_capabilities=as_symbol_list(field_at(values, 11)),
            desired_capabilities=as_symbol_list(field_at(values, 12)),
            properties=as_dict(field_at(values, 13)),
        )

    @classmethod
    def decode(cls, data: bytes) -> Attach:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_ATTACH, data))


@dataclass
class Flow:
    """The ``flow`` performative (descriptor ``0x13``): session and link flow control.

    Attributes:
        incoming_window: Transfer-ids this endpoint can currently receive.
        next_outgoing_id: Next transfer-id this endpoint will assign.
        outgoing_window: Transfer-ids this endpoint can currently send.
        next_incoming_id: Next expected incoming transfer-id; required once any
            transfer has been received.
        handle: Link this flow refers to; ``None`` for a session-only flow.
        delivery_count: Sender's delivery-count; required when ``handle`` is set.
        link_credit: Credit granted to the sender; required when ``handle`` is set.
        available: Messages the sender has ready to send.
        drain: Whether the sender should consume all credit and stop.
        echo: Whether the peer should reply with its own ``flow``.
        properties: Informational flow properties.
    """

    incoming_window: int
    next_outgoing_id: int
    outgoing_window: int
    next_incoming_id: int | None = None
    handle: int | None = None
    delivery_count: int | None = None
    link_credit: int | None = None
    available: int | None = None
    drain: bool = False
    echo: bool = False
    properties: dict[Any, Any] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_FLOW

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                None if self.next_incoming_id is None else encode_uint(self.next_incoming_id),
                encode_uint(self.incoming_window),
                encode_uint(self.next_outgoing_id),
                encode_uint(self.outgoing_window),
                None if self.handle is None else encode_uint(self.handle),
                None if self.delivery_count is None else encode_uint(self.delivery_count),
                None if self.link_credit is None else encode_uint(self.link_credit),
                None if self.available is None else encode_uint(self.available),
                encode_boolean(True) if self.drain else None,
                encode_boolean(True) if self.echo else None,
                None if self.properties is None else encode_symbol_map(self.properties),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Flow:
        """Build the performative from the decoded fields of its described list."""
        return cls(
            next_incoming_id=_optional_int(field_at(values, 0)),
            incoming_window=int(field_at(values, 1, 0)),
            next_outgoing_id=int(field_at(values, 2, 0)),
            outgoing_window=int(field_at(values, 3, 0)),
            handle=_optional_int(field_at(values, 4)),
            delivery_count=_optional_int(field_at(values, 5)),
            link_credit=_optional_int(field_at(values, 6)),
            available=_optional_int(field_at(values, 7)),
            drain=bool(field_at(values, 8, False)),
            echo=bool(field_at(values, 9, False)),
            properties=as_dict(field_at(values, 10)),
        )

    @classmethod
    def decode(cls, data: bytes) -> Flow:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_FLOW, data))


@dataclass
class Transfer:
    """The ``transfer`` performative (descriptor ``0x14``): one message fragment.

    The message bytes themselves are the frame payload that follows this
    performative and are handled by :mod:`.frames`, not by this dataclass.

    Attributes:
        handle: Link this transfer is on.
        delivery_id: Session-scoped delivery id; absent on continuation frames.
        delivery_tag: Link-scoped delivery tag; required on the first frame.
        message_format: Message encoding format version.
        settled: Whether the sender considers the delivery already settled.
        more: Whether more transfer frames follow for this delivery.
        rcv_settle_mode: Per-delivery receiver settlement mode override.
        state: Sender's delivery state, used when resuming.
        resume: Whether this transfer resumes a previously suspended delivery.
        aborted: Whether the receiver must discard the partial delivery.
        batchable: Whether the sender allows the peer to batch its disposition.
    """

    handle: int
    delivery_id: int | None = None
    delivery_tag: bytes | None = None
    message_format: int = 0
    settled: bool | None = None
    more: bool = False
    rcv_settle_mode: int | None = None
    state: DeliveryState | None = None
    resume: bool = False
    aborted: bool = False
    batchable: bool = False

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_TRANSFER

    def encode(self) -> bytes:
        """Encode the performative as a described list, without any payload."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_uint(self.handle),
                None if self.delivery_id is None else encode_uint(self.delivery_id),
                None if self.delivery_tag is None else encode_binary(self.delivery_tag),
                None if self.message_format == 0 else encode_uint(self.message_format),
                None if self.settled is None else encode_boolean(self.settled),
                encode_boolean(True) if self.more else None,
                None if self.rcv_settle_mode is None else encode_ubyte(self.rcv_settle_mode),
                None if self.state is None else self.state.encode(),
                encode_boolean(True) if self.resume else None,
                encode_boolean(True) if self.aborted else None,
                encode_boolean(True) if self.batchable else None,
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Transfer:
        """Build the performative from the decoded fields of its described list."""
        raw_state = field_at(values, 7)
        settled = field_at(values, 4)
        return cls(
            handle=int(field_at(values, 0, 0)),
            delivery_id=_optional_int(field_at(values, 1)),
            delivery_tag=_optional_bytes(field_at(values, 2)),
            message_format=int(field_at(values, 3, 0)),
            settled=None if settled is None else bool(settled),
            more=bool(field_at(values, 5, False)),
            rcv_settle_mode=_optional_int(field_at(values, 6)),
            state=None if raw_state is None else decode_delivery_state(raw_state),
            resume=bool(field_at(values, 8, False)),
            aborted=bool(field_at(values, 9, False)),
            batchable=bool(field_at(values, 10, False)),
        )

    @classmethod
    def decode(cls, data: bytes) -> Transfer:
        """Decode the performative from encoded bytes, ignoring any trailing payload."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_TRANSFER, data))


@dataclass
class Disposition:
    """The ``disposition`` performative (descriptor ``0x15``): settlement of a delivery range.

    Attributes:
        role: ``True`` when the receiver is reporting.
        first: First delivery-id in the range.
        last: Last delivery-id in the range; defaults to ``first``.
        settled: Whether this endpoint now considers the range settled.
        state: Outcome applied to the range.
        batchable: Whether the peer may batch its reply.
    """

    role: bool
    first: int
    last: int | None = None
    settled: bool = False
    state: DeliveryState | None = None
    batchable: bool = False

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_DISPOSITION

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_boolean(self.role),
                encode_uint(self.first),
                None if self.last is None else encode_uint(self.last),
                encode_boolean(True) if self.settled else None,
                None if self.state is None else self.state.encode(),
                encode_boolean(True) if self.batchable else None,
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Disposition:
        """Build the performative from the decoded fields of its described list."""
        raw_state = field_at(values, 4)
        return cls(
            role=bool(field_at(values, 0, ROLE_SENDER)),
            first=int(field_at(values, 1, 0)),
            last=_optional_int(field_at(values, 2)),
            settled=bool(field_at(values, 3, False)),
            state=None if raw_state is None else decode_delivery_state(raw_state),
            batchable=bool(field_at(values, 5, False)),
        )

    @classmethod
    def decode(cls, data: bytes) -> Disposition:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_DISPOSITION, data))


@dataclass
class Detach:
    """The ``detach`` performative (descriptor ``0x16``): closes or suspends a link.

    Attributes:
        handle: Link handle being detached.
        closed: ``True`` when the link is permanently closed, not suspended.
        error: Why the link was detached, when detaching because of an error.
    """

    handle: int
    closed: bool = False
    error: Error | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_DETACH

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_uint(self.handle),
                encode_boolean(True) if self.closed else None,
                None if self.error is None else self.error.encode(),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Detach:
        """Build the performative from the decoded fields of its described list."""
        raw_error = field_at(values, 2)
        return cls(
            handle=int(field_at(values, 0, 0)),
            closed=bool(field_at(values, 1, False)),
            error=None if raw_error is None else Error.decode(raw_error),
        )

    @classmethod
    def decode(cls, data: bytes) -> Detach:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_DETACH, data))


@dataclass
class End:
    """The ``end`` performative (descriptor ``0x17``): closes a session.

    Attributes:
        error: Why the session was ended, when ending because of an error.
    """

    error: Error | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_END

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(self.DESCRIPTOR, [None if self.error is None else self.error.encode()])

    @classmethod
    def from_fields(cls, values: list[Any]) -> End:
        """Build the performative from the decoded fields of its described list."""
        raw_error = field_at(values, 0)
        return cls(error=None if raw_error is None else Error.decode(raw_error))

    @classmethod
    def decode(cls, data: bytes) -> End:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_END, data))


@dataclass
class Close:
    """The ``close`` performative (descriptor ``0x18``): closes the connection.

    Attributes:
        error: Why the connection was closed, when closing because of an error.
    """

    error: Error | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_CLOSE

    def encode(self) -> bytes:
        """Encode the performative as a described list."""
        return encode_described_list(self.DESCRIPTOR, [None if self.error is None else self.error.encode()])

    @classmethod
    def from_fields(cls, values: list[Any]) -> Close:
        """Build the performative from the decoded fields of its described list."""
        raw_error = field_at(values, 0)
        return cls(error=None if raw_error is None else Error.decode(raw_error))

    @classmethod
    def decode(cls, data: bytes) -> Close:
        """Decode the performative from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_CLOSE, data))


Performative = Open | Begin | Attach | Flow | Transfer | Disposition | Detach | End | Close

_PERFORMATIVE_TYPES: dict[int, Any] = {
    DESCRIPTOR_OPEN: Open,
    DESCRIPTOR_BEGIN: Begin,
    DESCRIPTOR_ATTACH: Attach,
    DESCRIPTOR_FLOW: Flow,
    DESCRIPTOR_TRANSFER: Transfer,
    DESCRIPTOR_DISPOSITION: Disposition,
    DESCRIPTOR_DETACH: Detach,
    DESCRIPTOR_END: End,
    DESCRIPTOR_CLOSE: Close,
}


def read_performative(decoder: Decoder) -> Performative:
    """Read one performative from ``decoder``, dispatching on its descriptor.

    Args:
        decoder: Decoder positioned at the performative's described-type constructor.

    Returns:
        The matching performative dataclass.

    Raises:
        ProtocolError: If the descriptor is not a known performative.
    """
    descriptor, values = read_described_list(decoder)
    code = descriptor_code(descriptor, SYMBOLIC_DESCRIPTORS)
    performative_type = _PERFORMATIVE_TYPES.get(code)
    if performative_type is None:
        raise ProtocolError(f"unknown performative descriptor 0x{code:02x}")
    performative: Performative = performative_type.from_fields(values)
    return performative


def decode_performative(data: bytes) -> Performative:
    """Decode one performative from the start of ``data``, ignoring trailing bytes."""
    return read_performative(Decoder(data))


def decode_performative_with_payload(data: bytes) -> tuple[Performative, bytes]:
    """Decode one performative and return it with the bytes that follow it.

    Args:
        data: An AMQP frame body: a performative optionally followed by a
            ``transfer`` payload.

    Returns:
        The performative and the remaining raw payload bytes (empty when the
        frame carried no payload).
    """
    decoder = Decoder(data)
    performative = read_performative(decoder)
    return performative, data[decoder.position :]


def _checked_fields(expected: int, data: bytes) -> list[Any]:
    """Decode a described list and return its fields, checking the descriptor."""
    descriptor, values = read_described_list(Decoder(data))
    code = descriptor_code(descriptor, SYMBOLIC_DESCRIPTORS)
    if code != expected:
        raise ProtocolError(f"expected descriptor 0x{expected:02x}, got 0x{code:02x}")
    return values


def _terminus_fields(expected: int, data: bytes | Described) -> list[Any]:
    """Return the fields of a terminus given either encoded bytes or a decoded described value."""
    if not isinstance(data, Described):
        return _checked_fields(expected, data)
    code = descriptor_code(data.descriptor, SYMBOLIC_DESCRIPTORS)
    if code != expected:
        raise ProtocolError(f"expected descriptor 0x{expected:02x}, got 0x{code:02x}")
    values = [] if data.value is None else data.value
    if not isinstance(values, list):
        raise ProtocolError(f"expected a described list, got {type(values).__name__}")
    return values


def _encode_unsettled(unsettled: Mapping[bytes, DeliveryState]) -> bytes:
    """Encode the ``unsettled`` map, whose keys are delivery tags and values delivery states."""
    items: list[bytes] = []
    for delivery_tag, state in unsettled.items():
        items.append(encode_binary(delivery_tag))
        items.append(state.encode())
    return encode_map_of_encoded(items)


def _decode_unsettled(value: Any) -> dict[bytes, DeliveryState] | None:
    raw = as_dict(value)
    if raw is None:
        return None
    return {bytes(delivery_tag): decode_delivery_state(state) for delivery_tag, state in raw.items()}


def _encode_outcome(outcome: Any) -> bytes | None:
    """Encode a ``default-outcome`` field, which is any delivery-state value."""
    if outcome is None:
        return None
    encoded: bytes = outcome.encode()
    return encoded


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_bytes(value: Any) -> bytes | None:
    return None if value is None else bytes(value)
