"""AMQP 1.0 message sections and the annotated-message container.

A message is a concatenation of described sections (§3.2) which appear at most
once each — except ``data``/``amqp-sequence``, which may repeat — and always in
this order: ``header``, ``delivery-annotations``, ``message-annotations``,
``properties``, ``application-properties``, body, ``footer``.

``header`` and ``properties`` are described *lists*; the annotation-style
sections (``delivery-annotations``, ``message-annotations``,
``application-properties``, ``footer``) describe a bare ``map``, and the body
sections describe a ``binary``, a ``list`` or a single arbitrary value.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass, field
from typing import Any, ClassVar

from ..exceptions import ProtocolError
from .encoding import (
    Decoder,
    Described,
    Symbol,
    Timestamp,
    as_dict,
    descriptor_code,
    encode_binary,
    encode_boolean,
    encode_described,
    encode_described_list,
    encode_described_value,
    encode_list,
    encode_map,
    encode_string,
    encode_symbol,
    encode_timestamp,
    encode_ubyte,
    encode_uint,
    encode_ulong,
    encode_value,
    field_at,
)

DESCRIPTOR_HEADER = 0x70
DESCRIPTOR_DELIVERY_ANNOTATIONS = 0x71
DESCRIPTOR_MESSAGE_ANNOTATIONS = 0x72
DESCRIPTOR_PROPERTIES = 0x73
DESCRIPTOR_APPLICATION_PROPERTIES = 0x74
DESCRIPTOR_DATA = 0x75
DESCRIPTOR_AMQP_SEQUENCE = 0x76
DESCRIPTOR_AMQP_VALUE = 0x77
DESCRIPTOR_FOOTER = 0x78

SYMBOLIC_DESCRIPTORS: dict[str, int] = {
    "amqp:header:list": DESCRIPTOR_HEADER,
    "amqp:delivery-annotations:map": DESCRIPTOR_DELIVERY_ANNOTATIONS,
    "amqp:message-annotations:map": DESCRIPTOR_MESSAGE_ANNOTATIONS,
    "amqp:properties:list": DESCRIPTOR_PROPERTIES,
    "amqp:application-properties:map": DESCRIPTOR_APPLICATION_PROPERTIES,
    "amqp:data:binary": DESCRIPTOR_DATA,
    "amqp:amqp-sequence:list": DESCRIPTOR_AMQP_SEQUENCE,
    "amqp:amqp-value:*": DESCRIPTOR_AMQP_VALUE,
    "amqp:footer:map": DESCRIPTOR_FOOTER,
}

DEFAULT_PRIORITY = 4


@dataclass
class Header:
    """The ``header`` section (descriptor ``0x70``): transport-level delivery hints.

    Attributes:
        durable: Whether the message must not be lost if the broker restarts.
        priority: Relative delivery priority.
        ttl: Time to live in milliseconds.
        first_acquirer: Whether no other link has previously acquired the message.
        delivery_count: Number of prior unsuccessful delivery attempts.
    """

    durable: bool = False
    priority: int = DEFAULT_PRIORITY
    ttl: int | None = None
    first_acquirer: bool = False
    delivery_count: int = 0

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_HEADER

    def encode(self) -> bytes:
        """Encode the section as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_boolean(True) if self.durable else None,
                None if self.priority == DEFAULT_PRIORITY else encode_ubyte(self.priority),
                None if self.ttl is None else encode_uint(self.ttl),
                encode_boolean(True) if self.first_acquirer else None,
                None if self.delivery_count == 0 else encode_uint(self.delivery_count),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Header:
        """Build the section from the decoded fields of its described list."""
        ttl = field_at(values, 2)
        return cls(
            durable=bool(field_at(values, 0, False)),
            priority=int(field_at(values, 1, DEFAULT_PRIORITY)),
            ttl=None if ttl is None else int(ttl),
            first_acquirer=bool(field_at(values, 3, False)),
            delivery_count=int(field_at(values, 4, 0)),
        )


@dataclass
class Properties:
    """The ``properties`` section (descriptor ``0x73``): standard message metadata.

    Attributes:
        message_id: Application-assigned id (``ulong``, ``uuid``, ``binary`` or ``string``).
        user_id: Identity of the user producing the message.
        to: Address of the node the message is destined for.
        subject: Message summary.
        reply_to: Address of the node replies should be sent to.
        correlation_id: Id of the message this one is a reply to.
        content_type: MIME type of the body.
        content_encoding: Content encoding applied on top of ``content_type``.
        absolute_expiry_time: Absolute time after which the message is expired.
        creation_time: Absolute time the message was created.
        group_id: Group the message belongs to.
        group_sequence: Position of the message within its group.
        reply_to_group_id: Group that replies belong to.
    """

    message_id: Any = None
    user_id: bytes | None = None
    to: str | None = None
    subject: str | None = None
    reply_to: str | None = None
    correlation_id: Any = None
    content_type: str | None = None
    content_encoding: str | None = None
    absolute_expiry_time: int | None = None
    creation_time: int | None = None
    group_id: str | None = None
    group_sequence: int | None = None
    reply_to_group_id: str | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_PROPERTIES

    def encode(self) -> bytes:
        """Encode the section as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                None if self.message_id is None else encode_message_id(self.message_id),
                None if self.user_id is None else encode_binary(self.user_id),
                None if self.to is None else encode_string(self.to),
                None if self.subject is None else encode_string(self.subject),
                None if self.reply_to is None else encode_string(self.reply_to),
                None if self.correlation_id is None else encode_message_id(self.correlation_id),
                None if self.content_type is None else encode_symbol(self.content_type),
                None if self.content_encoding is None else encode_symbol(self.content_encoding),
                None if self.absolute_expiry_time is None else encode_timestamp(self.absolute_expiry_time),
                None if self.creation_time is None else encode_timestamp(self.creation_time),
                None if self.group_id is None else encode_string(self.group_id),
                None if self.group_sequence is None else encode_uint(self.group_sequence),
                None if self.reply_to_group_id is None else encode_string(self.reply_to_group_id),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Properties:
        """Build the section from the decoded fields of its described list."""
        return cls(
            message_id=field_at(values, 0),
            user_id=_optional_bytes(field_at(values, 1)),
            to=_optional_str(field_at(values, 2)),
            subject=_optional_str(field_at(values, 3)),
            reply_to=_optional_str(field_at(values, 4)),
            correlation_id=field_at(values, 5),
            content_type=_optional_str(field_at(values, 6)),
            content_encoding=_optional_str(field_at(values, 7)),
            absolute_expiry_time=_optional_int(field_at(values, 8)),
            creation_time=_optional_int(field_at(values, 9)),
            group_id=_optional_str(field_at(values, 10)),
            group_sequence=_optional_int(field_at(values, 11)),
            reply_to_group_id=_optional_str(field_at(values, 12)),
        )


@dataclass
class DeliveryAnnotations:
    """The ``delivery-annotations`` section (descriptor ``0x71``): next-hop-only annotations."""

    value: dict[Any, Any] = field(default_factory=dict)

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_DELIVERY_ANNOTATIONS

    def encode(self) -> bytes:
        """Encode the section as a described map."""
        return encode_described(self.DESCRIPTOR, encode_map(self.value))

    @classmethod
    def from_value(cls, value: Any) -> DeliveryAnnotations:
        """Build the section from its decoded described value."""
        return cls(value=as_dict(value) or {})


@dataclass
class MessageAnnotations:
    """The ``message-annotations`` section (descriptor ``0x72``): end-to-end annotations."""

    value: dict[Any, Any] = field(default_factory=dict)

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_MESSAGE_ANNOTATIONS

    def encode(self) -> bytes:
        """Encode the section as a described map."""
        return encode_described(self.DESCRIPTOR, encode_map(self.value))

    @classmethod
    def from_value(cls, value: Any) -> MessageAnnotations:
        """Build the section from its decoded described value."""
        return cls(value=as_dict(value) or {})


@dataclass
class ApplicationProperties:
    """The ``application-properties`` section (descriptor ``0x74``): application metadata.

    Keys must be strings; values must be primitive AMQP types.
    """

    value: dict[str, Any] = field(default_factory=dict)

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_APPLICATION_PROPERTIES

    def encode(self) -> bytes:
        """Encode the section as a described map with string keys."""
        items: dict[Any, Any] = {str(key): item for key, item in self.value.items()}
        return encode_described(self.DESCRIPTOR, encode_map(items))

    @classmethod
    def from_value(cls, value: Any) -> ApplicationProperties:
        """Build the section from its decoded described value."""
        decoded = as_dict(value) or {}
        return cls(value={str(key): item for key, item in decoded.items()})


@dataclass
class Footer:
    """The ``footer`` section (descriptor ``0x78``): annotations computed over the body."""

    value: dict[Any, Any] = field(default_factory=dict)

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_FOOTER

    def encode(self) -> bytes:
        """Encode the section as a described map."""
        return encode_described(self.DESCRIPTOR, encode_map(self.value))

    @classmethod
    def from_value(cls, value: Any) -> Footer:
        """Build the section from its decoded described value."""
        return cls(value=as_dict(value) or {})


@dataclass
class Data:
    """The ``data`` body section (descriptor ``0x75``): an opaque binary payload."""

    value: bytes = b""

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_DATA

    def encode(self) -> bytes:
        """Encode the section as a described binary."""
        return encode_described(self.DESCRIPTOR, encode_binary(self.value))

    @classmethod
    def from_value(cls, value: Any) -> Data:
        """Build the section from its decoded described value."""
        if value is None:
            return cls(b"")
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise ProtocolError(f"data section must hold binary, got {type(value).__name__}")
        return cls(bytes(value))


@dataclass
class AmqpSequence:
    """The ``amqp-sequence`` body section (descriptor ``0x76``): one AMQP list."""

    value: list[Any] = field(default_factory=list)

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_AMQP_SEQUENCE

    def encode(self) -> bytes:
        """Encode the section as a described list."""
        return encode_described(self.DESCRIPTOR, encode_list(self.value))

    @classmethod
    def from_value(cls, value: Any) -> AmqpSequence:
        """Build the section from its decoded described value."""
        if value is None:
            return cls([])
        if not isinstance(value, list):
            raise ProtocolError(f"amqp-sequence section must hold a list, got {type(value).__name__}")
        return cls(value)


@dataclass
class AmqpValue:
    """The ``amqp-value`` body section (descriptor ``0x77``): one arbitrary AMQP value."""

    value: Any = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_AMQP_VALUE

    def encode(self) -> bytes:
        """Encode the section as a described value."""
        return encode_described_value(self.DESCRIPTOR, self.value)

    @classmethod
    def from_value(cls, value: Any) -> AmqpValue:
        """Build the section from its decoded described value."""
        return cls(value)


BodySection = Data | AmqpSequence | AmqpValue
MessageBody = Data | AmqpSequence | AmqpValue | list[Data | AmqpSequence]


def encode_message_id(value: Any) -> bytes:
    """Encode a ``message-id`` field.

    The ``message-id``/``correlation-id`` fields accept ``ulong``, ``uuid``,
    ``binary`` or ``string`` only; a plain ``int`` is therefore encoded as
    ``ulong`` rather than the ``int``/``long`` that generic inference picks.

    Args:
        value: The id value.

    Returns:
        The encoded id.

    Raises:
        ProtocolError: If the value's type is not a valid ``message-id`` type.
    """
    if isinstance(value, bool):
        raise ProtocolError("message-id cannot be a boolean")
    if isinstance(value, int):
        return encode_ulong(value)
    if isinstance(value, (uuid_module.UUID, bytes, bytearray, str)):
        return encode_value(bytes(value) if isinstance(value, bytearray) else value)
    raise ProtocolError(f"invalid message-id type {type(value).__name__}")


@dataclass
class Message:
    """An annotated AMQP message: the payload of one or more ``transfer`` frames.

    A ``str`` or ``bytes`` passed as ``body`` is wrapped in a :class:`Data`
    section (``str`` is encoded as UTF-8), which covers the common case:

        >>> Message("hello", properties=Properties(subject="greeting"))

    Attributes:
        body: The body section(s), or ``None`` for a body-less message.
        header: Transport-level delivery hints.
        delivery_annotations: Annotations for the next hop only.
        message_annotations: Annotations carried end to end.
        properties: Standard message metadata.
        application_properties: Application-defined metadata.
        footer: Annotations computed over the body.
    """

    body: MessageBody | bytes | str | None = None
    header: Header | None = None
    delivery_annotations: DeliveryAnnotations | None = None
    message_annotations: MessageAnnotations | None = None
    properties: Properties | None = None
    application_properties: ApplicationProperties | None = None
    footer: Footer | None = None

    def __post_init__(self) -> None:
        self.body = _normalize_body(self.body)

    def encode(self) -> bytes:
        """Encode the message as the concatenation of its sections, in spec order."""
        sections: list[bytes] = []
        for section in (
            self.header,
            self.delivery_annotations,
            self.message_annotations,
            self.properties,
            self.application_properties,
        ):
            if section is not None:
                sections.append(section.encode())
        body = _normalize_body(self.body)
        if isinstance(body, list):
            sections.extend(item.encode() for item in body)
        elif body is not None:
            sections.append(body.encode())
        if self.footer is not None:
            sections.append(self.footer.encode())
        return b"".join(sections)

    def body_as_bytes(self) -> bytes:
        """Return the body as raw bytes.

        Concatenates repeated :class:`Data` sections, and converts an
        :class:`AmqpValue` holding a ``str`` or ``bytes``.

        Returns:
            The body bytes; empty when the message has no body.

        Raises:
            TypeError: If the body is a sequence, or a value that is neither a
                string nor binary.
        """
        body = self.body
        if body is None:
            return b""
        if isinstance(body, Data):
            return body.value
        if isinstance(body, list):
            if any(not isinstance(item, Data) for item in body):
                raise TypeError("message body holds amqp-sequence sections, which are not raw bytes")
            return b"".join(item.value for item in body if isinstance(item, Data))
        if isinstance(body, AmqpValue):
            if isinstance(body.value, str):
                return body.value.encode("utf-8")
            if isinstance(body.value, (bytes, bytearray)):
                return bytes(body.value)
            raise TypeError(f"amqp-value body of type {type(body.value).__name__} is not raw bytes")
        raise TypeError("message body is an amqp-sequence, which is not raw bytes")

    def body_as_string(self) -> str:
        """Return the body decoded as UTF-8.

        Raises:
            TypeError: If the body cannot be reduced to bytes.
        """
        return self.body_as_bytes().decode("utf-8")

    @classmethod
    def decode(cls, data: bytes | bytearray | memoryview) -> Message:
        """Decode a message from concatenated encoded sections.

        Reads one described section at a time until the buffer is exhausted, so
        it works for any subset of the optional sections.

        Args:
            data: The concatenated ``transfer`` payload bytes.

        Returns:
            The decoded message.

        Raises:
            ProtocolError: If a section is malformed, its descriptor unknown, or
                the body mixes an ``amqp-value`` section with
                ``data``/``amqp-sequence`` sections.
        """
        message = cls()
        body_sections: list[Data | AmqpSequence] = []
        decoder = Decoder(data)
        while decoder.remaining > 0:
            value = decoder.read_value()
            if not isinstance(value, Described):
                raise ProtocolError(f"message section must be a described type, got {type(value).__name__}")
            _apply_section(message, value, body_sections)
        if body_sections:
            if message.body is not None:
                raise ProtocolError("message body mixes an amqp-value section with data/amqp-sequence sections")
            message.body = body_sections[0] if len(body_sections) == 1 else body_sections
        return message


def _apply_section(message: Message, section: Described, body_sections: list[Data | AmqpSequence]) -> None:
    code = descriptor_code(section.descriptor, SYMBOLIC_DESCRIPTORS)
    if code == DESCRIPTOR_HEADER:
        message.header = Header.from_fields(_as_field_list(section))
    elif code == DESCRIPTOR_DELIVERY_ANNOTATIONS:
        message.delivery_annotations = DeliveryAnnotations.from_value(section.value)
    elif code == DESCRIPTOR_MESSAGE_ANNOTATIONS:
        message.message_annotations = MessageAnnotations.from_value(section.value)
    elif code == DESCRIPTOR_PROPERTIES:
        message.properties = Properties.from_fields(_as_field_list(section))
    elif code == DESCRIPTOR_APPLICATION_PROPERTIES:
        message.application_properties = ApplicationProperties.from_value(section.value)
    elif code == DESCRIPTOR_DATA:
        body_sections.append(Data.from_value(section.value))
    elif code == DESCRIPTOR_AMQP_SEQUENCE:
        body_sections.append(AmqpSequence.from_value(section.value))
    elif code == DESCRIPTOR_AMQP_VALUE:
        if message.body is not None:
            raise ProtocolError("message has more than one amqp-value body section")
        message.body = AmqpValue.from_value(section.value)
    elif code == DESCRIPTOR_FOOTER:
        message.footer = Footer.from_value(section.value)
    else:
        raise ProtocolError(f"unknown message section descriptor 0x{code:02x}")


def decode_section(data: bytes | bytearray | memoryview) -> Any:
    """Decode a single message section from the start of ``data``.

    Returns:
        The section dataclass matching the descriptor.

    Raises:
        ProtocolError: If the descriptor is not a known message section.
    """
    value = Decoder(data).read_value()
    if not isinstance(value, Described):
        raise ProtocolError(f"message section must be a described type, got {type(value).__name__}")
    code = descriptor_code(value.descriptor, SYMBOLIC_DESCRIPTORS)
    if code == DESCRIPTOR_HEADER:
        return Header.from_fields(_as_field_list(value))
    if code == DESCRIPTOR_PROPERTIES:
        return Properties.from_fields(_as_field_list(value))
    simple_sections: dict[int, Any] = {
        DESCRIPTOR_DELIVERY_ANNOTATIONS: DeliveryAnnotations,
        DESCRIPTOR_MESSAGE_ANNOTATIONS: MessageAnnotations,
        DESCRIPTOR_APPLICATION_PROPERTIES: ApplicationProperties,
        DESCRIPTOR_FOOTER: Footer,
        DESCRIPTOR_DATA: Data,
        DESCRIPTOR_AMQP_SEQUENCE: AmqpSequence,
        DESCRIPTOR_AMQP_VALUE: AmqpValue,
    }
    section_type = simple_sections.get(code)
    if section_type is None:
        raise ProtocolError(f"unknown message section descriptor 0x{code:02x}")
    return section_type.from_value(value.value)


def _as_field_list(section: Described) -> list[Any]:
    if section.value is None:
        return []
    if not isinstance(section.value, list):
        raise ProtocolError(f"section {section.descriptor!r} must be a described list")
    return section.value


def _normalize_body(body: MessageBody | bytes | str | None) -> MessageBody | None:
    if isinstance(body, str):
        return Data(body.encode("utf-8"))
    if isinstance(body, (bytes, bytearray, memoryview)):
        return Data(bytes(body))
    return body


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_bytes(value: Any) -> bytes | None:
    return None if value is None else bytes(value)


__all__ = [
    "AmqpSequence",
    "AmqpValue",
    "ApplicationProperties",
    "BodySection",
    "Data",
    "DeliveryAnnotations",
    "Footer",
    "Header",
    "Message",
    "MessageAnnotations",
    "MessageBody",
    "Properties",
    "Symbol",
    "Timestamp",
    "decode_section",
    "encode_message_id",
]
