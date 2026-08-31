"""AMQP 1.0 ``error`` and delivery-state/outcome types.

Delivery states are described lists carried inside ``transfer`` and
``disposition`` performatives (§3.4). ``error`` (§2.8.15) is carried inside
``detach``, ``end``, ``close`` and the ``rejected`` outcome.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

from ..exceptions import ProtocolError
from .encoding import (
    Decoder,
    Described,
    as_dict,
    descriptor_code,
    encode_boolean,
    encode_described_list,
    encode_string,
    encode_symbol,
    encode_symbol_map,
    encode_uint,
    encode_ulong,
    field_at,
    read_described_list,
)

DESCRIPTOR_ERROR = 0x1D
DESCRIPTOR_RECEIVED = 0x23
DESCRIPTOR_ACCEPTED = 0x24
DESCRIPTOR_REJECTED = 0x25
DESCRIPTOR_RELEASED = 0x26
DESCRIPTOR_MODIFIED = 0x27

SYMBOLIC_DESCRIPTORS: dict[str, int] = {
    "amqp:error:list": DESCRIPTOR_ERROR,
    "amqp:received:list": DESCRIPTOR_RECEIVED,
    "amqp:accepted:list": DESCRIPTOR_ACCEPTED,
    "amqp:rejected:list": DESCRIPTOR_REJECTED,
    "amqp:released:list": DESCRIPTOR_RELEASED,
    "amqp:modified:list": DESCRIPTOR_MODIFIED,
}

# Outcome symbols, as used in the Source.outcomes / Source.default-outcome fields.
OUTCOME_ACCEPTED = "amqp:accepted:list"
OUTCOME_REJECTED = "amqp:rejected:list"
OUTCOME_RELEASED = "amqp:released:list"
OUTCOME_MODIFIED = "amqp:modified:list"


@dataclass
class Error:
    """An AMQP ``error`` (descriptor ``0x1d``).

    Attributes:
        condition: Error condition symbol, e.g. ``amqp:not-found``.
        description: Human-readable explanation, if the peer supplied one.
        info: Extra condition-specific fields.
    """

    condition: str
    description: str | None = None
    info: dict[Any, Any] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_ERROR

    def encode(self) -> bytes:
        """Encode the error as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_symbol(self.condition),
                None if self.description is None else encode_string(self.description),
                None if self.info is None else encode_symbol_map(self.info),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Error:
        """Build an error from the decoded fields of its described list."""
        condition = field_at(values, 0)
        if condition is None:
            raise ProtocolError("error is missing its mandatory condition field")
        return cls(
            condition=str(condition),
            description=_optional_str(field_at(values, 1)),
            info=as_dict(field_at(values, 2)),
        )

    @classmethod
    def decode(cls, data: bytes | Described) -> Error:
        """Decode an error from encoded bytes or an already-decoded described value."""
        descriptor, values = _described_fields(data)
        if descriptor_code(descriptor, SYMBOLIC_DESCRIPTORS) != DESCRIPTOR_ERROR:
            raise ProtocolError(f"expected an error descriptor, got {descriptor!r}")
        return cls.from_fields(values)


@dataclass
class Accepted:
    """The ``accepted`` outcome (descriptor ``0x24``): the delivery was processed."""

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_ACCEPTED

    def encode(self) -> bytes:
        """Encode the outcome as an empty described list."""
        return encode_described_list(self.DESCRIPTOR, [])

    @classmethod
    def from_fields(cls, values: list[Any]) -> Accepted:
        """Build the outcome from the decoded fields of its described list."""
        del values
        return cls()


@dataclass
class Rejected:
    """The ``rejected`` outcome (descriptor ``0x25``): the delivery is unprocessable.

    Attributes:
        error: Why the delivery was rejected, if the peer said.
    """

    error: Error | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_REJECTED

    def encode(self) -> bytes:
        """Encode the outcome as a described list."""
        return encode_described_list(self.DESCRIPTOR, [None if self.error is None else self.error.encode()])

    @classmethod
    def from_fields(cls, values: list[Any]) -> Rejected:
        """Build the outcome from the decoded fields of its described list."""
        raw_error = field_at(values, 0)
        return cls(error=None if raw_error is None else Error.decode(raw_error))


@dataclass
class Released:
    """The ``released`` outcome (descriptor ``0x26``): the delivery is returned unchanged."""

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_RELEASED

    def encode(self) -> bytes:
        """Encode the outcome as an empty described list."""
        return encode_described_list(self.DESCRIPTOR, [])

    @classmethod
    def from_fields(cls, values: list[Any]) -> Released:
        """Build the outcome from the decoded fields of its described list."""
        del values
        return cls()


@dataclass
class Modified:
    """The ``modified`` outcome (descriptor ``0x27``): the delivery is returned, annotated.

    Attributes:
        delivery_failed: Whether the delivery-count should be incremented.
        undeliverable_here: Whether the message must not be redelivered to this link.
        message_annotations: Annotations to merge into the message.
    """

    delivery_failed: bool = False
    undeliverable_here: bool = False
    message_annotations: dict[Any, Any] | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_MODIFIED

    def encode(self) -> bytes:
        """Encode the outcome as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_boolean(True) if self.delivery_failed else None,
                encode_boolean(True) if self.undeliverable_here else None,
                None if self.message_annotations is None else encode_symbol_map(self.message_annotations),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Modified:
        """Build the outcome from the decoded fields of its described list."""
        return cls(
            delivery_failed=bool(field_at(values, 0, False)),
            undeliverable_here=bool(field_at(values, 1, False)),
            message_annotations=as_dict(field_at(values, 2)),
        )


@dataclass
class Received:
    """The ``received`` state (descriptor ``0x23``): progress of a partial delivery.

    Attributes:
        section_number: Index of the section the receiver has data for.
        section_offset: Byte offset reached within that section.
    """

    section_number: int
    section_offset: int

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_RECEIVED

    def encode(self) -> bytes:
        """Encode the state as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [encode_uint(self.section_number), encode_ulong(self.section_offset)],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> Received:
        """Build the state from the decoded fields of its described list."""
        return cls(
            section_number=int(field_at(values, 0, 0)),
            section_offset=int(field_at(values, 1, 0)),
        )


DeliveryState = Accepted | Rejected | Released | Modified | Received

_STATE_TYPES: dict[int, Any] = {
    DESCRIPTOR_ACCEPTED: Accepted,
    DESCRIPTOR_REJECTED: Rejected,
    DESCRIPTOR_RELEASED: Released,
    DESCRIPTOR_MODIFIED: Modified,
    DESCRIPTOR_RECEIVED: Received,
}


def decode_delivery_state(data: bytes | bytearray | Described) -> DeliveryState:
    """Decode a delivery state, dispatching on its descriptor.

    Args:
        data: Encoded described list, or an already-decoded :class:`Described`
            (as produced when the state is nested inside a performative).

    Returns:
        The matching delivery-state dataclass.

    Raises:
        ProtocolError: If the descriptor is not a known delivery state.
    """
    descriptor, values = _described_fields(data)
    code = descriptor_code(descriptor, SYMBOLIC_DESCRIPTORS)
    state_type = _STATE_TYPES.get(code)
    if state_type is None:
        raise ProtocolError(f"unknown delivery-state descriptor 0x{code:02x}")
    state: DeliveryState = state_type.from_fields(values)
    return state


def _described_fields(data: bytes | bytearray | Described) -> tuple[int | str, list[Any]]:
    """Return the descriptor and field list of an encoded or decoded described list."""
    if isinstance(data, Described):
        if data.value is None:
            return data.descriptor, []
        if not isinstance(data.value, list):
            raise ProtocolError(f"expected a described list, got {type(data.value).__name__}")
        return data.descriptor, data.value
    return read_described_list(Decoder(data))


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)
