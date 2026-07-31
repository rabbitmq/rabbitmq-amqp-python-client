"""AMQP 1.0 SASL security layer frame bodies.

SASL frames (frame type ``0x01``) always travel on channel 0 and never carry a
payload. The layer is entered with its own protocol header
(:data:`AMQP_SASL_HEADER`); once ``sasl-outcome`` reports success both peers
re-send the plain AMQP protocol header (:data:`AMQP_PROTOCOL_HEADER`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar

from ..exceptions import ProtocolError
from .encoding import (
    Decoder,
    as_symbol_list,
    descriptor_code,
    encode_binary,
    encode_described_list,
    encode_string,
    encode_symbol,
    encode_symbol_array,
    encode_ubyte,
    field_at,
    read_described_list,
)

PROTOCOL_ID_AMQP = 0x00
PROTOCOL_ID_TLS = 0x02
PROTOCOL_ID_SASL = 0x03

AMQP_PROTOCOL_HEADER = b"AMQP\x00\x01\x00\x00"
AMQP_SASL_HEADER = b"AMQP\x03\x01\x00\x00"
AMQP_TLS_HEADER = b"AMQP\x02\x01\x00\x00"
PROTOCOL_HEADER_SIZE = 8

DESCRIPTOR_SASL_MECHANISMS = 0x40
DESCRIPTOR_SASL_INIT = 0x41
DESCRIPTOR_SASL_CHALLENGE = 0x42
DESCRIPTOR_SASL_RESPONSE = 0x43
DESCRIPTOR_SASL_OUTCOME = 0x44

SYMBOLIC_DESCRIPTORS: dict[str, int] = {
    "amqp:sasl-mechanisms:list": DESCRIPTOR_SASL_MECHANISMS,
    "amqp:sasl-init:list": DESCRIPTOR_SASL_INIT,
    "amqp:sasl-challenge:list": DESCRIPTOR_SASL_CHALLENGE,
    "amqp:sasl-response:list": DESCRIPTOR_SASL_RESPONSE,
    "amqp:sasl-outcome:list": DESCRIPTOR_SASL_OUTCOME,
}

MECHANISM_PLAIN = "PLAIN"
MECHANISM_ANONYMOUS = "ANONYMOUS"
MECHANISM_EXTERNAL = "EXTERNAL"

SASL_OK = 0
SASL_AUTH = 1
SASL_SYS = 2
SASL_SYS_PERM = 3
SASL_SYS_TEMP = 4

SASL_OUTCOME_DESCRIPTIONS: dict[int, str] = {
    SASL_OK: "authentication successful",
    SASL_AUTH: "authentication failed: bad credentials",
    SASL_SYS: "system error",
    SASL_SYS_PERM: "permanent system error: authentication cannot succeed",
    SASL_SYS_TEMP: "transient system error: retry later",
}


@dataclass
class SaslMechanisms:
    """The ``sasl-mechanisms`` body (descriptor ``0x40``).

    Attributes:
        server_mechanisms: Mechanism names the server supports.
    """

    server_mechanisms: list[str] = field(default_factory=list)

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_SASL_MECHANISMS

    def encode(self) -> bytes:
        """Encode the body as a described list."""
        return encode_described_list(self.DESCRIPTOR, [encode_symbol_array(self.server_mechanisms)])

    @classmethod
    def from_fields(cls, values: list[Any]) -> SaslMechanisms:
        """Build the body from the decoded fields of its described list."""
        mechanisms = as_symbol_list(field_at(values, 0))
        if mechanisms is None:
            raise ProtocolError("sasl-mechanisms is missing its mandatory sasl-server-mechanisms field")
        return cls(server_mechanisms=mechanisms)

    @classmethod
    def decode(cls, data: bytes) -> SaslMechanisms:
        """Decode the body from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_SASL_MECHANISMS, data))


@dataclass
class SaslInit:
    """The ``sasl-init`` body (descriptor ``0x41``).

    Attributes:
        mechanism: Mechanism the client selected.
        initial_response: Mechanism-specific first response, e.g. from
            :func:`build_plain_initial_response`.
        hostname: Host the client is authenticating to.
    """

    mechanism: str
    initial_response: bytes | None = None
    hostname: str | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_SASL_INIT

    def encode(self) -> bytes:
        """Encode the body as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_symbol(self.mechanism),
                None if self.initial_response is None else encode_binary(self.initial_response),
                None if self.hostname is None else encode_string(self.hostname),
            ],
        )

    @classmethod
    def from_fields(cls, values: list[Any]) -> SaslInit:
        """Build the body from the decoded fields of its described list."""
        mechanism = field_at(values, 0)
        if mechanism is None:
            raise ProtocolError("sasl-init is missing its mandatory mechanism field")
        raw_response = field_at(values, 1)
        hostname = field_at(values, 2)
        return cls(
            mechanism=str(mechanism),
            initial_response=None if raw_response is None else bytes(raw_response),
            hostname=None if hostname is None else str(hostname),
        )

    @classmethod
    def decode(cls, data: bytes) -> SaslInit:
        """Decode the body from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_SASL_INIT, data))


@dataclass
class SaslChallenge:
    """The ``sasl-challenge`` body (descriptor ``0x42``).

    Attributes:
        challenge: Mechanism-specific challenge bytes.
    """

    challenge: bytes = b""

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_SASL_CHALLENGE

    def encode(self) -> bytes:
        """Encode the body as a described list."""
        return encode_described_list(self.DESCRIPTOR, [encode_binary(self.challenge)])

    @classmethod
    def from_fields(cls, values: list[Any]) -> SaslChallenge:
        """Build the body from the decoded fields of its described list."""
        return cls(challenge=bytes(field_at(values, 0, b"")))

    @classmethod
    def decode(cls, data: bytes) -> SaslChallenge:
        """Decode the body from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_SASL_CHALLENGE, data))


@dataclass
class SaslResponse:
    """The ``sasl-response`` body (descriptor ``0x43``).

    Attributes:
        response: Mechanism-specific response to the server's challenge.
    """

    response: bytes = b""

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_SASL_RESPONSE

    def encode(self) -> bytes:
        """Encode the body as a described list."""
        return encode_described_list(self.DESCRIPTOR, [encode_binary(self.response)])

    @classmethod
    def from_fields(cls, values: list[Any]) -> SaslResponse:
        """Build the body from the decoded fields of its described list."""
        return cls(response=bytes(field_at(values, 0, b"")))

    @classmethod
    def decode(cls, data: bytes) -> SaslResponse:
        """Decode the body from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_SASL_RESPONSE, data))


@dataclass
class SaslOutcome:
    """The ``sasl-outcome`` body (descriptor ``0x44``).

    Attributes:
        code: Outcome code; 0 ok, 1 auth, 2 sys, 3 sys-perm, 4 sys-temp.
        additional_data: Mechanism-specific data for a successful outcome.
    """

    code: int
    additional_data: bytes | None = None

    DESCRIPTOR: ClassVar[int] = DESCRIPTOR_SASL_OUTCOME

    def encode(self) -> bytes:
        """Encode the body as a described list."""
        return encode_described_list(
            self.DESCRIPTOR,
            [
                encode_ubyte(self.code),
                None if self.additional_data is None else encode_binary(self.additional_data),
            ],
        )

    @property
    def succeeded(self) -> bool:
        """Whether the negotiation succeeded (``code`` is ``sasl-code.ok``)."""
        return self.code == SASL_OK

    def describe(self) -> str:
        """Return a human-readable description of the outcome code."""
        return SASL_OUTCOME_DESCRIPTIONS.get(self.code, f"unknown sasl-code {self.code}")

    @classmethod
    def from_fields(cls, values: list[Any]) -> SaslOutcome:
        """Build the body from the decoded fields of its described list."""
        code = field_at(values, 0)
        if code is None:
            raise ProtocolError("sasl-outcome is missing its mandatory code field")
        raw_data = field_at(values, 1)
        return cls(code=int(code), additional_data=None if raw_data is None else bytes(raw_data))

    @classmethod
    def decode(cls, data: bytes) -> SaslOutcome:
        """Decode the body from encoded bytes."""
        return cls.from_fields(_checked_fields(DESCRIPTOR_SASL_OUTCOME, data))


SaslFrame = SaslMechanisms | SaslInit | SaslChallenge | SaslResponse | SaslOutcome

_SASL_TYPES: dict[int, Any] = {
    DESCRIPTOR_SASL_MECHANISMS: SaslMechanisms,
    DESCRIPTOR_SASL_INIT: SaslInit,
    DESCRIPTOR_SASL_CHALLENGE: SaslChallenge,
    DESCRIPTOR_SASL_RESPONSE: SaslResponse,
    DESCRIPTOR_SASL_OUTCOME: SaslOutcome,
}


def build_plain_initial_response(username: str, password: str) -> bytes:
    """Build the SASL PLAIN initial response.

    RFC 4616 defines the PLAIN message as
    ``authzid NUL authcid NUL passwd``; the client leaves the authorization
    identity empty, so the message starts with a NUL byte.

    Args:
        username: Authentication identity.
        password: Password for that identity.

    Returns:
        The UTF-8 encoded initial-response bytes.
    """
    return b"\x00" + username.encode("utf-8") + b"\x00" + password.encode("utf-8")


def protocol_header(protocol_id: int) -> bytes:
    """Build the 8-byte protocol header for a layer.

    Args:
        protocol_id: 0 for AMQP, 2 for TLS, 3 for SASL.

    Returns:
        The ``"AMQP" protocol-id major minor revision`` header.
    """
    return b"AMQP" + bytes((protocol_id, 1, 0, 0))


def read_sasl_frame(decoder: Decoder) -> SaslFrame:
    """Read one SASL frame body from ``decoder``, dispatching on its descriptor.

    Raises:
        ProtocolError: If the descriptor is not a known SASL frame body.
    """
    descriptor, values = read_described_list(decoder)
    code = descriptor_code(descriptor, SYMBOLIC_DESCRIPTORS)
    sasl_type = _SASL_TYPES.get(code)
    if sasl_type is None:
        raise ProtocolError(f"unknown SASL frame descriptor 0x{code:02x}")
    frame: SaslFrame = sasl_type.from_fields(values)
    return frame


def decode_sasl_frame(data: bytes) -> SaslFrame:
    """Decode one SASL frame body from the start of ``data``."""
    return read_sasl_frame(Decoder(data))


def _checked_fields(expected: int, data: bytes) -> list[Any]:
    """Decode a described list and return its fields, checking the descriptor."""
    descriptor, values = read_described_list(Decoder(data))
    code = descriptor_code(descriptor, SYMBOLIC_DESCRIPTORS)
    if code != expected:
        raise ProtocolError(f"expected descriptor 0x{expected:02x}, got 0x{code:02x}")
    return values
