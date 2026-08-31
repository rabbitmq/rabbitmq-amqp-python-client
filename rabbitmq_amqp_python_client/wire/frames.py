"""AMQP 1.0 frame framing: reading and writing whole frames on a socket.

Every frame starts with an 8-byte header (§2.3.1)::

    Size (uint32) DOFF (uint8) Type (uint8) Channel (uint16) [ExtendedHeader] [FrameBody]

``Size`` counts itself, ``DOFF`` is the body offset in 4-byte words (minimum 2),
and the ``(DOFF - 2) * 4`` bytes between the header and the body are a
currently-unused extended header that this module skips.

An empty body means an empty frame, which AMQP uses as a heartbeat.

**End of stream:** every read raises
:class:`~..exceptions.ProtocolError` when the peer closes the connection, both
at a frame boundary and mid-frame; callers never see a sentinel value.
"""

from __future__ import annotations

import struct
from typing import Any, Protocol

from ..exceptions import ProtocolError
from .performatives import Performative, decode_performative_with_payload
from .sasl import (
    AMQP_PROTOCOL_HEADER,
    AMQP_SASL_HEADER,
    PROTOCOL_HEADER_SIZE,
    SaslFrame,
    decode_sasl_frame,
)

FRAME_TYPE_AMQP = 0x00
FRAME_TYPE_SASL = 0x01

FRAME_HEADER_SIZE = 8
DEFAULT_DOFF = 2

#: Smallest ``max-frame-size`` an AMQP peer may negotiate (§2.7.1).
MIN_MAX_FRAME_SIZE = 512

#: Hard cap on an accepted frame size, so a bogus length cannot make the client
#: allocate an unbounded buffer before the frame is rejected.
FRAME_SIZE_LIMIT = 1 << 30

FrameBody = Performative | SaslFrame | None


class SupportsRecv(Protocol):
    """Anything that can hand out received bytes, e.g. :class:`socket.socket`."""

    def recv(self, buffer_size: int, /) -> bytes:
        """Return up to ``buffer_size`` bytes, or ``b""`` at end of stream."""
        ...


class SupportsSendAll(Protocol):
    """Anything that can send a whole buffer, e.g. :class:`socket.socket`."""

    def sendall(self, data: bytes, /) -> Any:
        """Send every byte of ``data``."""
        ...


def recv_exactly(sock: SupportsRecv, count: int) -> bytes:
    """Read exactly ``count`` bytes, looping until they all arrive.

    A single ``recv()`` is not guaranteed to return everything asked for, so
    short reads are accumulated.

    Args:
        sock: Source of bytes.
        count: Number of bytes to read; 0 returns ``b""`` without reading.

    Returns:
        Exactly ``count`` bytes.

    Raises:
        ProtocolError: If the peer closes the connection before ``count`` bytes
            arrive, or if the underlying socket fails.
    """
    if count == 0:
        return b""
    chunks: list[bytes] = []
    received = 0
    while received < count:
        try:
            chunk = sock.recv(count - received)
        except OSError as error:
            raise ProtocolError(f"socket error after {received} of {count} bytes: {error}") from error
        if not chunk:
            raise ProtocolError(f"peer closed the connection after {received} of {count} expected bytes")
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def read_frame(sock: SupportsRecv) -> tuple[int, int, bytes]:
    """Read one complete frame.

    Args:
        sock: Source of bytes; anything with a ``recv(n) -> bytes`` method,
            which makes this testable over :func:`socket.socketpair`.

    Returns:
        A ``(frame_type, channel, body)`` triple, where ``body`` is everything
        after the ``DOFF * 4``-byte header: the encoded performative plus, for
        ``transfer`` frames, the trailing raw payload. ``body`` is empty for a
        heartbeat frame.

    Raises:
        ProtocolError: If the peer closes the connection, if the declared size
            or data offset is invalid, or if a protocol header arrives where a
            frame was expected.
    """
    size_bytes = recv_exactly(sock, 4)
    if size_bytes == b"AMQP":
        remainder = recv_exactly(sock, PROTOCOL_HEADER_SIZE - 4)
        raise ProtocolError(f"expected a frame but received protocol header {(size_bytes + remainder)!r}")
    size = struct.unpack(">I", size_bytes)[0]
    if size < FRAME_HEADER_SIZE:
        raise ProtocolError(f"frame size {size} is smaller than the {FRAME_HEADER_SIZE}-byte frame header")
    if size > FRAME_SIZE_LIMIT:
        raise ProtocolError(f"frame size {size} exceeds the {FRAME_SIZE_LIMIT}-byte limit this client accepts")

    rest = recv_exactly(sock, size - 4)
    doff, frame_type, channel = struct.unpack(">BBH", rest[:4])
    header_size = doff * 4
    if header_size < FRAME_HEADER_SIZE:
        raise ProtocolError(f"frame data offset {doff} is below the minimum of {DEFAULT_DOFF} words")
    if header_size > size:
        raise ProtocolError(f"frame data offset {doff} words overruns the {size}-byte frame")
    body = rest[header_size - 4 :]
    return frame_type, channel, body


def read_protocol_header(sock: SupportsRecv) -> bytes:
    """Read one 8-byte protocol header.

    Returns:
        The raw header, e.g. :data:`~.sasl.AMQP_SASL_HEADER`.

    Raises:
        ProtocolError: If the peer closes the connection or the header does not
            start with ``"AMQP"``.
    """
    header = recv_exactly(sock, PROTOCOL_HEADER_SIZE)
    if not header.startswith(b"AMQP"):
        raise ProtocolError(f"invalid protocol header {header!r}: does not start with b'AMQP'")
    return header


def build_frame(frame_type: int, channel: int, body: bytes = b"") -> bytes:
    """Build one complete frame with a minimal (``DOFF = 2``) header.

    Args:
        frame_type: :data:`FRAME_TYPE_AMQP` or :data:`FRAME_TYPE_SASL`.
        channel: Channel the frame applies to; 0 for SASL frames.
        body: Encoded performative plus optional payload; empty for a heartbeat.

    Returns:
        The framed bytes, ready to be written to the socket.
    """
    size = FRAME_HEADER_SIZE + len(body)
    header = struct.pack(">IBBH", size, DEFAULT_DOFF, frame_type, channel)
    return header + body


def write_frame(sock: SupportsSendAll, frame_type: int, channel: int, body: bytes = b"") -> None:
    """Write one complete frame.

    Args:
        sock: Destination; anything with a ``sendall(bytes)`` method.
        frame_type: :data:`FRAME_TYPE_AMQP` or :data:`FRAME_TYPE_SASL`.
        channel: Channel the frame applies to; 0 for SASL frames.
        body: Encoded performative plus optional payload.

    Raises:
        ProtocolError: If the underlying socket fails while sending.
    """
    frame = build_frame(frame_type, channel, body)
    try:
        sock.sendall(frame)
    except OSError as error:
        raise ProtocolError(f"socket error while writing a {len(frame)}-byte frame: {error}") from error


def write_protocol_header(sock: SupportsSendAll, header: bytes) -> None:
    """Write one 8-byte protocol header.

    Args:
        sock: Destination; anything with a ``sendall(bytes)`` method.
        header: The header to send, e.g. :data:`~.sasl.AMQP_SASL_HEADER`.

    Raises:
        ProtocolError: If ``header`` is not 8 bytes or the socket fails.
    """
    if len(header) != PROTOCOL_HEADER_SIZE:
        raise ProtocolError(f"protocol header must be {PROTOCOL_HEADER_SIZE} bytes, got {len(header)}")
    try:
        sock.sendall(header)
    except OSError as error:
        raise ProtocolError(f"socket error while writing protocol header {header!r}: {error}") from error


def decode_frame_body(frame_type: int, body: bytes) -> tuple[FrameBody, bytes]:
    """Decode a frame body into its performative and trailing payload.

    Args:
        frame_type: :data:`FRAME_TYPE_AMQP` or :data:`FRAME_TYPE_SASL`.
        body: Frame body as returned by :func:`read_frame`.

    Returns:
        A ``(frame_body, payload)`` pair. ``frame_body`` is a performative for
        AMQP frames, a SASL frame body for SASL frames, and ``None`` for an
        empty (heartbeat) frame. ``payload`` holds the raw message bytes that
        follow a ``transfer`` performative and is empty otherwise.

    Raises:
        ProtocolError: If the frame type is unknown or the body is malformed.
    """
    if not body:
        return None, b""
    if frame_type == FRAME_TYPE_AMQP:
        return decode_performative_with_payload(body)
    if frame_type == FRAME_TYPE_SASL:
        return decode_sasl_frame(body), b""
    raise ProtocolError(f"unknown frame type 0x{frame_type:02x}")


#: A heartbeat: an AMQP frame with an empty body on channel 0.
EMPTY_FRAME = build_frame(FRAME_TYPE_AMQP, 0)

__all__ = [
    "AMQP_PROTOCOL_HEADER",
    "AMQP_SASL_HEADER",
    "DEFAULT_DOFF",
    "EMPTY_FRAME",
    "FRAME_HEADER_SIZE",
    "FRAME_SIZE_LIMIT",
    "FRAME_TYPE_AMQP",
    "FRAME_TYPE_SASL",
    "FrameBody",
    "MIN_MAX_FRAME_SIZE",
    "PROTOCOL_HEADER_SIZE",
    "SupportsRecv",
    "SupportsSendAll",
    "build_frame",
    "decode_frame_body",
    "read_frame",
    "read_protocol_header",
    "recv_exactly",
    "write_frame",
    "write_protocol_header",
]
