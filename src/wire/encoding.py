"""AMQP 1.0 primitive type codec.

Implements the self-describing binary type system of the OASIS AMQP 1.0
specification (Part 1): every value is encoded as a *constructor* (a one-byte
format code, optionally preceded by a described-type wrapper) followed by the
value's data. All multi-byte values are big-endian.

Decoding maps AMQP types onto Python as follows:

============================  ==========================
AMQP type                     Python type
============================  ==========================
``null``                      ``None``
``boolean``                   ``bool``
all integral types            ``int``
``float`` / ``double``        ``float``
``char``                      ``str`` (one code point)
``timestamp``                 :class:`Timestamp`
``uuid``                      :class:`uuid.UUID`
``binary``                    ``bytes``
``string``                    ``str``
``symbol``                    :class:`Symbol`
``list`` / ``array``          ``list``
``map``                       ``dict``
described                     :class:`Described`
============================  ==========================

Encoding infers the AMQP type from the Python type. The wrapper classes below
(:class:`Symbol`, :class:`Uint`, :class:`Timestamp`, ...) are ``str``/``int``/
``float`` subclasses that force a specific AMQP type where inference is not
enough; they compare equal to their plain counterparts, so a decode/encode
round trip is value-preserving.
"""

from __future__ import annotations

import struct
import uuid as uuid_module
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..exceptions import ProtocolError

# --- Format codes (§1.6) ---
CODE_DESCRIBED = 0x00
CODE_NULL = 0x40
CODE_BOOLEAN_TRUE = 0x41
CODE_BOOLEAN_FALSE = 0x42
CODE_BOOLEAN = 0x56
CODE_UBYTE = 0x50
CODE_BYTE = 0x51
CODE_USHORT = 0x60
CODE_SHORT = 0x61
CODE_UINT0 = 0x43
CODE_SMALLUINT = 0x52
CODE_UINT = 0x70
CODE_SMALLINT = 0x54
CODE_INT = 0x71
CODE_ULONG0 = 0x44
CODE_SMALLULONG = 0x53
CODE_ULONG = 0x80
CODE_SMALLLONG = 0x55
CODE_LONG = 0x81
CODE_FLOAT = 0x72
CODE_DOUBLE = 0x82
CODE_CHAR = 0x73
CODE_TIMESTAMP = 0x83
CODE_UUID = 0x98
CODE_VBIN8 = 0xA0
CODE_VBIN32 = 0xB0
CODE_STR8 = 0xA1
CODE_STR32 = 0xB1
CODE_SYM8 = 0xA3
CODE_SYM32 = 0xB3
CODE_LIST0 = 0x45
CODE_LIST8 = 0xC0
CODE_LIST32 = 0xD0
CODE_MAP8 = 0xC1
CODE_MAP32 = 0xD1
CODE_ARRAY8 = 0xE0
CODE_ARRAY32 = 0xF0

NULL_BYTES = b"\x40"

_UINT8_MAX = 0xFF
_UINT32_MAX = 0xFFFFFFFF
_UINT16_MAX = 0xFFFF
_UINT64_MAX = 0xFFFFFFFFFFFFFFFF
_INT8_MIN, _INT8_MAX = -0x80, 0x7F
_INT16_MIN, _INT16_MAX = -0x8000, 0x7FFF
_INT32_MIN, _INT32_MAX = -0x80000000, 0x7FFFFFFF
_INT64_MIN, _INT64_MAX = -0x8000000000000000, 0x7FFFFFFFFFFFFFFF


class Symbol(str):
    """A ``str`` that encodes as an AMQP ``symbol`` (ASCII) instead of a ``string``."""

    __slots__ = ()


class Char(str):
    """A single-code-point ``str`` that encodes as an AMQP ``char``."""

    __slots__ = ()


class Timestamp(int):
    """An ``int`` (milliseconds since the Unix epoch) that encodes as an AMQP ``timestamp``."""

    __slots__ = ()


class Ubyte(int):
    """An ``int`` that encodes as an AMQP ``ubyte``."""

    __slots__ = ()


class Byte(int):
    """An ``int`` that encodes as an AMQP ``byte``."""

    __slots__ = ()


class Ushort(int):
    """An ``int`` that encodes as an AMQP ``ushort``."""

    __slots__ = ()


class Short(int):
    """An ``int`` that encodes as an AMQP ``short``."""

    __slots__ = ()


class Uint(int):
    """An ``int`` that encodes as an AMQP ``uint``."""

    __slots__ = ()


class Int(int):
    """An ``int`` that encodes as an AMQP ``int``."""

    __slots__ = ()


class Ulong(int):
    """An ``int`` that encodes as an AMQP ``ulong``."""

    __slots__ = ()


class Long(int):
    """An ``int`` that encodes as an AMQP ``long``."""

    __slots__ = ()


class Float(float):
    """A ``float`` that encodes as an AMQP ``float`` (binary32) instead of a ``double``."""

    __slots__ = ()


class Double(float):
    """A ``float`` that encodes as an AMQP ``double`` (binary64)."""

    __slots__ = ()


@dataclass(frozen=True)
class Array:
    """An AMQP ``array``: many values sharing one constructor.

    Attributes:
        element_code: Format code shared by every element, e.g. :data:`CODE_SYM8`.
        values: The element values, encoded without their own constructor byte.
    """

    element_code: int
    values: list[Any]


@dataclass(frozen=True)
class Described:
    """A described type: a descriptor plus the value it annotates.

    Attributes:
        descriptor: Numeric (``ulong``) or symbolic descriptor.
        value: The described value.
    """

    descriptor: int | str
    value: Any


def _check_range(value: int, low: int, high: int, type_name: str) -> None:
    if not low <= value <= high:
        raise ProtocolError(f"value {value} out of range for AMQP {type_name} [{low}, {high}]")


def encode_null() -> bytes:
    """Encode an AMQP ``null``."""
    return NULL_BYTES


def encode_boolean(value: bool) -> bytes:
    """Encode an AMQP ``boolean`` using the one-byte true/false constructors."""
    return bytes((CODE_BOOLEAN_TRUE,)) if value else bytes((CODE_BOOLEAN_FALSE,))


def encode_ubyte(value: int) -> bytes:
    """Encode an AMQP ``ubyte``."""
    _check_range(value, 0, _UINT8_MAX, "ubyte")
    return bytes((CODE_UBYTE, value))


def encode_byte(value: int) -> bytes:
    """Encode an AMQP ``byte``."""
    _check_range(value, _INT8_MIN, _INT8_MAX, "byte")
    return bytes((CODE_BYTE,)) + struct.pack(">b", value)


def encode_ushort(value: int) -> bytes:
    """Encode an AMQP ``ushort``."""
    _check_range(value, 0, _UINT16_MAX, "ushort")
    return bytes((CODE_USHORT,)) + struct.pack(">H", value)


def encode_short(value: int) -> bytes:
    """Encode an AMQP ``short``."""
    _check_range(value, _INT16_MIN, _INT16_MAX, "short")
    return bytes((CODE_SHORT,)) + struct.pack(">h", value)


def encode_uint(value: int) -> bytes:
    """Encode an AMQP ``uint``, preferring the ``uint0``/``smalluint`` compact forms."""
    _check_range(value, 0, _UINT32_MAX, "uint")
    if value == 0:
        return bytes((CODE_UINT0,))
    if value <= _UINT8_MAX:
        return bytes((CODE_SMALLUINT, value))
    return bytes((CODE_UINT,)) + struct.pack(">I", value)


def encode_int(value: int) -> bytes:
    """Encode an AMQP ``int``, preferring the ``smallint`` compact form."""
    _check_range(value, _INT32_MIN, _INT32_MAX, "int")
    if _INT8_MIN <= value <= _INT8_MAX:
        return bytes((CODE_SMALLINT,)) + struct.pack(">b", value)
    return bytes((CODE_INT,)) + struct.pack(">i", value)


def encode_ulong(value: int) -> bytes:
    """Encode an AMQP ``ulong``, preferring the ``ulong0``/``smallulong`` compact forms."""
    _check_range(value, 0, _UINT64_MAX, "ulong")
    if value == 0:
        return bytes((CODE_ULONG0,))
    if value <= _UINT8_MAX:
        return bytes((CODE_SMALLULONG, value))
    return bytes((CODE_ULONG,)) + struct.pack(">Q", value)


def encode_long(value: int) -> bytes:
    """Encode an AMQP ``long``, preferring the ``smalllong`` compact form."""
    _check_range(value, _INT64_MIN, _INT64_MAX, "long")
    if _INT8_MIN <= value <= _INT8_MAX:
        return bytes((CODE_SMALLLONG,)) + struct.pack(">b", value)
    return bytes((CODE_LONG,)) + struct.pack(">q", value)


def encode_float(value: float) -> bytes:
    """Encode an AMQP ``float`` (IEEE-754 binary32)."""
    return bytes((CODE_FLOAT,)) + struct.pack(">f", value)


def encode_double(value: float) -> bytes:
    """Encode an AMQP ``double`` (IEEE-754 binary64)."""
    return bytes((CODE_DOUBLE,)) + struct.pack(">d", value)


def encode_char(value: str) -> bytes:
    """Encode an AMQP ``char`` as a UTF-32BE code point.

    Args:
        value: A string holding exactly one code point.

    Raises:
        ProtocolError: If ``value`` is not exactly one code point.
    """
    if len(value) != 1:
        raise ProtocolError(f"AMQP char must be exactly one code point, got {len(value)}")
    return bytes((CODE_CHAR,)) + struct.pack(">I", ord(value))


def encode_timestamp(value: int) -> bytes:
    """Encode an AMQP ``timestamp`` (signed 64-bit milliseconds since the Unix epoch)."""
    _check_range(value, _INT64_MIN, _INT64_MAX, "timestamp")
    return bytes((CODE_TIMESTAMP,)) + struct.pack(">q", value)


def encode_uuid(value: uuid_module.UUID) -> bytes:
    """Encode an AMQP ``uuid``."""
    return bytes((CODE_UUID,)) + value.bytes


def _encode_variable(value: bytes, code8: int, code32: int) -> bytes:
    if len(value) <= _UINT8_MAX:
        return bytes((code8, len(value))) + value
    if len(value) > _UINT32_MAX:
        raise ProtocolError(f"variable-width value of {len(value)} bytes exceeds the AMQP maximum")
    return bytes((code32,)) + struct.pack(">I", len(value)) + value


def encode_binary(value: bytes) -> bytes:
    """Encode an AMQP ``binary``, choosing ``vbin8`` or ``vbin32`` by length."""
    return _encode_variable(bytes(value), CODE_VBIN8, CODE_VBIN32)


def encode_string(value: str) -> bytes:
    """Encode an AMQP ``string``, choosing ``str8-utf8`` or ``str32-utf8`` by encoded length."""
    return _encode_variable(value.encode("utf-8"), CODE_STR8, CODE_STR32)


def encode_symbol(value: str) -> bytes:
    """Encode an AMQP ``symbol``, choosing ``sym8`` or ``sym32`` by length."""
    return _encode_variable(value.encode("ascii"), CODE_SYM8, CODE_SYM32)


def _encode_compound(items: Sequence[bytes], count: int, code8: int, code32: int) -> bytes:
    body = b"".join(items)
    # The size field counts the count field itself, hence the +1 / +4.
    if len(body) + 1 <= _UINT8_MAX and count <= _UINT8_MAX:
        return bytes((code8, len(body) + 1, count)) + body
    return bytes((code32,)) + struct.pack(">II", len(body) + 4, count) + body


def encode_list(values: Sequence[Any]) -> bytes:
    """Encode an AMQP ``list``, choosing ``list0``/``list8``/``list32`` by size."""
    return encode_list_of_encoded([encode_value(v) for v in values])


def encode_list_of_encoded(items: Sequence[bytes]) -> bytes:
    """Encode an AMQP ``list`` from already-encoded elements.

    Args:
        items: One fully-encoded (constructor + data) element per list entry.

    Returns:
        The encoded list.
    """
    if not items:
        return bytes((CODE_LIST0,))
    return _encode_compound(items, len(items), CODE_LIST8, CODE_LIST32)


def encode_map_of_encoded(items: Sequence[bytes]) -> bytes:
    """Encode an AMQP ``map`` from already-encoded alternating keys and values.

    Args:
        items: Fully-encoded values in ``key, value, key, value, ...`` order.

    Returns:
        The encoded map.

    Raises:
        ProtocolError: If ``items`` does not hold complete key/value pairs.
    """
    if len(items) % 2 != 0:
        raise ProtocolError(f"map needs an even number of encoded entries, got {len(items)}")
    return _encode_compound(items, len(items), CODE_MAP8, CODE_MAP32)


def encode_map(value: Mapping[Any, Any]) -> bytes:
    """Encode an AMQP ``map``, choosing ``map8`` or ``map32`` by size."""
    items: list[bytes] = []
    for key, item in value.items():
        items.append(encode_value(key))
        items.append(encode_value(item))
    return encode_map_of_encoded(items)


def encode_symbol_map(value: Mapping[str, Any]) -> bytes:
    """Encode an AMQP ``fields``-style map whose keys are symbols."""
    items: list[bytes] = []
    for key, item in value.items():
        items.append(encode_symbol(key))
        items.append(encode_value(item))
    return encode_map_of_encoded(items)


def encode_array(value: Array) -> bytes:
    """Encode an AMQP ``array``, choosing ``array8`` or ``array32`` by size."""
    bodies = [_encode_array_element(value.element_code, item) for item in value.values]
    body = b"".join(bodies)
    count = len(value.values)
    # Size covers the count field, the shared constructor byte and the elements.
    size8 = len(body) + 2
    if size8 <= _UINT8_MAX and count <= _UINT8_MAX:
        return bytes((CODE_ARRAY8, size8, count, value.element_code)) + body
    header = struct.pack(">II", len(body) + 5, count)
    return bytes((CODE_ARRAY32,)) + header + bytes((value.element_code,)) + body


def encode_symbol_array(values: Sequence[str]) -> bytes:
    """Encode a sequence of symbols as an AMQP ``array``.

    The widest element decides the shared constructor: a single symbol longer
    than 255 bytes forces ``sym32`` for every element.
    """
    element_code = CODE_SYM8 if all(len(v.encode("ascii")) <= _UINT8_MAX for v in values) else CODE_SYM32
    return encode_array(Array(element_code, list(values)))


def _encode_array_element(code: int, value: Any) -> bytes:
    if code in (CODE_NULL, CODE_BOOLEAN_TRUE, CODE_BOOLEAN_FALSE):
        return b""
    if code == CODE_BOOLEAN:
        return b"\x01" if value else b"\x00"
    if code == CODE_UBYTE:
        return struct.pack(">B", value)
    if code == CODE_BYTE:
        return struct.pack(">b", value)
    if code == CODE_USHORT:
        return struct.pack(">H", value)
    if code == CODE_SHORT:
        return struct.pack(">h", value)
    if code == CODE_UINT:
        return struct.pack(">I", value)
    if code == CODE_INT:
        return struct.pack(">i", value)
    if code == CODE_ULONG:
        return struct.pack(">Q", value)
    if code in (CODE_LONG, CODE_TIMESTAMP):
        return struct.pack(">q", value)
    if code == CODE_FLOAT:
        return struct.pack(">f", value)
    if code == CODE_DOUBLE:
        return struct.pack(">d", value)
    if code == CODE_CHAR:
        return struct.pack(">I", ord(value))
    if code == CODE_UUID:
        return bytes(value.bytes)
    if code in (CODE_VBIN8, CODE_VBIN32, CODE_STR8, CODE_STR32, CODE_SYM8, CODE_SYM32):
        raw = _array_element_raw(code, value)
        if code in (CODE_VBIN8, CODE_STR8, CODE_SYM8):
            if len(raw) > _UINT8_MAX:
                raise ProtocolError(f"array element of {len(raw)} bytes does not fit an 8-bit width")
            return bytes((len(raw),)) + raw
        return struct.pack(">I", len(raw)) + raw
    raise ProtocolError(f"unsupported AMQP array element constructor 0x{code:02x}")


def _array_element_raw(code: int, value: Any) -> bytes:
    if code in (CODE_VBIN8, CODE_VBIN32):
        return bytes(value)
    if code in (CODE_STR8, CODE_STR32):
        encoded: bytes = str(value).encode("utf-8")
        return encoded
    return str(value).encode("ascii")


def encode_described(descriptor: int | str, encoded_value: bytes) -> bytes:
    """Wrap an already-encoded value in a described-type constructor.

    Args:
        descriptor: Numeric descriptor (encoded as ``ulong``) or symbolic name.
        encoded_value: The fully-encoded described value.

    Returns:
        The encoded described type.
    """
    descriptor_bytes = encode_symbol(descriptor) if isinstance(descriptor, str) else encode_ulong(descriptor)
    return bytes((CODE_DESCRIBED,)) + descriptor_bytes + encoded_value


def encode_described_value(descriptor: int | str, value: Any) -> bytes:
    """Encode ``value`` by inference and wrap it in a described-type constructor."""
    return encode_described(descriptor, encode_value(value))


def encode_described_list(descriptor: int | str, fields: Sequence[bytes | None]) -> bytes:
    """Encode a described list, applying the trailing-field omission rule.

    Performatives, message sections and the ``error`` type are all described
    lists whose fields are positional. A trailing run of absent/default fields
    may be dropped by shortening the list, but an absent field that still has a
    present field after it must be encoded as ``null``.

    Args:
        descriptor: Numeric or symbolic descriptor of the described list.
        fields: One entry per field in index order; ``None`` means the field is
            absent or equal to its default.

    Returns:
        The encoded described list.
    """
    trimmed = list(fields)
    while trimmed and trimmed[-1] is None:
        trimmed.pop()
    items = [NULL_BYTES if item is None else item for item in trimmed]
    return encode_described(descriptor, encode_list_of_encoded(items))


_WRAPPER_ENCODERS: tuple[tuple[type, Any], ...] = (
    (Symbol, encode_symbol),
    (Char, encode_char),
    (Timestamp, encode_timestamp),
    (Ubyte, encode_ubyte),
    (Byte, encode_byte),
    (Ushort, encode_ushort),
    (Short, encode_short),
    (Uint, encode_uint),
    (Int, encode_int),
    (Ulong, encode_ulong),
    (Long, encode_long),
    (Float, encode_float),
    (Double, encode_double),
)


def encode_value(value: Any) -> bytes:
    """Encode any supported Python value, inferring its AMQP type.

    Plain ``int`` values encode as ``int`` when they fit in signed 32 bits and
    as ``long`` otherwise; wrap them in :class:`Uint`, :class:`Ulong`, ... to
    pin a specific width.

    Args:
        value: The value to encode.

    Returns:
        The encoded value, constructor included.

    Raises:
        ProtocolError: If the Python type has no AMQP mapping.
    """
    if value is None:
        return encode_null()
    if isinstance(value, bool):
        return encode_boolean(value)
    for wrapper, encoder in _WRAPPER_ENCODERS:
        if isinstance(value, wrapper):
            result: bytes = encoder(value)
            return result
    if isinstance(value, int):
        return encode_int(value) if _INT32_MIN <= value <= _INT32_MAX else encode_long(value)
    if isinstance(value, float):
        return encode_double(value)
    if isinstance(value, str):
        return encode_string(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return encode_binary(bytes(value))
    if isinstance(value, uuid_module.UUID):
        return encode_uuid(value)
    if isinstance(value, Array):
        return encode_array(value)
    if isinstance(value, Described):
        return encode_described_value(value.descriptor, value.value)
    if isinstance(value, Mapping):
        return encode_map(value)
    if isinstance(value, (list, tuple)):
        return encode_list(value)
    raise ProtocolError(f"cannot encode Python value of type {type(value).__name__} as an AMQP type")


class Decoder:
    """Sequential reader for AMQP-encoded values.

    Args:
        data: Buffer to decode from.
        position: Index of the first byte to read.
    """

    __slots__ = ("_data", "_position")

    def __init__(self, data: bytes | bytearray | memoryview, position: int = 0) -> None:
        self._data = bytes(data)
        self._position = position

    @property
    def position(self) -> int:
        """Index of the next byte to be read."""
        return self._position

    @property
    def remaining(self) -> int:
        """Number of unread bytes left in the buffer."""
        return len(self._data) - self._position

    def read(self, count: int) -> bytes:
        """Read exactly ``count`` raw bytes.

        Raises:
            ProtocolError: If fewer than ``count`` bytes remain.
        """
        if count > self.remaining:
            raise ProtocolError(f"truncated AMQP value: need {count} bytes, {self.remaining} available")
        chunk = self._data[self._position : self._position + count]
        self._position += count
        return chunk

    def read_code(self) -> int:
        """Read a one-byte format code."""
        return self.read(1)[0]

    def read_value(self) -> Any:
        """Read one complete value, constructor included."""
        return self._read_body(self.read_code())

    def _unpack(self, fmt: str, size: int) -> Any:
        return struct.unpack(fmt, self.read(size))[0]

    def _read_body(self, code: int) -> Any:
        if code == CODE_NULL:
            return None
        if code == CODE_BOOLEAN_TRUE:
            return True
        if code == CODE_BOOLEAN_FALSE:
            return False
        if code == CODE_BOOLEAN:
            return self.read(1)[0] != 0
        if code in (CODE_UINT0, CODE_ULONG0):
            return 0
        if code == CODE_UBYTE:
            return self.read(1)[0]
        if code == CODE_BYTE:
            return int(self._unpack(">b", 1))
        if code == CODE_USHORT:
            return int(self._unpack(">H", 2))
        if code == CODE_SHORT:
            return int(self._unpack(">h", 2))
        if code in (CODE_SMALLUINT, CODE_SMALLULONG):
            return self.read(1)[0]
        if code in (CODE_SMALLINT, CODE_SMALLLONG):
            return int(self._unpack(">b", 1))
        if code == CODE_UINT:
            return int(self._unpack(">I", 4))
        if code == CODE_INT:
            return int(self._unpack(">i", 4))
        if code == CODE_ULONG:
            return int(self._unpack(">Q", 8))
        if code == CODE_LONG:
            return int(self._unpack(">q", 8))
        if code == CODE_FLOAT:
            return float(self._unpack(">f", 4))
        if code == CODE_DOUBLE:
            return float(self._unpack(">d", 8))
        if code == CODE_CHAR:
            return chr(int(self._unpack(">I", 4)))
        if code == CODE_TIMESTAMP:
            return Timestamp(self._unpack(">q", 8))
        if code == CODE_UUID:
            return uuid_module.UUID(bytes=self.read(16))
        if code in (CODE_VBIN8, CODE_STR8, CODE_SYM8):
            return self._read_variable(code, self.read(1)[0])
        if code in (CODE_VBIN32, CODE_STR32, CODE_SYM32):
            return self._read_variable(code, int(self._unpack(">I", 4)))
        if code == CODE_LIST0:
            return []
        if code in (CODE_LIST8, CODE_MAP8, CODE_ARRAY8):
            return self._read_compound(code, self.read(1)[0], self.read(1)[0])
        if code in (CODE_LIST32, CODE_MAP32, CODE_ARRAY32):
            return self._read_compound(code, int(self._unpack(">I", 4)), int(self._unpack(">I", 4)))
        if code == CODE_DESCRIBED:
            descriptor = self.read_value()
            return Described(descriptor, self.read_value())
        raise ProtocolError(f"unknown AMQP format code 0x{code:02x}")

    def _read_variable(self, code: int, length: int) -> Any:
        raw = self.read(length)
        if code in (CODE_VBIN8, CODE_VBIN32):
            return raw
        if code in (CODE_STR8, CODE_STR32):
            return raw.decode("utf-8")
        return Symbol(raw.decode("ascii"))

    def _read_compound(self, code: int, size: int, count: int) -> Any:
        # `size` is a hint used only to reject a declared length that could
        # never fit in the buffer; the true end of the compound value is
        # wherever parsing its `count` self-describing elements actually
        # lands. Some producers get the size field's byte-count wrong (e.g.
        # forgetting the count field's own width), so trusting the declared
        # `size` to reposition the cursor would silently truncate or corrupt
        # a well-formed trailing element instead of reading it correctly.
        width = 1 if code in (CODE_LIST8, CODE_MAP8, CODE_ARRAY8) else 4
        end = self._position + size - width
        if end > len(self._data):
            raise ProtocolError(f"truncated AMQP compound value: declared size {size} overruns the buffer")
        if code in (CODE_ARRAY8, CODE_ARRAY32):
            element_code = self.read_code()
            return [self._read_body(element_code) for _ in range(count)]
        if code in (CODE_MAP8, CODE_MAP32):
            if count % 2 != 0:
                raise ProtocolError(f"AMQP map has an odd element count ({count})")
            result: dict[Any, Any] = {}
            for _ in range(count // 2):
                key = self.read_value()
                result[key] = self.read_value()
            return result
        return [self.read_value() for _ in range(count)]


def decode_value(data: bytes | bytearray | memoryview) -> Any:
    """Decode the first complete AMQP value in ``data``, ignoring trailing bytes."""
    return Decoder(data).read_value()


def read_described_list(decoder: Decoder) -> tuple[int | str, list[Any]]:
    """Read one described list and return its descriptor and field values.

    Args:
        decoder: Decoder positioned at the described-type constructor.

    Returns:
        The descriptor and the field values in index order. Fields omitted by
        the trailing-omission rule are simply absent from the list.

    Raises:
        ProtocolError: If the value is not a described list.
    """
    code = decoder.read_code()
    if code != CODE_DESCRIBED:
        raise ProtocolError(f"expected a described type (0x00), got format code 0x{code:02x}")
    descriptor = decoder.read_value()
    if not isinstance(descriptor, (int, str)):
        raise ProtocolError(f"unsupported descriptor type {type(descriptor).__name__}")
    body = decoder.read_value()
    if body is None:
        body = []
    if not isinstance(body, list):
        raise ProtocolError(f"expected a list body for descriptor {descriptor!r}, got {type(body).__name__}")
    return descriptor, body


def decode_described_list(data: bytes | bytearray | memoryview) -> tuple[int | str, list[Any]]:
    """Decode one described list from the start of ``data``."""
    return read_described_list(Decoder(data))


def peek_descriptor(data: bytes | bytearray | memoryview) -> int | str:
    """Return the descriptor of the described type at the start of ``data``."""
    decoder = Decoder(data)
    code = decoder.read_code()
    if code != CODE_DESCRIBED:
        raise ProtocolError(f"expected a described type (0x00), got format code 0x{code:02x}")
    descriptor = decoder.read_value()
    if not isinstance(descriptor, (int, str)):
        raise ProtocolError(f"unsupported descriptor type {type(descriptor).__name__}")
    return descriptor


def descriptor_code(descriptor: int | str, symbolic_names: Mapping[str, int]) -> int:
    """Reduce a descriptor to its numeric code.

    The high 32 bits of every AMQP-defined descriptor are zero, so only the low
    32 bits are compared; peers are also allowed to send the equivalent
    symbolic name instead of the numeric code.

    Args:
        descriptor: Numeric or symbolic descriptor as decoded from the wire.
        symbolic_names: Mapping of accepted symbolic names to numeric codes.

    Returns:
        The numeric descriptor code.

    Raises:
        ProtocolError: If a symbolic descriptor is not in ``symbolic_names``.
    """
    if isinstance(descriptor, int):
        return descriptor & _UINT32_MAX
    code = symbolic_names.get(descriptor)
    if code is None:
        raise ProtocolError(f"unknown symbolic descriptor {descriptor!r}")
    return code


def field_at(values: Sequence[Any], index: int, default: Any = None) -> Any:
    """Return field ``index`` of a decoded described list.

    Args:
        values: Decoded field values, possibly shortened by trailing omission.
        index: Zero-based field index.
        default: Value to use when the field is absent or ``null``.

    Returns:
        The field value, or ``default`` when the field is missing or ``null``.
    """
    if index >= len(values) or values[index] is None:
        return default
    return values[index]


def as_symbol_list(value: Any) -> list[str] | None:
    """Normalize a "symbol, multiple" field to a list of plain strings.

    A multiple field may arrive as a single symbol, as an array/list of
    symbols, or absent.

    Raises:
        ProtocolError: If the value is neither a symbol nor a list of symbols.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return [str(value)]
    if isinstance(value, list):
        return [str(item) for item in value]
    raise ProtocolError(f"expected a symbol or list of symbols, got {type(value).__name__}")


def as_dict(value: Any) -> dict[Any, Any] | None:
    """Normalize a decoded map field to a ``dict``.

    Raises:
        ProtocolError: If the value is present but not a map.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    raise ProtocolError(f"expected a map, got {type(value).__name__}")
