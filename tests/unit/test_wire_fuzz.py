"""Property-based (Hypothesis) fuzz tests for the AMQP 1.0 wire codec.

The rest of ``tests/unit`` is example-based: every case is a value someone
(human or AI) thought to write down. These tests instead check invariants
that must hold for *any* input, which is the only way to exercise byte
patterns nobody thought to hand-pick:

* **Round trip** - ``decode_value(encode_value(x)) == x`` for arbitrary,
  possibly deeply nested Python values.
* **Crash resistance** - decoding arbitrary or mutated-but-otherwise-valid
  bytes must only ever raise :class:`ProtocolError`. Any other exception
  means a hostile or buggy peer can crash the client instead of having its
  frame rejected.

Requires the ``hypothesis`` dev dependency (``pip install -e ".[dev]"``).
"""

from __future__ import annotations

import math
import struct
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rabbitmq_amqp_python_client.exceptions import ProtocolError
from rabbitmq_amqp_python_client.wire import encoding as enc
from rabbitmq_amqp_python_client.wire.frames import FRAME_TYPE_AMQP, FRAME_TYPE_SASL, decode_frame_body

# --- Strategies for values `encode_value`/`decode_value` round-trip ---


def _wrapped_int(wrapper: type, low: int, high: int) -> st.SearchStrategy[Any]:
    return st.integers(min_value=low, max_value=high).map(wrapper)


_ASCII_SYMBOL_TEXT = st.text(alphabet=st.characters(min_codepoint=0x21, max_codepoint=0x7E), max_size=32)

_SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-0x80000000, max_value=0x7FFFFFFF),  # plain int -> AMQP int
    st.integers(min_value=-0x8000000000000000, max_value=0x7FFFFFFFFFFFFFFF),  # plain int -> AMQP long
    _wrapped_int(enc.Ubyte, 0, 0xFF),
    _wrapped_int(enc.Byte, -0x80, 0x7F),
    _wrapped_int(enc.Ushort, 0, 0xFFFF),
    _wrapped_int(enc.Short, -0x8000, 0x7FFF),
    _wrapped_int(enc.Uint, 0, 0xFFFFFFFF),
    _wrapped_int(enc.Int, -0x80000000, 0x7FFFFFFF),
    _wrapped_int(enc.Ulong, 0, 0xFFFFFFFFFFFFFFFF),
    _wrapped_int(enc.Long, -0x8000000000000000, 0x7FFFFFFFFFFFFFFF),
    _wrapped_int(enc.Timestamp, -0x8000000000000000, 0x7FFFFFFFFFFFFFFF),
    _ASCII_SYMBOL_TEXT.map(enc.Symbol),
    st.characters(max_codepoint=0x10FFFF).map(enc.Char),
    st.integers(min_value=0, max_value=0xFFFFFFFF).map(
        lambda bits: enc.Float(struct.unpack(">f", struct.pack(">I", bits))[0])
    ),
    st.floats(allow_nan=True, allow_infinity=True).map(enc.Double),
    st.text(max_size=32),
    st.binary(max_size=32),
    st.uuids(),
)

_VALUES = st.recursive(
    _SCALARS,
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(st.text(max_size=16), children, max_size=5),
    ),
    max_leaves=20,
)


def _values_equal(left: Any, right: Any) -> bool:
    """Compare decoded/original values, treating NaN as equal to itself."""
    if isinstance(left, float) and isinstance(right, float) and math.isnan(left) and math.isnan(right):
        return True
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(_values_equal(a, b) for a, b in zip(left, right, strict=True))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_values_equal(left[key], right[key]) for key in left)
    return bool(left == right)


@given(value=_VALUES)
@settings(max_examples=300, deadline=None)
def test_round_trip_preserves_arbitrary_values(value: Any) -> None:
    encoded = enc.encode_value(value)
    decoded = enc.decode_value(encoded)
    assert _values_equal(decoded, value), f"round trip mismatch: {value!r} -> {decoded!r}"


# --- Crash resistance: a hostile/corrupt peer must only ever get a ProtocolError ---


@given(data=st.binary(max_size=256))
@settings(max_examples=500, deadline=None)
def test_decode_value_never_leaks_a_non_protocol_error(data: bytes) -> None:
    try:
        enc.decode_value(data)
    except ProtocolError:
        pass
    except Exception as exc:  # noqa: BLE001 - intentionally broad: this is the property under test
        pytest.fail(f"decode_value raised {type(exc).__name__} instead of ProtocolError for {data!r}: {exc!r}")


@given(value=_VALUES, mutations=st.lists(st.tuples(st.integers(min_value=0), st.integers(0, 255)), max_size=6))
@settings(max_examples=300, deadline=None)
def test_decode_value_never_leaks_a_non_protocol_error_on_mutated_input(
    value: Any, mutations: list[tuple[int, int]]
) -> None:
    """Byte-flip a validly encoded value: more likely than raw random bytes to
    reach the compound/variable-width/described decode paths."""
    encoded = bytearray(enc.encode_value(value))
    if not encoded:
        return
    for index, byte_value in mutations:
        encoded[index % len(encoded)] = byte_value
    try:
        enc.decode_value(bytes(encoded))
    except ProtocolError:
        pass
    except Exception as exc:  # noqa: BLE001 - intentionally broad: this is the property under test
        pytest.fail(
            f"decode_value raised {type(exc).__name__} instead of ProtocolError for {bytes(encoded)!r}: {exc!r}"
        )


@given(frame_type=st.sampled_from([FRAME_TYPE_AMQP, FRAME_TYPE_SASL]), body=st.binary(max_size=256))
@settings(max_examples=500, deadline=None)
def test_decode_frame_body_never_leaks_a_non_protocol_error(frame_type: int, body: bytes) -> None:
    try:
        decode_frame_body(frame_type, body)
    except ProtocolError:
        pass
    except Exception as exc:  # noqa: BLE001 - intentionally broad: this is the property under test
        pytest.fail(f"decode_frame_body raised {type(exc).__name__} instead of ProtocolError for {body!r}: {exc!r}")


# --- Regression tests for the two resource-exhaustion bugs this fuzzing found ---


class TestDecoderResourceLimits:
    """A hostile peer must not be able to turn a small frame into unbounded work.

    These reproduce two bugs found while writing the properties above:
    `_read_compound` did not cap an array's declared element count against
    anything but the buffer, and `read_value`'s recursion had no depth limit.
    """

    def test_array_of_null_rejects_a_count_that_would_allocate_millions_of_elements(self):
        # 10 bytes on the wire; MAX_COMPOUND_COUNT + 1 declared `null` elements
        # that (before the fix) cost zero further bytes each, so the array
        # would happily build a multi-million-entry list from this alone.
        count = enc.MAX_COMPOUND_COUNT + 1
        header = struct.pack(">II", 5, count)  # size = count-field width + constructor byte
        data = bytes((enc.CODE_ARRAY32,)) + header + bytes((enc.CODE_NULL,))
        with pytest.raises(ProtocolError, match="exceeding"):
            enc.decode_value(data)

    def test_array_of_null_at_the_count_limit_still_decodes(self):
        count = enc.MAX_COMPOUND_COUNT
        header = struct.pack(">II", 5, count)
        data = bytes((enc.CODE_ARRAY32,)) + header + bytes((enc.CODE_NULL,))
        assert enc.decode_value(data) == [None] * count

    def test_a_long_chain_of_described_types_is_rejected_before_the_stack_overflows(self):
        # Each 0x00 opens another described-type level; without a depth limit
        # this recurses through `read_value` until CPython raises an uncaught
        # RecursionError instead of a ProtocolError.
        chain = bytes((enc.CODE_DESCRIBED,)) * 4000 + enc.NULL_BYTES * 2
        with pytest.raises(ProtocolError, match="nesting"):
            enc.decode_value(chain)

    def test_nesting_well_under_the_depth_limit_still_round_trips(self):
        value: Any = None
        for _ in range(40):
            value = enc.Described(1, value)
        assert enc.decode_value(enc.encode_value(value)) == value

    def test_nesting_well_over_the_depth_limit_is_rejected(self):
        value: Any = None
        for _ in range(200):
            value = enc.Described(1, value)
        with pytest.raises(ProtocolError, match="nesting"):
            enc.decode_value(enc.encode_value(value))
