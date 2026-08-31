"""Unit tests for the AMQP 1.0 primitive type codec."""

from __future__ import annotations

import math
import struct
import uuid

import pytest

from rabbitmq_amqp_python_client.exceptions import ProtocolError
from rabbitmq_amqp_python_client.wire import encoding as enc


def round_trip(encoded: bytes):
    """Decode ``encoded`` and assert it was consumed entirely."""
    decoder = enc.Decoder(encoded)
    value = decoder.read_value()
    assert decoder.remaining == 0, "decoder did not consume the whole buffer"
    return value


class TestNullAndBoolean:
    def test_null_round_trip(self):
        assert enc.encode_null() == b"\x40"
        assert round_trip(enc.encode_null()) is None

    @pytest.mark.parametrize(("value", "code"), [(True, 0x41), (False, 0x42)])
    def test_boolean_uses_single_byte_constructors(self, value, code):
        encoded = enc.encode_boolean(value)
        assert encoded == bytes((code,))
        assert round_trip(encoded) is value

    @pytest.mark.parametrize(("payload", "expected"), [(b"\x00", False), (b"\x01", True)])
    def test_decodes_wide_boolean_form(self, payload, expected):
        assert round_trip(bytes((0x56,)) + payload) is expected


class TestIntegers:
    @pytest.mark.parametrize("value", [0, 1, 127, 128, 255])
    def test_ubyte_round_trip(self, value):
        encoded = enc.encode_ubyte(value)
        assert encoded[0] == 0x50
        assert round_trip(encoded) == value

    @pytest.mark.parametrize("value", [-128, -1, 0, 1, 127])
    def test_byte_round_trip(self, value):
        encoded = enc.encode_byte(value)
        assert encoded[0] == 0x51
        assert round_trip(encoded) == value

    @pytest.mark.parametrize("value", [0, 1, 255, 256, 65535])
    def test_ushort_round_trip(self, value):
        encoded = enc.encode_ushort(value)
        assert encoded[0] == 0x60
        assert round_trip(encoded) == value

    @pytest.mark.parametrize("value", [-32768, -1, 0, 1, 32767])
    def test_short_round_trip(self, value):
        encoded = enc.encode_short(value)
        assert encoded[0] == 0x61
        assert round_trip(encoded) == value

    @pytest.mark.parametrize(
        ("value", "expected_code", "expected_length"),
        [
            (0, 0x43, 1),
            (1, 0x52, 2),
            (255, 0x52, 2),
            (256, 0x70, 5),
            (0xFFFFFFFF, 0x70, 5),
        ],
    )
    def test_uint_picks_the_compact_form(self, value, expected_code, expected_length):
        encoded = enc.encode_uint(value)
        assert encoded[0] == expected_code
        assert len(encoded) == expected_length
        assert round_trip(encoded) == value

    @pytest.mark.parametrize(
        ("value", "expected_code", "expected_length"),
        [
            (-129, 0x71, 5),
            (-128, 0x54, 2),
            (0, 0x54, 2),
            (127, 0x54, 2),
            (128, 0x71, 5),
            (-2147483648, 0x71, 5),
            (2147483647, 0x71, 5),
        ],
    )
    def test_int_picks_the_compact_form(self, value, expected_code, expected_length):
        encoded = enc.encode_int(value)
        assert encoded[0] == expected_code
        assert len(encoded) == expected_length
        assert round_trip(encoded) == value

    @pytest.mark.parametrize(
        ("value", "expected_code", "expected_length"),
        [
            (0, 0x44, 1),
            (1, 0x53, 2),
            (255, 0x53, 2),
            (256, 0x80, 9),
            (0xFFFFFFFFFFFFFFFF, 0x80, 9),
        ],
    )
    def test_ulong_picks_the_compact_form(self, value, expected_code, expected_length):
        encoded = enc.encode_ulong(value)
        assert encoded[0] == expected_code
        assert len(encoded) == expected_length
        assert round_trip(encoded) == value

    @pytest.mark.parametrize(
        ("value", "expected_code", "expected_length"),
        [
            (-129, 0x81, 9),
            (-128, 0x55, 2),
            (0, 0x55, 2),
            (127, 0x55, 2),
            (128, 0x81, 9),
            (-(2**63), 0x81, 9),
            (2**63 - 1, 0x81, 9),
        ],
    )
    def test_long_picks_the_compact_form(self, value, expected_code, expected_length):
        encoded = enc.encode_long(value)
        assert encoded[0] == expected_code
        assert len(encoded) == expected_length
        assert round_trip(encoded) == value

    def test_multi_byte_integers_are_big_endian(self):
        assert enc.encode_uint(0x01020304) == b"\x70\x01\x02\x03\x04"
        assert enc.encode_ushort(0x0102) == b"\x60\x01\x02"
        assert enc.encode_ulong(0x0102030405060708) == b"\x80\x01\x02\x03\x04\x05\x06\x07\x08"

    @pytest.mark.parametrize(
        ("encoder", "value"),
        [
            (enc.encode_ubyte, 256),
            (enc.encode_ubyte, -1),
            (enc.encode_byte, 128),
            (enc.encode_ushort, 65536),
            (enc.encode_uint, -1),
            (enc.encode_uint, 2**32),
            (enc.encode_int, 2**31),
            (enc.encode_ulong, -1),
            (enc.encode_long, 2**63),
        ],
    )
    def test_out_of_range_values_are_rejected(self, encoder, value):
        with pytest.raises(ProtocolError, match="out of range"):
            encoder(value)


class TestFloatingPointAndScalars:
    @pytest.mark.parametrize("value", [0.0, 1.5, -2.25, 1e10])
    def test_double_round_trip(self, value):
        encoded = enc.encode_double(value)
        assert encoded[0] == 0x82
        assert round_trip(encoded) == value

    @pytest.mark.parametrize("value", [0.0, 1.5, -2.25])
    def test_float_round_trip(self, value):
        encoded = enc.encode_float(value)
        assert encoded[0] == 0x72
        assert len(encoded) == 5
        assert round_trip(encoded) == pytest.approx(value)

    def test_float_keeps_nan(self):
        assert math.isnan(round_trip(enc.encode_float(float("nan"))))

    @pytest.mark.parametrize("value", ["a", "é", "\U0001f600"])
    def test_char_round_trip_uses_utf32be(self, value):
        encoded = enc.encode_char(value)
        assert encoded[0] == 0x73
        assert encoded[1:] == struct.pack(">I", ord(value))
        assert round_trip(encoded) == value

    @pytest.mark.parametrize("value", ["", "ab"])
    def test_char_rejects_wrong_length(self, value):
        with pytest.raises(ProtocolError, match="exactly one code point"):
            enc.encode_char(value)

    @pytest.mark.parametrize("value", [0, 1700000000000, -1])
    def test_timestamp_round_trip(self, value):
        encoded = enc.encode_timestamp(value)
        assert encoded[0] == 0x83
        assert round_trip(encoded) == value

    def test_timestamp_decodes_to_the_timestamp_wrapper(self):
        assert isinstance(round_trip(enc.encode_timestamp(5)), enc.Timestamp)

    def test_uuid_round_trip(self):
        value = uuid.uuid4()
        encoded = enc.encode_uuid(value)
        assert encoded[0] == 0x98
        assert len(encoded) == 17
        assert round_trip(encoded) == value


class TestVariableWidth:
    @pytest.mark.parametrize(
        ("length", "expected_code"),
        [(0, 0xA0), (1, 0xA0), (255, 0xA0), (256, 0xB0)],
    )
    def test_binary_picks_vbin8_or_vbin32(self, length, expected_code):
        value = b"x" * length
        encoded = enc.encode_binary(value)
        assert encoded[0] == expected_code
        assert round_trip(encoded) == value

    @pytest.mark.parametrize(
        ("length", "expected_code"),
        [(0, 0xA1), (1, 0xA1), (255, 0xA1), (256, 0xB1)],
    )
    def test_string_picks_str8_or_str32(self, length, expected_code):
        value = "x" * length
        encoded = enc.encode_string(value)
        assert encoded[0] == expected_code
        assert round_trip(encoded) == value

    def test_string_length_counts_utf8_bytes_not_code_points(self):
        # 128 two-byte code points are 256 bytes, which no longer fits str8.
        value = "é" * 128
        encoded = enc.encode_string(value)
        assert encoded[0] == 0xB1
        assert round_trip(encoded) == value

    @pytest.mark.parametrize(
        ("length", "expected_code"),
        [(0, 0xA3), (255, 0xA3), (256, 0xB3)],
    )
    def test_symbol_picks_sym8_or_sym32(self, length, expected_code):
        value = "s" * length
        encoded = enc.encode_symbol(value)
        assert encoded[0] == expected_code
        decoded = round_trip(encoded)
        assert decoded == value
        assert isinstance(decoded, enc.Symbol)

    def test_symbol_rejects_non_ascii(self):
        with pytest.raises(UnicodeEncodeError):
            enc.encode_symbol("café")


class TestCompound:
    def test_empty_list_uses_list0(self):
        encoded = enc.encode_list([])
        assert encoded == b"\x45"
        assert round_trip(encoded) == []

    def test_small_list_uses_list8(self):
        encoded = enc.encode_list([1, "two", None, True])
        assert encoded[0] == 0xC0
        assert round_trip(encoded) == [1, "two", None, True]

    def test_large_list_uses_list32(self):
        values = [f"item-{index:03d}" for index in range(40)]
        encoded = enc.encode_list(values)
        assert encoded[0] == 0xD0
        assert round_trip(encoded) == values

    def test_list_size_and_count_fields(self):
        encoded = enc.encode_list([1, 2])
        size, count = encoded[1], encoded[2]
        assert count == 2
        assert size == len(encoded) - 2

    def test_many_element_list_uses_list32_even_when_small(self):
        # 300 one-byte elements overflow the 8-bit count field.
        values = [0] * 300
        encoded = enc.encode_list(values)
        assert encoded[0] == 0xD0
        assert round_trip(encoded) == values

    def test_empty_map_round_trip(self):
        encoded = enc.encode_map({})
        assert encoded[0] == 0xC1
        assert round_trip(encoded) == {}

    def test_map_round_trip(self):
        value = {"a": 1, "b": "two", "c": None}
        encoded = enc.encode_map(value)
        assert encoded[0] == 0xC1
        assert round_trip(encoded) == value

    def test_map_count_is_twice_the_entry_count(self):
        encoded = enc.encode_map({"a": 1, "b": 2})
        assert encoded[2] == 4

    def test_large_map_uses_map32(self):
        value = {f"key-{index:03d}": f"value-{index:03d}" for index in range(30)}
        encoded = enc.encode_map(value)
        assert encoded[0] == 0xD1
        assert round_trip(encoded) == value

    def test_symbol_map_keys_decode_as_symbols(self):
        encoded = enc.encode_symbol_map({"rabbitmq:key": "value"})
        decoded = round_trip(encoded)
        assert decoded == {"rabbitmq:key": "value"}
        assert all(isinstance(key, enc.Symbol) for key in decoded)

    def test_nested_compounds_round_trip(self):
        value = {"outer": [1, {"inner": [True, None]}]}
        assert round_trip(enc.encode_map(value)) == value

    def test_map_with_odd_element_count_is_rejected(self):
        malformed = bytes((0xC1, 0x02, 0x01, 0x40))
        with pytest.raises(ProtocolError, match="odd element count"):
            enc.decode_value(malformed)

    def test_list32_tolerates_a_declared_size_shorter_than_its_true_content(self):
        # A real producer quirk (seen in 004_amqp10_validation's
        # message_from_version_1_0_0.amqp fixture): the size field undercounts
        # by exactly the count field's own width. Each element is still
        # self-describing, so the reader must trust how many bytes parsing
        # `count` of them actually took, not the (here, wrong) declared size.
        item = enc.encode_string("abcdefgh")
        correct_size = len(item) + 4
        undercounted_size = correct_size - 4
        encoded = bytes((enc.CODE_LIST32,)) + struct.pack(">II", undercounted_size, 1) + item
        assert round_trip(encoded) == ["abcdefgh"]


class TestArrays:
    def test_symbol_array_uses_array8_and_one_constructor(self):
        encoded = enc.encode_symbol_array(["ONE", "TWO"])
        assert encoded[0] == 0xE0
        assert encoded[2] == 2
        assert encoded[3] == 0xA3
        assert round_trip(encoded) == ["ONE", "TWO"]

    def test_empty_symbol_array_round_trip(self):
        assert round_trip(enc.encode_symbol_array([])) == []

    def test_symbol_array_switches_to_sym32_for_long_elements(self):
        values = ["short", "s" * 300]
        encoded = enc.encode_symbol_array(values)
        assert encoded[0] == 0xF0
        assert round_trip(encoded) == values

    def test_uint_array_round_trip(self):
        encoded = enc.encode_array(enc.Array(enc.CODE_UINT, [0, 1, 2**32 - 1]))
        assert encoded[0] == 0xE0
        assert round_trip(encoded) == [0, 1, 2**32 - 1]

    def test_large_array_uses_array32(self):
        values = list(range(100))
        encoded = enc.encode_array(enc.Array(enc.CODE_ULONG, values))
        assert encoded[0] == 0xF0
        assert round_trip(encoded) == values

    def test_boolean_array_has_no_element_bodies(self):
        encoded = enc.encode_array(enc.Array(enc.CODE_BOOLEAN, [True, False, True]))
        assert round_trip(encoded) == [True, False, True]

    def test_null_array_round_trip(self):
        encoded = enc.encode_array(enc.Array(enc.CODE_NULL, [None, None]))
        assert round_trip(encoded) == [None, None]

    def test_unsupported_array_element_constructor_is_rejected(self):
        with pytest.raises(ProtocolError, match="unsupported AMQP array element constructor"):
            enc.encode_array(enc.Array(enc.CODE_LIST8, [[1]]))


class TestDescribedTypes:
    def test_described_value_round_trip(self):
        encoded = enc.encode_described_value(0x77, "payload")
        assert encoded[0] == 0x00
        decoded = round_trip(encoded)
        assert decoded == enc.Described(0x77, "payload")

    def test_symbolic_descriptor_round_trip(self):
        decoded = round_trip(enc.encode_described_value("amqp:my-type:list", [1, 2]))
        assert decoded.descriptor == "amqp:my-type:list"
        assert decoded.value == [1, 2]

    def test_descriptor_uses_smallulong_for_small_codes(self):
        assert enc.encode_described_value(0x10, None)[:3] == b"\x00\x53\x10"

    def test_trailing_none_fields_are_omitted(self):
        encoded = enc.encode_described_list(0x10, [enc.encode_string("id"), None, None])
        descriptor, values = enc.decode_described_list(encoded)
        assert descriptor == 0x10
        assert values == ["id"]

    def test_absent_field_before_a_present_one_is_encoded_as_null(self):
        encoded = enc.encode_described_list(0x10, [enc.encode_string("id"), None, enc.encode_uint(7)])
        descriptor, values = enc.decode_described_list(encoded)
        assert descriptor == 0x10
        assert values == ["id", None, 7]
        assert b"\x40" in encoded

    def test_all_none_fields_encode_as_an_empty_list(self):
        encoded = enc.encode_described_list(0x17, [None, None])
        assert encoded == b"\x00\x53\x17\x45"
        assert enc.decode_described_list(encoded) == (0x17, [])

    def test_field_at_returns_defaults_for_missing_and_null_fields(self):
        values = ["present", None]
        assert enc.field_at(values, 0) == "present"
        assert enc.field_at(values, 1, "fallback") == "fallback"
        assert enc.field_at(values, 9, "fallback") == "fallback"

    def test_read_described_list_rejects_a_non_described_value(self):
        with pytest.raises(ProtocolError, match="expected a described type"):
            enc.decode_described_list(enc.encode_list([1]))

    def test_read_described_list_rejects_a_non_list_body(self):
        with pytest.raises(ProtocolError, match="expected a list body"):
            enc.decode_described_list(enc.encode_described_value(0x10, "not-a-list"))

    def test_peek_descriptor(self):
        assert enc.peek_descriptor(enc.encode_described_list(0x12, [])) == 0x12

    def test_descriptor_code_masks_the_high_word(self):
        assert enc.descriptor_code(0x0000000000000010, {}) == 0x10

    def test_descriptor_code_resolves_symbolic_names(self):
        assert enc.descriptor_code("amqp:open:list", {"amqp:open:list": 0x10}) == 0x10

    def test_descriptor_code_rejects_unknown_symbolic_names(self):
        with pytest.raises(ProtocolError, match="unknown symbolic descriptor"):
            enc.descriptor_code("amqp:nope:list", {})


class TestEncodeValueInference:
    @pytest.mark.parametrize(
        ("value", "expected_code"),
        [
            (None, 0x40),
            (True, 0x41),
            (7, 0x54),
            (2**40, 0x81),
            (1.5, 0x82),
            ("text", 0xA1),
            (b"bytes", 0xA0),
            (enc.Symbol("sym"), 0xA3),
            (enc.Ubyte(3), 0x50),
            (enc.Ushort(3), 0x60),
            (enc.Uint(300), 0x70),
            (enc.Ulong(3), 0x53),
            (enc.Byte(-3), 0x51),
            (enc.Short(-3), 0x61),
            (enc.Int(-3), 0x54),
            (enc.Long(-3), 0x55),
            (enc.Float(1.5), 0x72),
            (enc.Double(1.5), 0x82),
            (enc.Timestamp(5), 0x83),
            (enc.Char("c"), 0x73),
            ([1], 0xC0),
            ({"a": 1}, 0xC1),
        ],
    )
    def test_inferred_constructor(self, value, expected_code):
        assert enc.encode_value(value)[0] == expected_code

    def test_bool_is_not_treated_as_an_integer(self):
        assert enc.encode_value(False) == b"\x42"

    def test_bytearray_and_memoryview_encode_as_binary(self):
        assert enc.encode_value(bytearray(b"ab")) == enc.encode_binary(b"ab")
        assert enc.encode_value(memoryview(b"ab")) == enc.encode_binary(b"ab")

    def test_tuple_encodes_as_a_list(self):
        assert round_trip(enc.encode_value((1, 2))) == [1, 2]

    def test_described_wrapper_round_trip(self):
        value = enc.Described(0x24, [])
        assert round_trip(enc.encode_value(value)) == value

    def test_unsupported_type_is_rejected(self):
        with pytest.raises(ProtocolError, match="cannot encode Python value"):
            enc.encode_value(object())


class TestDecoderErrors:
    def test_truncated_value_is_rejected(self):
        with pytest.raises(ProtocolError, match="truncated AMQP value"):
            enc.decode_value(b"\x70\x00\x01")

    def test_unknown_format_code_is_rejected(self):
        with pytest.raises(ProtocolError, match="unknown AMQP format code 0x2a"):
            enc.decode_value(b"\x2a")

    def test_compound_size_overrunning_the_buffer_is_rejected(self):
        with pytest.raises(ProtocolError, match="overruns the buffer"):
            enc.decode_value(bytes((0xC0, 0x40, 0x01, 0x41)))

    def test_decoder_tracks_position_and_ignores_trailing_bytes(self):
        decoder = enc.Decoder(enc.encode_uint(1) + b"trailing")
        assert decoder.read_value() == 1
        assert decoder.position == len(enc.encode_uint(1))
        assert decoder.remaining == len(b"trailing")

    def test_read_beyond_the_buffer_is_rejected(self):
        decoder = enc.Decoder(b"ab")
        with pytest.raises(ProtocolError, match="need 3 bytes, 2 available"):
            decoder.read(3)


class TestNormalizers:
    def test_as_symbol_list_accepts_a_single_symbol(self):
        assert enc.as_symbol_list(enc.Symbol("ONE")) == ["ONE"]

    def test_as_symbol_list_accepts_a_list(self):
        assert enc.as_symbol_list([enc.Symbol("ONE"), enc.Symbol("TWO")]) == ["ONE", "TWO"]

    def test_as_symbol_list_passes_through_none(self):
        assert enc.as_symbol_list(None) is None

    def test_as_symbol_list_rejects_other_types(self):
        with pytest.raises(ProtocolError, match="expected a symbol or list of symbols"):
            enc.as_symbol_list(7)

    def test_as_dict_passes_through_none_and_dicts(self):
        assert enc.as_dict(None) is None
        assert enc.as_dict({"a": 1}) == {"a": 1}

    def test_as_dict_rejects_other_types(self):
        with pytest.raises(ProtocolError, match="expected a map"):
            enc.as_dict([1])
