"""Unit tests for the AMQP 1.0 message sections and the ``Message`` container."""

from __future__ import annotations

import uuid

import pytest

from src.exceptions import ProtocolError
from src.wire import encoding as enc
from src.wire import message as msg


def fields_of(encoded: bytes) -> list:
    """Return the decoded positional fields of an encoded described list."""
    _, values = enc.decode_described_list(encoded)
    return values


class TestHeader:
    def test_round_trip_with_all_fields(self):
        header = msg.Header(durable=True, priority=7, ttl=60000, first_acquirer=True, delivery_count=3)
        assert msg.decode_section(header.encode()) == header

    def test_round_trip_with_defaults(self):
        header = msg.Header()
        assert msg.decode_section(header.encode()) == header
        assert header.encode() == b"\x00\x53\x70\x45"

    def test_descriptor_is_0x70(self):
        assert enc.peek_descriptor(msg.Header(durable=True).encode()) == 0x70

    def test_default_priority_is_omitted(self):
        assert fields_of(msg.Header(durable=True).encode()) == [True]

    def test_ttl_after_a_default_priority_becomes_null(self):
        header = msg.Header(ttl=1000)
        assert fields_of(header.encode()) == [None, None, 1000]
        assert msg.decode_section(header.encode()) == header

    def test_priority_is_encoded_as_a_ubyte(self):
        assert b"\x50\x09" in msg.Header(priority=9).encode()


class TestProperties:
    def test_round_trip_with_all_fields(self):
        properties = msg.Properties(
            message_id="id-1",
            user_id=b"guest",
            to="/queues/q",
            subject="hello",
            reply_to="/queues/reply",
            correlation_id="corr-1",
            content_type="text/plain",
            content_encoding="identity",
            absolute_expiry_time=1700000001000,
            creation_time=1700000000000,
            group_id="group-1",
            group_sequence=4,
            reply_to_group_id="group-2",
        )
        assert msg.decode_section(properties.encode()) == properties

    def test_round_trip_when_empty(self):
        assert msg.decode_section(msg.Properties().encode()) == msg.Properties()

    def test_descriptor_is_0x73(self):
        assert enc.peek_descriptor(msg.Properties(subject="s").encode()) == 0x73

    def test_subject_after_absent_fields_becomes_null(self):
        properties = msg.Properties(subject="hello")
        assert fields_of(properties.encode()) == [None, None, None, "hello"]

    def test_content_type_is_encoded_as_a_symbol(self):
        values = fields_of(msg.Properties(content_type="application/json").encode())
        assert isinstance(values[6], enc.Symbol)

    def test_timestamps_are_encoded_as_timestamps(self):
        properties = msg.Properties(creation_time=1700000000000)
        assert b"\x83" in properties.encode()
        assert msg.decode_section(properties.encode()).creation_time == 1700000000000

    @pytest.mark.parametrize("message_id", ["text-id", b"binary-id", 12345, uuid.uuid4()])
    def test_message_id_accepts_every_allowed_type(self, message_id):
        properties = msg.Properties(message_id=message_id)
        assert msg.decode_section(properties.encode()).message_id == message_id

    def test_integer_message_id_is_encoded_as_a_ulong(self):
        assert msg.encode_message_id(5) == enc.encode_ulong(5)

    def test_message_id_rejects_invalid_types(self):
        with pytest.raises(ProtocolError, match="invalid message-id type"):
            msg.encode_message_id(1.5)

    def test_message_id_rejects_booleans(self):
        with pytest.raises(ProtocolError, match="message-id cannot be a boolean"):
            msg.encode_message_id(True)


class TestAnnotationSections:
    @pytest.mark.parametrize(
        ("section_type", "descriptor"),
        [
            (msg.DeliveryAnnotations, 0x71),
            (msg.MessageAnnotations, 0x72),
            (msg.ApplicationProperties, 0x74),
            (msg.Footer, 0x78),
        ],
    )
    def test_descriptors(self, section_type, descriptor):
        assert enc.peek_descriptor(section_type({"key": "value"}).encode()) == descriptor

    @pytest.mark.parametrize(
        "section",
        [
            msg.DeliveryAnnotations({"x-hop": 1}),
            msg.MessageAnnotations({"x-stream-filter-value": "region-1"}),
            msg.ApplicationProperties({"key": "value", "count": 3, "flag": True}),
            msg.Footer({"checksum": b"\x01\x02"}),
        ],
    )
    def test_round_trip(self, section):
        assert msg.decode_section(section.encode()) == section

    @pytest.mark.parametrize(
        "section_type",
        [msg.DeliveryAnnotations, msg.MessageAnnotations, msg.ApplicationProperties, msg.Footer],
    )
    def test_empty_round_trip(self, section_type):
        assert msg.decode_section(section_type().encode()) == section_type()

    def test_the_described_value_is_a_bare_map(self):
        described = enc.decode_value(msg.MessageAnnotations({"a": 1}).encode())
        assert described.value == {"a": 1}

    def test_application_property_keys_are_strings(self):
        described = enc.decode_value(msg.ApplicationProperties({"key": "value"}).encode())
        assert all(type(key) is str for key in described.value)


class TestBodySections:
    def test_data_round_trip(self):
        section = msg.Data(b"\x01\x02payload")
        assert msg.decode_section(section.encode()) == section
        assert enc.peek_descriptor(section.encode()) == 0x75

    def test_data_holds_a_bare_binary(self):
        described = enc.decode_value(msg.Data(b"abc").encode())
        assert described.value == b"abc"

    def test_data_rejects_a_non_binary_body(self):
        with pytest.raises(ProtocolError, match="data section must hold binary"):
            msg.Data.from_value(["not", "binary"])

    def test_amqp_sequence_round_trip(self):
        section = msg.AmqpSequence([1, "two", None, True])
        assert msg.decode_section(section.encode()) == section
        assert enc.peek_descriptor(section.encode()) == 0x76

    def test_amqp_sequence_rejects_a_non_list_body(self):
        with pytest.raises(ProtocolError, match="amqp-sequence section must hold a list"):
            msg.AmqpSequence.from_value(b"bytes")

    @pytest.mark.parametrize("value", ["text", 42, None, {"a": 1}, [1, 2], b"bytes"])
    def test_amqp_value_round_trip(self, value):
        section = msg.AmqpValue(value)
        assert msg.decode_section(section.encode()) == section

    def test_amqp_value_descriptor_is_0x77(self):
        assert enc.peek_descriptor(msg.AmqpValue("x").encode()) == 0x77

    def test_unknown_section_descriptor_is_rejected(self):
        with pytest.raises(ProtocolError, match="unknown message section descriptor"):
            msg.decode_section(enc.encode_described_value(0x79, {}))

    def test_a_non_described_section_is_rejected(self):
        with pytest.raises(ProtocolError, match="must be a described type"):
            msg.decode_section(enc.encode_string("bare"))


class TestMessageConstruction:
    def test_bytes_body_is_wrapped_in_a_data_section(self):
        assert msg.Message(b"payload").body == msg.Data(b"payload")

    def test_string_body_is_wrapped_as_utf8_data(self):
        assert msg.Message("héllo").body == msg.Data("héllo".encode())

    def test_an_explicit_section_is_kept_as_is(self):
        assert msg.Message(msg.AmqpValue(42)).body == msg.AmqpValue(42)

    def test_body_defaults_to_none(self):
        assert msg.Message().body is None

    def test_keyword_sections_are_accepted(self):
        message = msg.Message(
            "hello",
            properties=msg.Properties(subject="greeting"),
            application_properties=msg.ApplicationProperties({"key": "value"}),
        )
        assert message.properties is not None
        assert message.properties.subject == "greeting"
        assert message.application_properties == msg.ApplicationProperties({"key": "value"})

    def test_body_as_bytes_for_a_data_body(self):
        assert msg.Message(b"payload").body_as_bytes() == b"payload"

    def test_body_as_bytes_for_no_body(self):
        assert msg.Message().body_as_bytes() == b""

    def test_body_as_bytes_concatenates_repeated_data_sections(self):
        message = msg.Message([msg.Data(b"part-1"), msg.Data(b"part-2")])
        assert message.body_as_bytes() == b"part-1part-2"

    @pytest.mark.parametrize("value", ["text", b"bytes"])
    def test_body_as_bytes_converts_a_string_or_binary_amqp_value(self, value):
        expected = value.encode() if isinstance(value, str) else value
        assert msg.Message(msg.AmqpValue(value)).body_as_bytes() == expected

    def test_body_as_bytes_rejects_a_non_binary_amqp_value(self):
        with pytest.raises(TypeError, match="is not raw bytes"):
            msg.Message(msg.AmqpValue(42)).body_as_bytes()

    def test_body_as_bytes_rejects_a_sequence_body(self):
        with pytest.raises(TypeError, match="amqp-sequence"):
            msg.Message(msg.AmqpSequence([1, 2])).body_as_bytes()

    def test_body_as_bytes_rejects_mixed_sequence_sections(self):
        with pytest.raises(TypeError, match="amqp-sequence"):
            msg.Message([msg.Data(b"a"), msg.AmqpSequence([1])]).body_as_bytes()

    def test_body_as_string_decodes_utf8(self):
        assert msg.Message("héllo").body_as_string() == "héllo"


class TestMessageEncodeDecode:
    def test_round_trip_with_a_data_body_only(self):
        message = msg.Message(b"payload")
        assert msg.Message.decode(message.encode()) == message

    def test_round_trip_with_every_section(self):
        message = msg.Message(
            b"payload",
            header=msg.Header(durable=True, priority=2, ttl=1000),
            delivery_annotations=msg.DeliveryAnnotations({"x-hop": 1}),
            message_annotations=msg.MessageAnnotations({"x-stream-filter-value": "r1"}),
            properties=msg.Properties(message_id="id-1", subject="hello", content_type="text/plain"),
            application_properties=msg.ApplicationProperties({"key": "value"}),
            footer=msg.Footer({"checksum": b"\x01"}),
        )
        assert msg.Message.decode(message.encode()) == message

    def test_sections_are_encoded_in_spec_order(self):
        message = msg.Message(
            b"payload",
            header=msg.Header(durable=True),
            delivery_annotations=msg.DeliveryAnnotations({"a": 1}),
            message_annotations=msg.MessageAnnotations({"b": 2}),
            properties=msg.Properties(subject="s"),
            application_properties=msg.ApplicationProperties({"c": 3}),
            footer=msg.Footer({"d": 4}),
        )
        decoder = enc.Decoder(message.encode())
        descriptors = []
        while decoder.remaining > 0:
            descriptors.append(decoder.read_value().descriptor)
        assert descriptors == [0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x78]

    def test_round_trip_with_an_amqp_value_body(self):
        message = msg.Message(msg.AmqpValue({"key": "value"}))
        assert msg.Message.decode(message.encode()) == message

    def test_round_trip_with_an_amqp_sequence_body(self):
        message = msg.Message(msg.AmqpSequence([1, "two"]))
        assert msg.Message.decode(message.encode()) == message

    def test_round_trip_with_repeated_data_sections(self):
        message = msg.Message([msg.Data(b"part-1"), msg.Data(b"part-2")])
        decoded = msg.Message.decode(message.encode())
        assert decoded.body == [msg.Data(b"part-1"), msg.Data(b"part-2")]
        assert decoded.body_as_bytes() == b"part-1part-2"

    def test_round_trip_with_repeated_sequence_sections(self):
        message = msg.Message([msg.AmqpSequence([1]), msg.AmqpSequence([2])])
        assert msg.Message.decode(message.encode()).body == [msg.AmqpSequence([1]), msg.AmqpSequence([2])]

    def test_round_trip_with_no_body(self):
        message = msg.Message(properties=msg.Properties(subject="body-less"))
        decoded = msg.Message.decode(message.encode())
        assert decoded == message
        assert decoded.body is None

    def test_empty_payload_decodes_to_an_empty_message(self):
        assert msg.Message.decode(b"") == msg.Message()

    def test_a_single_data_section_decodes_to_a_single_section_not_a_list(self):
        assert msg.Message.decode(msg.Data(b"one").encode()).body == msg.Data(b"one")

    def test_round_trip_with_a_large_body_switching_to_vbin32(self):
        payload = b"x" * 1024
        message = msg.Message(payload)
        assert msg.Message.decode(message.encode()).body_as_bytes() == payload

    def test_decode_rejects_a_bare_non_described_section(self):
        with pytest.raises(ProtocolError, match="must be a described type"):
            msg.Message.decode(enc.encode_string("bare"))

    def test_decode_rejects_an_unknown_section(self):
        with pytest.raises(ProtocolError, match="unknown message section descriptor"):
            msg.Message.decode(enc.encode_described_value(0x7F, {}))

    def test_decode_rejects_a_header_that_is_not_a_list(self):
        with pytest.raises(ProtocolError, match="must be a described list"):
            msg.Message.decode(enc.encode_described_value(0x70, "not-a-list"))

    def test_decode_rejects_a_body_mixing_amqp_value_and_data(self):
        payload = msg.AmqpValue("v").encode() + msg.Data(b"d").encode()
        with pytest.raises(ProtocolError, match="mixes an amqp-value section"):
            msg.Message.decode(payload)

    def test_decode_rejects_two_amqp_value_sections(self):
        payload = msg.AmqpValue("one").encode() + msg.AmqpValue("two").encode()
        with pytest.raises(ProtocolError, match="more than one amqp-value"):
            msg.Message.decode(payload)

    def test_symbolic_section_descriptors_are_accepted(self):
        encoded = enc.encode_described("amqp:data:binary", enc.encode_binary(b"payload"))
        assert msg.Message.decode(encoded).body_as_bytes() == b"payload"
