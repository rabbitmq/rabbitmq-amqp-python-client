"""Fixture-driven Reader/decode validation (``004_amqp10_validation``).

Each ``*.amqp`` file under ``tests/unit/resources/amqp10_validation/`` is a raw
bare-message byte dump copied from the specs repository's
``004_amqp10_validation/`` directory — no frame header, no ``transfer``
performative, exactly the byte layout ``protocol-commands.md``'s "Message
format" describes. This mirrors rabbitmq-stream-dotnet-client's
``Tests/Amqp10Tests.cs``: one test (or case) per fixture, asserting the
specific decode edge case that fixture exists to prove. See
``004_amqp10_validation/read_validation.md`` for the full fixture catalogue.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from src.exceptions import ProtocolError
from src.wire import encoding as enc
from src.wire import message as msg

RESOURCES = Path(__file__).parent / "resources" / "amqp10_validation"


def load(name: str) -> bytes:
    """Read one fixture's raw bare-message bytes."""
    return (RESOURCES / name).read_bytes()


class TestEmptyMessage:
    def test_decodes_to_a_single_empty_data_section(self):
        message = msg.Message.decode(load("empty_message.amqp"))
        assert message == msg.Message(b"")
        assert message.header is None
        assert message.properties is None
        assert message.application_properties is None


class TestDataBodySizeBoundary:
    """``message_body_250``/``_700``: the ``vbin8``/``vbin32`` boundary at 255 bytes."""

    def test_250_byte_body_uses_the_8_bit_binary_length_form(self):
        data = load("message_body_250.amqp")
        assert data[3] == enc.CODE_VBIN8
        assert len(msg.Message.decode(data).body_as_bytes()) == 250

    def test_700_byte_body_uses_the_32_bit_binary_length_form(self):
        data = load("message_body_700.amqp")
        assert data[3] == enc.CODE_VBIN32
        assert len(msg.Message.decode(data).body_as_bytes()) == 700


class TestLargeApplicationProperties:
    """``message_random_application_properties_300``/``_500``: map decoding at scale."""

    @pytest.mark.parametrize(
        ("fixture", "expected_count"),
        [
            ("message_random_application_properties_300.amqp", 10),
            ("message_random_application_properties_500.amqp", 3),
        ],
    )
    def test_every_declared_entry_is_decoded_with_a_string_key(self, fixture, expected_count):
        message = msg.Message.decode(load(fixture))
        properties = message.application_properties.value
        assert len(properties) == expected_count
        assert all(isinstance(key, str) for key in properties)

    def test_a_full_properties_section_and_a_large_map_share_one_message(self):
        """``..._properties_900``: the reader must advance by each section's own size, not a fixed offset."""
        message = msg.Message.decode(load("message_random_application_properties_properties_900.amqp"))
        assert message.properties is not None
        assert message.properties.message_id == 33333333
        assert message.properties.correlation_id == uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        assert message.properties.content_type == "json"
        assert message.properties.content_encoding == "myCoding"
        assert message.properties.group_sequence == 10
        assert len(message.application_properties.value) == 1


class TestUnicodeApplicationProperties:
    def test_multi_byte_utf8_text_decodes_using_the_declared_byte_length(self):
        message = msg.Message.decode(load("message_unicode_message.amqp"))
        properties = message.application_properties.value
        assert properties["from_go_byte"].startswith("Alan  Mathison Turing")
        assert "Τούρινγκ" in properties["from_go_greek"]
        assert "图灵" in properties["from_go_ch_long"]
        assert properties["from_go"] == "祝您有美好的一天，并享受客户"


class TestUuidMessageId:
    def test_message_id_and_correlation_id_decode_as_uuid(self):
        message = msg.Message.decode(load("uuid_message.amqp"))
        expected = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        assert message.properties.message_id == expected
        assert message.properties.correlation_id == expected


class TestNilAndMixedTypes:
    def test_a_null_entry_does_not_desynchronize_the_entries_that_follow_it(self):
        properties = msg.Message.decode(load("nil_and_types.amqp")).application_properties.value
        assert properties["null"] is None
        assert properties["empty"] == ""
        assert properties["bool_value"] is True
        assert properties["byte_value"] == 216
        assert properties["int_value"] == 1
        assert properties["long_value"] == 91000001001
        assert properties["float"] == pytest.approx(1.1)
        assert properties["double"] == pytest.approx(1.1)
        assert properties["uuid"] == uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")


class TestStaticMessageCompare:
    def test_decodes_to_the_exact_expected_message(self):
        expected = msg.Message(
            b"test",
            message_annotations=msg.MessageAnnotations({"test": "test", 1: 1, 100000: 100000}),
            properties=msg.Properties(
                message_id="test",
                user_id=b"test",
                to="test",
                subject="test",
                reply_to="test",
                correlation_id=1,
                content_type="test",
                content_encoding="test",
                group_id="test",
                group_sequence=1,
                reply_to_group_id="test",
            ),
            application_properties=msg.ApplicationProperties({"test": "test", "double": 64.646464}),
        )
        assert msg.Message.decode(load("static_test_message_compare.amqp")) == expected


class TestGoldenFixtureFromAnotherProducer:
    """``message_from_version_1_0_0``: a message captured from an independent AMQP 1.0 producer."""

    def test_decodes_despite_a_properties_size_field_that_undercounts_by_the_count_fields_width(self):
        message = msg.Message.decode(load("message_from_version_1_0_0.amqp"))
        assert message.properties == msg.Properties(
            message_id="MyMessageId",
            user_id=b"guest",
            correlation_id="MyCorrelationId",
            content_type="text/plain",
            content_encoding="utf-8",
            group_sequence=9999,
            reply_to_group_id="MyReplyToGroupId",
        )
        assert message.application_properties.value == {
            "key_string": "value",
            "key2_int": 1111,
            "key2_decimal": 10000000000,
            "key2_bool": True,
        }
        assert message.body_as_bytes() == b"Message100"

    def test_content_type_and_content_encoding_are_accepted_even_when_sent_as_a_plain_string(self):
        # §3 point 6: these fields are documented as `symbol`, but this producer
        # encodes them as `string` (0xa1) — the reader must not require `symbol`.
        message = msg.Message.decode(load("message_from_version_1_0_0.amqp"))
        assert message.properties.content_type == "text/plain"
        assert message.properties.content_encoding == "utf-8"


class TestNestedAnnotationMap:
    def test_message_annotations_decodes_a_map_within_a_map(self):
        message = msg.Message.decode(load("shovel_annotations.amqp"))
        assert message.header == msg.Header(durable=True, first_acquirer=True)
        shovelled = message.message_annotations.value["x-shovelled"]
        assert isinstance(shovelled, list)
        assert shovelled[0]["shovel-name"] == "bug"
        assert shovelled[0]["dest-exchange-key"] == "hello-key"
        assert message.body_as_bytes() == b"from shovel"


class TestConflictingBodySections:
    def test_a_message_with_both_a_data_and_an_amqp_value_section_is_rejected(self):
        """``header_amqpvalue_message``: a body must be exactly one of Data/AmqpSequence/AmqpValue."""
        with pytest.raises(ProtocolError, match="mixes an amqp-value section"):
            msg.Message.decode(load("header_amqpvalue_message.amqp"))
