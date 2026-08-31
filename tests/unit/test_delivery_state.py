"""Unit tests for the ``error`` type and the delivery-state/outcome types."""

from __future__ import annotations

import pytest

from rabbitmq_amqp_python_client.exceptions import ProtocolError
from rabbitmq_amqp_python_client.wire import delivery_state as ds
from rabbitmq_amqp_python_client.wire import encoding as enc


class TestError:
    def test_round_trip_with_all_fields(self):
        error = ds.Error(
            condition="amqp:precondition-failed",
            description="queue is exclusive",
            info={"node": "rabbit@host"},
        )
        assert ds.Error.decode(error.encode()) == error

    def test_round_trip_with_only_the_condition(self):
        error = ds.Error(condition="amqp:not-found")
        decoded = ds.Error.decode(error.encode())
        assert decoded == error
        assert decoded.description is None
        assert decoded.info is None

    def test_descriptor_is_0x1d(self):
        assert enc.peek_descriptor(ds.Error(condition="amqp:not-found").encode()) == 0x1D

    def test_condition_is_encoded_as_a_symbol(self):
        _, values = enc.decode_described_list(ds.Error(condition="amqp:not-found").encode())
        assert isinstance(values[0], enc.Symbol)

    def test_trailing_absent_fields_are_omitted(self):
        _, values = enc.decode_described_list(ds.Error(condition="amqp:not-found").encode())
        assert values == ["amqp:not-found"]

    def test_info_after_an_absent_description_becomes_null(self):
        error = ds.Error(condition="amqp:internal-error", info={"retry": True})
        _, values = enc.decode_described_list(error.encode())
        assert values == ["amqp:internal-error", None, {"retry": True}]
        assert ds.Error.decode(error.encode()) == error

    def test_missing_mandatory_condition_is_rejected(self):
        with pytest.raises(ProtocolError, match="missing its mandatory condition"):
            ds.Error.decode(enc.encode_described_list(0x1D, []))

    def test_wrong_descriptor_is_rejected(self):
        with pytest.raises(ProtocolError, match="expected an error descriptor"):
            ds.Error.decode(ds.Accepted().encode())

    def test_decodes_from_an_already_decoded_described_value(self):
        error = ds.Error(condition="amqp:not-found", description="gone")
        described = enc.decode_value(error.encode())
        assert ds.Error.decode(described) == error


class TestOutcomes:
    def test_accepted_is_an_empty_described_list(self):
        assert ds.Accepted().encode() == b"\x00\x53\x24\x45"

    def test_released_is_an_empty_described_list(self):
        assert ds.Released().encode() == b"\x00\x53\x26\x45"

    @pytest.mark.parametrize(
        ("state", "descriptor"),
        [
            (ds.Accepted(), 0x24),
            (ds.Rejected(), 0x25),
            (ds.Released(), 0x26),
            (ds.Modified(), 0x27),
            (ds.Received(section_number=0, section_offset=0), 0x23),
        ],
    )
    def test_descriptors(self, state, descriptor):
        assert enc.peek_descriptor(state.encode()) == descriptor

    def test_rejected_round_trip_with_an_error(self):
        state = ds.Rejected(error=ds.Error(condition="amqp:not-allowed", description="nope"))
        assert ds.decode_delivery_state(state.encode()) == state

    def test_rejected_round_trip_without_an_error(self):
        assert ds.decode_delivery_state(ds.Rejected().encode()) == ds.Rejected()

    def test_modified_round_trip_with_all_fields(self):
        state = ds.Modified(
            delivery_failed=True,
            undeliverable_here=True,
            message_annotations={"x-opt-reason": "poison"},
        )
        assert ds.decode_delivery_state(state.encode()) == state

    def test_modified_defaults_are_omitted(self):
        _, values = enc.decode_described_list(ds.Modified().encode())
        assert values == []

    def test_modified_annotations_after_default_flags_become_null(self):
        state = ds.Modified(message_annotations={"key": "value"})
        _, values = enc.decode_described_list(state.encode())
        assert values == [None, None, {"key": "value"}]
        assert ds.decode_delivery_state(state.encode()) == state

    def test_received_round_trip(self):
        state = ds.Received(section_number=2, section_offset=1024)
        assert ds.decode_delivery_state(state.encode()) == state

    def test_received_offset_is_encoded_as_a_ulong(self):
        state = ds.Received(section_number=1, section_offset=2**40)
        _, values = enc.decode_described_list(state.encode())
        assert values == [1, 2**40]


class TestDecodeDeliveryStateDispatch:
    @pytest.mark.parametrize(
        "state",
        [
            ds.Accepted(),
            ds.Rejected(error=ds.Error(condition="amqp:not-found")),
            ds.Released(),
            ds.Modified(delivery_failed=True),
            ds.Received(section_number=1, section_offset=2),
        ],
    )
    def test_dispatcher_returns_the_matching_type(self, state):
        decoded = ds.decode_delivery_state(state.encode())
        assert decoded == state
        assert type(decoded) is type(state)

    def test_dispatcher_accepts_an_already_decoded_described_value(self):
        described = enc.decode_value(ds.Modified(undeliverable_here=True).encode())
        assert ds.decode_delivery_state(described) == ds.Modified(undeliverable_here=True)

    def test_dispatcher_accepts_symbolic_descriptors(self):
        encoded = enc.encode_described_list("amqp:accepted:list", [])
        assert ds.decode_delivery_state(encoded) == ds.Accepted()

    def test_unknown_descriptor_is_rejected(self):
        with pytest.raises(ProtocolError, match="unknown delivery-state descriptor"):
            ds.decode_delivery_state(enc.encode_described_list(0x2F, []))

    def test_non_list_described_body_is_rejected(self):
        described = enc.Described(0x24, "not-a-list")
        with pytest.raises(ProtocolError, match="expected a described list"):
            ds.decode_delivery_state(described)

    def test_described_value_with_a_null_body_is_treated_as_no_fields(self):
        assert ds.decode_delivery_state(enc.Described(0x24, None)) == ds.Accepted()
