"""Unit tests for the AMQP 1.0 performatives and terminus types."""

from __future__ import annotations

import pytest

from rabbitmq_amqp_python_client.exceptions import ProtocolError
from rabbitmq_amqp_python_client.wire import encoding as enc
from rabbitmq_amqp_python_client.wire import performatives as perf
from rabbitmq_amqp_python_client.wire.delivery_state import Accepted, Error, Modified, Rejected


def fields_of(encoded: bytes) -> list:
    """Return the decoded positional fields of an encoded described list."""
    _, values = enc.decode_described_list(encoded)
    return values


class TestOpen:
    def test_round_trip_with_all_fields(self):
        performative = perf.Open(
            container_id="client-1",
            hostname="vhost",
            max_frame_size=131072,
            channel_max=256,
            idle_time_out=60000,
            outgoing_locales=["en-US"],
            incoming_locales=["en-US"],
            offered_capabilities=["cap-a"],
            desired_capabilities=["cap-b"],
            properties={"product": "rabbitmq-amqp-python-client"},
        )
        assert perf.Open.decode(performative.encode()) == performative

    def test_round_trip_with_only_the_container_id(self):
        performative = perf.Open(container_id="client-1")
        decoded = perf.Open.decode(performative.encode())
        assert decoded == performative
        assert decoded.max_frame_size == perf.MAX_UINT
        assert decoded.channel_max == perf.MAX_USHORT

    def test_defaults_are_omitted_from_the_trailing_run(self):
        assert fields_of(perf.Open(container_id="client-1").encode()) == ["client-1"]

    def test_absent_hostname_before_a_present_field_becomes_null(self):
        performative = perf.Open(container_id="client-1", max_frame_size=1024)
        assert fields_of(performative.encode()) == ["client-1", None, 1024]

    def test_default_fields_between_present_ones_become_null(self):
        performative = perf.Open(container_id="c", idle_time_out=30000)
        assert fields_of(performative.encode()) == ["c", None, None, None, 30000]
        assert perf.Open.decode(performative.encode()) == performative

    def test_descriptor_is_0x10(self):
        assert enc.peek_descriptor(perf.Open(container_id="c").encode()) == 0x10

    def test_channel_max_is_encoded_as_a_ushort(self):
        encoded = perf.Open(container_id="c", channel_max=7).encode()
        assert b"\x60\x00\x07" in encoded

    def test_capabilities_are_encoded_as_symbol_arrays(self):
        performative = perf.Open(container_id="c", offered_capabilities=["one", "two"])
        assert fields_of(performative.encode())[7] == ["one", "two"]

    def test_a_single_symbol_is_accepted_for_a_multiple_field(self):
        encoded = enc.encode_described_list(
            0x10,
            [enc.encode_string("c"), None, None, None, None, None, None, enc.encode_symbol("solo")],
        )
        assert perf.Open.decode(encoded).offered_capabilities == ["solo"]

    def test_missing_mandatory_container_id_is_rejected(self):
        with pytest.raises(ProtocolError, match="missing its mandatory container-id"):
            perf.Open.decode(enc.encode_described_list(0x10, []))

    def test_wrong_descriptor_is_rejected(self):
        with pytest.raises(ProtocolError, match="expected descriptor 0x10"):
            perf.Open.decode(perf.Close().encode())


class TestBegin:
    def test_round_trip_with_all_fields(self):
        performative = perf.Begin(
            remote_channel=3,
            next_outgoing_id=1,
            incoming_window=100,
            outgoing_window=100,
            handle_max=255,
            offered_capabilities=["cap"],
            desired_capabilities=["cap"],
            properties={"key": "value"},
        )
        assert perf.Begin.decode(performative.encode()) == performative

    def test_round_trip_when_initiating_without_a_remote_channel(self):
        performative = perf.Begin(next_outgoing_id=0, incoming_window=8, outgoing_window=8)
        decoded = perf.Begin.decode(performative.encode())
        assert decoded == performative
        assert decoded.remote_channel is None
        assert decoded.handle_max == perf.MAX_UINT

    def test_absent_remote_channel_is_encoded_as_null(self):
        performative = perf.Begin(next_outgoing_id=0, incoming_window=1, outgoing_window=2)
        assert fields_of(performative.encode()) == [None, 0, 1, 2]

    def test_remote_channel_is_encoded_as_a_ushort(self):
        performative = perf.Begin(remote_channel=1, next_outgoing_id=0, incoming_window=1, outgoing_window=1)
        assert performative.encode().count(b"\x60\x00\x01") == 1

    def test_descriptor_is_0x11(self):
        performative = perf.Begin(next_outgoing_id=0, incoming_window=1, outgoing_window=1)
        assert enc.peek_descriptor(performative.encode()) == 0x11


class TestSourceAndTarget:
    def test_source_round_trip_with_all_fields(self):
        source = perf.Source(
            address="/queues/my-queue",
            durable=perf.TERMINUS_DURABILITY_UNSETTLED_STATE,
            expiry_policy=perf.EXPIRY_POLICY_NEVER,
            timeout=30,
            dynamic=True,
            dynamic_node_properties={"lifetime": "delete-on-close"},
            distribution_mode=perf.DISTRIBUTION_MODE_MOVE,
            filter={"rabbitmq:stream-offset-spec": "first"},
            default_outcome=Modified(delivery_failed=True),
            outcomes=["amqp:accepted:list", "amqp:rejected:list"],
            capabilities=["queue"],
        )
        assert perf.Source.decode(source.encode()) == source

    def test_source_round_trip_with_only_an_address(self):
        source = perf.Source(address="/queues/q")
        decoded = perf.Source.decode(source.encode())
        assert decoded == source
        assert decoded.expiry_policy == perf.EXPIRY_POLICY_SESSION_END
        assert fields_of(source.encode()) == ["/queues/q"]

    def test_source_filter_after_defaults_becomes_null(self):
        source = perf.Source(address="/queues/q", filter={"rabbitmq:stream-filter": ["a"]})
        values = fields_of(source.encode())
        assert values[1:7] == [None, None, None, None, None, None]
        assert values[7] == {"rabbitmq:stream-filter": ["a"]}

    def test_source_descriptor_is_0x28(self):
        assert enc.peek_descriptor(perf.Source().encode()) == 0x28

    def test_target_round_trip(self):
        target = perf.Target(
            address="/exchanges/amq.direct/key",
            durable=perf.TERMINUS_DURABILITY_CONFIGURATION,
            expiry_policy=perf.EXPIRY_POLICY_LINK_DETACH,
            timeout=5,
            dynamic=True,
            dynamic_node_properties={"a": 1},
            capabilities=["exchange"],
        )
        assert perf.Target.decode(target.encode()) == target

    def test_target_descriptor_is_0x29(self):
        assert enc.peek_descriptor(perf.Target().encode()) == 0x29

    def test_empty_target_encodes_as_an_empty_list(self):
        assert perf.Target().encode() == b"\x00\x53\x29\x45"

    def test_terminus_decodes_from_an_already_decoded_described_value(self):
        source = perf.Source(address="/queues/q")
        assert perf.Source.decode(enc.decode_value(source.encode())) == source

    def test_terminus_rejects_a_wrong_descriptor(self):
        with pytest.raises(ProtocolError, match="expected descriptor 0x28"):
            perf.Source.decode(perf.Target().encode())

    def test_terminus_rejects_a_non_list_described_body(self):
        with pytest.raises(ProtocolError, match="expected a described list"):
            perf.Source.decode(enc.Described(0x28, "oops"))


class TestAttach:
    def test_round_trip_with_all_fields(self):
        performative = perf.Attach(
            name="link-1",
            handle=0,
            role=perf.ROLE_SENDER,
            snd_settle_mode=perf.SND_SETTLE_MODE_SETTLED,
            rcv_settle_mode=perf.RCV_SETTLE_MODE_SECOND,
            source=perf.Source(address="/queues/q"),
            target=perf.Target(address="/exchanges/e"),
            unsettled={b"\x01": Accepted()},
            incomplete_unsettled=True,
            initial_delivery_count=0,
            max_message_size=1048576,
            offered_capabilities=["cap"],
            desired_capabilities=["cap"],
            properties={"key": "value"},
        )
        assert perf.Attach.decode(performative.encode()) == performative

    def test_unsettled_map_holds_delivery_tags_and_states(self):
        performative = perf.Attach(
            name="link-1",
            handle=0,
            role=perf.ROLE_SENDER,
            unsettled={b"\x01": Accepted(), b"\x02": Rejected(error=Error(condition="amqp:not-found"))},
        )
        decoded = perf.Attach.decode(performative.encode())
        assert decoded.unsettled == performative.unsettled

    def test_round_trip_with_only_mandatory_fields(self):
        performative = perf.Attach(name="link-1", handle=2, role=perf.ROLE_RECEIVER)
        decoded = perf.Attach.decode(performative.encode())
        assert decoded == performative
        assert fields_of(performative.encode()) == ["link-1", 2, True]

    def test_defaults_between_present_fields_become_null(self):
        performative = perf.Attach(
            name="link-1",
            handle=0,
            role=perf.ROLE_SENDER,
            target=perf.Target(address="/exchanges/e"),
            initial_delivery_count=0,
        )
        values = fields_of(performative.encode())
        assert values[3:5] == [None, None]
        assert values[5] is None
        assert values[7:9] == [None, None]
        assert values[9] == 0
        assert len(values) == 10

    def test_role_is_encoded_as_a_boolean(self):
        assert fields_of(perf.Attach(name="l", handle=0, role=perf.ROLE_SENDER).encode())[2] is False
        assert fields_of(perf.Attach(name="l", handle=0, role=perf.ROLE_RECEIVER).encode())[2] is True

    def test_a_refused_source_decodes_as_none(self):
        performative = perf.Attach(name="l", handle=0, role=perf.ROLE_RECEIVER, target=perf.Target())
        decoded = perf.Attach.decode(performative.encode())
        assert decoded.source is None
        assert decoded.target == perf.Target()

    def test_descriptor_is_0x12(self):
        performative = perf.Attach(name="l", handle=0, role=perf.ROLE_SENDER)
        assert enc.peek_descriptor(performative.encode()) == 0x12

    def test_missing_mandatory_name_is_rejected(self):
        with pytest.raises(ProtocolError, match="missing its mandatory name"):
            perf.Attach.decode(enc.encode_described_list(0x12, []))


class TestFlow:
    def test_round_trip_with_all_fields(self):
        performative = perf.Flow(
            next_incoming_id=1,
            incoming_window=100,
            next_outgoing_id=2,
            outgoing_window=100,
            handle=0,
            delivery_count=5,
            link_credit=10,
            available=3,
            drain=True,
            echo=True,
            properties={"key": "value"},
        )
        assert perf.Flow.decode(performative.encode()) == performative

    def test_round_trip_with_only_session_fields(self):
        performative = perf.Flow(incoming_window=8, next_outgoing_id=0, outgoing_window=8)
        decoded = perf.Flow.decode(performative.encode())
        assert decoded == performative
        assert decoded.handle is None
        assert decoded.link_credit is None
        assert decoded.drain is False

    def test_absent_next_incoming_id_is_encoded_as_null(self):
        performative = perf.Flow(incoming_window=1, next_outgoing_id=2, outgoing_window=3)
        assert fields_of(performative.encode()) == [None, 1, 2, 3]

    def test_zero_link_credit_is_still_encoded(self):
        performative = perf.Flow(
            next_incoming_id=0,
            incoming_window=1,
            next_outgoing_id=0,
            outgoing_window=1,
            handle=0,
            delivery_count=0,
            link_credit=0,
        )
        values = fields_of(performative.encode())
        assert values[6] == 0
        assert perf.Flow.decode(performative.encode()).link_credit == 0

    def test_descriptor_is_0x13(self):
        performative = perf.Flow(incoming_window=1, next_outgoing_id=0, outgoing_window=1)
        assert enc.peek_descriptor(performative.encode()) == 0x13


class TestTransfer:
    def test_round_trip_with_all_fields(self):
        performative = perf.Transfer(
            handle=0,
            delivery_id=7,
            delivery_tag=b"\x00\x00\x00\x07",
            message_format=1,
            settled=True,
            more=True,
            rcv_settle_mode=perf.RCV_SETTLE_MODE_SECOND,
            state=Accepted(),
            resume=True,
            aborted=True,
            batchable=True,
        )
        assert perf.Transfer.decode(performative.encode()) == performative

    def test_round_trip_with_only_a_handle(self):
        performative = perf.Transfer(handle=3)
        decoded = perf.Transfer.decode(performative.encode())
        assert decoded == performative
        assert fields_of(performative.encode()) == [3]

    def test_settled_false_is_distinct_from_absent(self):
        explicit = perf.Transfer(handle=0, settled=False)
        assert fields_of(explicit.encode()) == [0, None, None, None, False]
        assert perf.Transfer.decode(explicit.encode()).settled is False
        assert perf.Transfer.decode(perf.Transfer(handle=0).encode()).settled is None

    def test_delivery_tag_is_encoded_as_binary(self):
        performative = perf.Transfer(handle=0, delivery_id=1, delivery_tag=b"tag")
        assert fields_of(performative.encode())[2] == b"tag"

    def test_rejected_state_round_trip(self):
        state = Rejected(error=Error(condition="amqp:not-allowed"))
        performative = perf.Transfer(handle=0, delivery_id=1, state=state)
        assert perf.Transfer.decode(performative.encode()).state == state

    def test_descriptor_is_0x14(self):
        assert enc.peek_descriptor(perf.Transfer(handle=0).encode()) == 0x14

    def test_trailing_payload_is_ignored_by_decode(self):
        encoded = perf.Transfer(handle=1, delivery_id=2).encode()
        assert perf.Transfer.decode(encoded + b"raw-message-bytes").delivery_id == 2


class TestDisposition:
    def test_round_trip_with_all_fields(self):
        performative = perf.Disposition(
            role=perf.ROLE_RECEIVER,
            first=1,
            last=4,
            settled=True,
            state=Accepted(),
            batchable=True,
        )
        assert perf.Disposition.decode(performative.encode()) == performative

    def test_round_trip_with_only_mandatory_fields(self):
        performative = perf.Disposition(role=perf.ROLE_RECEIVER, first=9)
        decoded = perf.Disposition.decode(performative.encode())
        assert decoded == performative
        assert decoded.last is None
        assert fields_of(performative.encode()) == [True, 9]

    def test_state_after_an_absent_last_becomes_null(self):
        performative = perf.Disposition(role=perf.ROLE_RECEIVER, first=0, state=Accepted())
        values = fields_of(performative.encode())
        assert values[2] is None
        assert values[3] is None
        assert isinstance(values[4], enc.Described)
        assert perf.Disposition.decode(performative.encode()) == performative

    def test_descriptor_is_0x15(self):
        performative = perf.Disposition(role=perf.ROLE_SENDER, first=0)
        assert enc.peek_descriptor(performative.encode()) == 0x15


class TestDetachEndClose:
    def test_detach_round_trip_with_an_error(self):
        performative = perf.Detach(
            handle=2,
            closed=True,
            error=Error(condition="amqp:link:detach-forced", description="forced"),
        )
        assert perf.Detach.decode(performative.encode()) == performative

    def test_detach_round_trip_without_an_error(self):
        performative = perf.Detach(handle=2)
        assert perf.Detach.decode(performative.encode()) == performative
        assert fields_of(performative.encode()) == [2]

    def test_detach_error_after_a_default_closed_becomes_null(self):
        performative = perf.Detach(handle=0, error=Error(condition="amqp:internal-error"))
        values = fields_of(performative.encode())
        assert values[1] is None
        assert isinstance(values[2], enc.Described)
        assert perf.Detach.decode(performative.encode()) == performative

    def test_end_round_trip(self):
        assert perf.End.decode(perf.End().encode()) == perf.End()
        performative = perf.End(error=Error(condition="amqp:session:window-violation"))
        assert perf.End.decode(performative.encode()) == performative

    def test_end_without_an_error_is_an_empty_described_list(self):
        assert perf.End().encode() == b"\x00\x53\x17\x45"

    def test_close_round_trip(self):
        assert perf.Close.decode(perf.Close().encode()) == perf.Close()
        performative = perf.Close(error=Error(condition="amqp:connection:forced", description="bye"))
        assert perf.Close.decode(performative.encode()) == performative

    def test_close_without_an_error_is_an_empty_described_list(self):
        assert perf.Close().encode() == b"\x00\x53\x18\x45"

    @pytest.mark.parametrize(
        ("performative", "descriptor"),
        [(perf.Detach(handle=0), 0x16), (perf.End(), 0x17), (perf.Close(), 0x18)],
    )
    def test_descriptors(self, performative, descriptor):
        assert enc.peek_descriptor(performative.encode()) == descriptor


class TestDecodePerformativeDispatch:
    @pytest.mark.parametrize(
        "performative",
        [
            perf.Open(container_id="c"),
            perf.Begin(next_outgoing_id=0, incoming_window=1, outgoing_window=1),
            perf.Attach(name="l", handle=0, role=perf.ROLE_SENDER),
            perf.Flow(incoming_window=1, next_outgoing_id=0, outgoing_window=1),
            perf.Transfer(handle=0, delivery_id=1, delivery_tag=b"\x01"),
            perf.Disposition(role=perf.ROLE_RECEIVER, first=0),
            perf.Detach(handle=0, closed=True),
            perf.End(),
            perf.Close(),
        ],
    )
    def test_dispatcher_returns_the_matching_type(self, performative):
        decoded = perf.decode_performative(performative.encode())
        assert decoded == performative
        assert type(decoded) is type(performative)

    def test_dispatcher_accepts_symbolic_descriptors(self):
        encoded = enc.encode_described_list("amqp:close:list", [])
        assert perf.decode_performative(encoded) == perf.Close()

    def test_unknown_descriptor_is_rejected(self):
        with pytest.raises(ProtocolError, match="unknown performative descriptor"):
            perf.decode_performative(enc.encode_described_list(0x19, []))

    def test_decode_with_payload_splits_the_transfer_payload(self):
        performative = perf.Transfer(handle=1, delivery_id=5, delivery_tag=b"\x05")
        decoded, payload = perf.decode_performative_with_payload(performative.encode() + b"body-bytes")
        assert decoded == performative
        assert payload == b"body-bytes"

    def test_decode_with_payload_returns_empty_payload_when_absent(self):
        decoded, payload = perf.decode_performative_with_payload(perf.Close().encode())
        assert decoded == perf.Close()
        assert payload == b""
