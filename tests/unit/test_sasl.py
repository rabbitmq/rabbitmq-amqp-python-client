"""Unit tests for the SASL frame bodies and protocol headers."""

from __future__ import annotations

import pytest

from src.exceptions import ProtocolError
from src.wire import encoding as enc
from src.wire import sasl


class TestProtocolHeaders:
    def test_sasl_header_bytes(self):
        assert sasl.AMQP_SASL_HEADER == b"AMQP\x03\x01\x00\x00"

    def test_amqp_header_bytes(self):
        assert sasl.AMQP_PROTOCOL_HEADER == b"AMQP\x00\x01\x00\x00"

    def test_headers_are_eight_bytes(self):
        assert len(sasl.AMQP_SASL_HEADER) == sasl.PROTOCOL_HEADER_SIZE
        assert len(sasl.AMQP_PROTOCOL_HEADER) == sasl.PROTOCOL_HEADER_SIZE

    @pytest.mark.parametrize(
        ("protocol_id", "expected"),
        [
            (sasl.PROTOCOL_ID_AMQP, sasl.AMQP_PROTOCOL_HEADER),
            (sasl.PROTOCOL_ID_TLS, sasl.AMQP_TLS_HEADER),
            (sasl.PROTOCOL_ID_SASL, sasl.AMQP_SASL_HEADER),
        ],
    )
    def test_protocol_header_builder_matches_the_constants(self, protocol_id, expected):
        assert sasl.protocol_header(protocol_id) == expected


class TestPlainInitialResponse:
    def test_byte_layout_is_nul_user_nul_password(self):
        assert sasl.build_plain_initial_response("guest", "guest") == b"\x00guest\x00guest"

    def test_starts_with_an_empty_authorization_identity(self):
        response = sasl.build_plain_initial_response("user", "pass")
        assert response[0] == 0
        assert response.split(b"\x00") == [b"", b"user", b"pass"]

    def test_encodes_credentials_as_utf8(self):
        response = sasl.build_plain_initial_response("üser", "påss")
        assert response == b"\x00" + "üser".encode() + b"\x00" + "påss".encode()

    def test_empty_credentials_produce_two_nul_bytes(self):
        assert sasl.build_plain_initial_response("", "") == b"\x00\x00"


class TestSaslMechanisms:
    def test_round_trip(self):
        body = sasl.SaslMechanisms(["PLAIN", "AMQPLAIN", "ANONYMOUS"])
        assert sasl.SaslMechanisms.decode(body.encode()) == body

    def test_descriptor_is_0x40(self):
        assert enc.peek_descriptor(sasl.SaslMechanisms(["PLAIN"]).encode()) == 0x40

    def test_mechanisms_are_encoded_as_a_symbol_array(self):
        _, values = enc.decode_described_list(sasl.SaslMechanisms(["PLAIN"]).encode())
        assert values[0] == ["PLAIN"]

    def test_a_single_symbol_is_accepted_for_the_multiple_field(self):
        encoded = enc.encode_described_list(0x40, [enc.encode_symbol("PLAIN")])
        assert sasl.SaslMechanisms.decode(encoded).server_mechanisms == ["PLAIN"]

    def test_missing_mandatory_field_is_rejected(self):
        with pytest.raises(ProtocolError, match="missing its mandatory sasl-server-mechanisms"):
            sasl.SaslMechanisms.decode(enc.encode_described_list(0x40, []))


class TestSaslInit:
    def test_round_trip_with_all_fields(self):
        body = sasl.SaslInit(
            mechanism="PLAIN",
            initial_response=sasl.build_plain_initial_response("guest", "guest"),
            hostname="vhost:/",
        )
        assert sasl.SaslInit.decode(body.encode()) == body

    def test_round_trip_with_only_the_mechanism(self):
        body = sasl.SaslInit(mechanism="ANONYMOUS")
        decoded = sasl.SaslInit.decode(body.encode())
        assert decoded == body
        assert decoded.initial_response is None
        assert decoded.hostname is None

    def test_trailing_absent_fields_are_omitted(self):
        _, values = enc.decode_described_list(sasl.SaslInit(mechanism="EXTERNAL").encode())
        assert values == ["EXTERNAL"]

    def test_hostname_after_an_absent_initial_response_becomes_null(self):
        body = sasl.SaslInit(mechanism="EXTERNAL", hostname="host")
        _, values = enc.decode_described_list(body.encode())
        assert values == ["EXTERNAL", None, "host"]
        assert sasl.SaslInit.decode(body.encode()) == body

    def test_mechanism_is_encoded_as_a_symbol(self):
        _, values = enc.decode_described_list(sasl.SaslInit(mechanism="PLAIN").encode())
        assert isinstance(values[0], enc.Symbol)

    def test_missing_mandatory_field_is_rejected(self):
        with pytest.raises(ProtocolError, match="missing its mandatory mechanism"):
            sasl.SaslInit.decode(enc.encode_described_list(0x41, []))


class TestSaslChallengeAndResponse:
    def test_challenge_round_trip(self):
        body = sasl.SaslChallenge(b"\x01\x02challenge")
        assert sasl.SaslChallenge.decode(body.encode()) == body
        assert enc.peek_descriptor(body.encode()) == 0x42

    def test_response_round_trip(self):
        body = sasl.SaslResponse(b"\x03\x04response")
        assert sasl.SaslResponse.decode(body.encode()) == body
        assert enc.peek_descriptor(body.encode()) == 0x43

    def test_empty_challenge_round_trip(self):
        assert sasl.SaslChallenge.decode(sasl.SaslChallenge().encode()).challenge == b""


class TestSaslOutcome:
    def test_round_trip_with_additional_data(self):
        body = sasl.SaslOutcome(code=sasl.SASL_OK, additional_data=b"extra")
        assert sasl.SaslOutcome.decode(body.encode()) == body

    def test_round_trip_without_additional_data(self):
        body = sasl.SaslOutcome(code=sasl.SASL_AUTH)
        decoded = sasl.SaslOutcome.decode(body.encode())
        assert decoded == body
        assert decoded.additional_data is None

    def test_descriptor_is_0x44(self):
        assert enc.peek_descriptor(sasl.SaslOutcome(code=0).encode()) == 0x44

    def test_code_is_encoded_as_a_ubyte(self):
        assert sasl.SaslOutcome(code=1).encode().endswith(b"\x50\x01")

    @pytest.mark.parametrize(
        ("code", "expected"),
        [(sasl.SASL_OK, True), (sasl.SASL_AUTH, False), (sasl.SASL_SYS_TEMP, False)],
    )
    def test_succeeded_reflects_the_code(self, code, expected):
        assert sasl.SaslOutcome(code=code).succeeded is expected

    def test_describe_explains_known_codes(self):
        assert "bad credentials" in sasl.SaslOutcome(code=sasl.SASL_AUTH).describe()

    def test_describe_falls_back_for_unknown_codes(self):
        assert "unknown sasl-code 9" in sasl.SaslOutcome(code=9).describe()

    def test_missing_mandatory_field_is_rejected(self):
        with pytest.raises(ProtocolError, match="missing its mandatory code"):
            sasl.SaslOutcome.decode(enc.encode_described_list(0x44, []))


class TestSaslFrameDispatch:
    @pytest.mark.parametrize(
        "body",
        [
            sasl.SaslMechanisms(["PLAIN"]),
            sasl.SaslInit(mechanism="PLAIN", initial_response=b"\x00u\x00p"),
            sasl.SaslChallenge(b"c"),
            sasl.SaslResponse(b"r"),
            sasl.SaslOutcome(code=0),
        ],
    )
    def test_dispatcher_returns_the_matching_type(self, body):
        assert sasl.decode_sasl_frame(body.encode()) == body

    def test_symbolic_descriptor_is_accepted(self):
        encoded = enc.encode_described_list("amqp:sasl-outcome:list", [enc.encode_ubyte(0)])
        assert sasl.decode_sasl_frame(encoded) == sasl.SaslOutcome(code=0)

    def test_unknown_descriptor_is_rejected(self):
        with pytest.raises(ProtocolError, match="unknown SASL frame descriptor"):
            sasl.decode_sasl_frame(enc.encode_described_list(0x4F, []))

    def test_wrong_descriptor_for_a_specific_type_is_rejected(self):
        with pytest.raises(ProtocolError, match="expected descriptor 0x44"):
            sasl.SaslOutcome.decode(sasl.SaslResponse(b"r").encode())
