"""Management protocol behaviour: encoding, paths, requests, response validation.

The link-pair and correlation tests drive a real ``Connection`` against the
in-process :class:`~tests.unit.fake_broker.FakeBroker`, so the actual
``attach``/``flow``/``transfer`` code paths run; the broker plays the
``/management`` node by hand because it has no topology of its own.
"""

from __future__ import annotations

import threading

import pytest

from rabbitmq_amqp_python_client import AMQPTimeoutError, ManagementError, ProtocolError
from rabbitmq_amqp_python_client.constants import (
    MANAGEMENT_LINK_CREDIT,
    MANAGEMENT_LINK_NAME,
    MANAGEMENT_NODE_ADDRESS,
    MANAGEMENT_REPLY_TO,
)
from rabbitmq_amqp_python_client.management import (
    BINDINGS_PATH,
    GENERATED_NAME_PREFIX,
    Management,
    bindings_query_path,
    build_request,
    encode_path_segment,
    encode_query_value,
    exchange_path,
    generate_queue_name,
    queue_messages_path,
    queue_path,
    unbind_path,
    validate_response,
)
from rabbitmq_amqp_python_client.wire import (
    EXPIRY_POLICY_LINK_DETACH,
    EXPIRY_POLICY_SESSION_END,
    RCV_SETTLE_MODE_FIRST,
    SND_SETTLE_MODE_SETTLED,
    AmqpSequence,
    AmqpValue,
    Attach,
    Begin,
    Data,
    Flow,
    Message,
    Properties,
    Transfer,
)

#: Short enough to keep a "no response ever arrives" test fast.
REQUEST_TIMEOUT = 1.0

#: Session-local handle the client's receiver half is attached with.
RECEIVER_HANDLE = 1


def _response(message_id, subject, body=None, *, correlation_id=...):
    """Build a response message echoing ``message_id`` unless told otherwise."""
    return Message(
        body=body,
        properties=Properties(
            subject=subject,
            correlation_id=message_id if correlation_id is ... else correlation_id,
        ),
    )


class TestPathEncoding:
    """§6: path segments use percent encoding, never form encoding."""

    @pytest.mark.parametrize(
        ("raw", "encoded"),
        [
            ("orders", "orders"),
            ("my queue", "my%20queue"),
            ("a/b", "a%2Fb"),
            ("a+b", "a%2Bb"),
            ("100%", "100%25"),
            ("key;args=", "key%3Bargs%3D"),
            ("café", "caf%C3%A9"),
            ("~-._", "~-._"),
            ("", ""),
        ],
    )
    def test_percent_encodes_everything_outside_the_unreserved_set(self, raw, encoded):
        assert encode_path_segment(raw) == encoded

    def test_hex_digits_are_uppercase(self):
        assert encode_path_segment("é") == "%C3%A9"

    def test_unreserved_characters_survive_untouched(self):
        unreserved = "abcXYZ0189-._~"
        assert encode_path_segment(unreserved) == unreserved


class TestQueryEncoding:
    """§6: query values use standard form encoding, so a space becomes ``+``."""

    @pytest.mark.parametrize(
        ("raw", "encoded"),
        [
            ("orders", "orders"),
            ("my queue", "my+queue"),
            ("a/b", "a%2Fb"),
            ("a+b", "a%2Bb"),
            ("café", "caf%C3%A9"),
            ("", ""),
        ],
    )
    def test_form_encodes_values(self, raw, encoded):
        assert encode_query_value(raw) == encoded

    def test_space_differs_from_the_path_encoding(self):
        assert encode_query_value("a b") == "a+b"
        assert encode_path_segment("a b") == "a%20b"


class TestPaths:
    def test_queue_paths(self):
        assert queue_path("my queue") == "/queues/my%20queue"
        assert queue_messages_path("my queue") == "/queues/my%20queue/messages"

    def test_exchange_path(self):
        assert exchange_path("my/exchange") == "/exchanges/my%2Fexchange"

    def test_unbind_path_for_a_queue_destination(self):
        assert unbind_path("ex", "q", "key", to_queue=True) == "/bindings/src=ex;dstq=q;key=key;args="

    def test_unbind_path_for_an_exchange_destination(self):
        assert unbind_path("ex", "other", "key", to_queue=False) == "/bindings/src=ex;dste=other;key=key;args="

    def test_unbind_path_percent_encodes_every_placeholder(self):
        assert unbind_path("a b", "c;d", "e=f", to_queue=True) == "/bindings/src=a%20b;dstq=c%3Bd;key=e%3Df;args="

    def test_unbind_path_always_ends_with_an_empty_args_segment(self):
        assert unbind_path("ex", "q", "", to_queue=True).endswith(";key=;args=")

    def test_bindings_query_path_form_encodes_values(self):
        assert bindings_query_path("a b", "c d", "e f", to_queue=True) == "/bindings?src=a+b&dstq=c+d&key=e+f"

    def test_bindings_query_path_uses_dste_for_an_exchange(self):
        assert bindings_query_path("ex", "other", "k", to_queue=False) == "/bindings?src=ex&dste=other&key=k"


class TestGeneratedQueueName:
    def test_uses_the_documented_prefix(self):
        assert generate_queue_name().startswith(GENERATED_NAME_PREFIX)

    def test_is_unpadded_base64url(self):
        suffix = generate_queue_name()[len(GENERATED_NAME_PREFIX) :]
        assert "=" not in suffix
        assert "+" not in suffix
        assert "/" not in suffix
        assert len(suffix) == 22  # 16 md5 bytes -> 22 base64 characters without padding

    def test_is_unique_per_call(self):
        assert len({generate_queue_name() for _ in range(50)}) == 50

    def test_survives_path_encoding_unchanged(self):
        name = generate_queue_name()
        assert encode_path_segment(name) == name


class TestBuildRequest:
    def test_sets_the_four_mandatory_properties(self):
        properties = build_request("id-1", "PUT", "/queues/q", {"durable": True}).properties
        assert properties is not None
        assert properties.message_id == "id-1"
        assert properties.to == "/queues/q"
        assert properties.subject == "PUT"
        assert properties.reply_to == MANAGEMENT_REPLY_TO

    def test_wraps_the_body_in_an_amqp_value_section(self):
        message = build_request("id-1", "POST", BINDINGS_PATH, {"source": "ex"})
        assert isinstance(message.body, AmqpValue)
        assert message.body.value == {"source": "ex"}

    def test_encodes_a_body_less_operation_as_an_amqp_value_null(self):
        # RabbitMQ matches the decoded body against null and errors on a
        # missing body section, so the section is always present.
        message = build_request("id-1", "DELETE", "/queues/q", None)
        assert isinstance(message.body, AmqpValue)
        assert message.body.value is None
        decoded = Message.decode(message.encode())
        assert isinstance(decoded.body, AmqpValue)
        assert decoded.body.value is None

    def test_round_trips_through_the_wire_codec(self):
        decoded = Message.decode(build_request("id-1", "GET", "/queues/q%20x", None).encode())
        assert decoded.properties is not None
        assert decoded.properties.subject == "GET"
        assert decoded.properties.to == "/queues/q%20x"
        assert decoded.properties.reply_to == MANAGEMENT_REPLY_TO

    def test_round_trips_a_map_body(self):
        decoded = Message.decode(build_request("id-1", "PUT", "/queues/q", {"durable": True, "n": 3}).encode())
        assert isinstance(decoded.body, AmqpValue)
        assert decoded.body.value == {"durable": True, "n": 3}


class TestValidateResponseOrder:
    """§4: the five checks run in order, and the order is observable."""

    def test_returns_the_decoded_body_when_every_check_passes(self):
        assert validate_response("id-1", _response("id-1", "200", AmqpValue({"name": "q"})), {200}) == {"name": "q"}

    def test_rejects_a_subject_that_is_not_an_integer(self):
        with pytest.raises(ProtocolError, match="not an integer status code"):
            validate_response("id-1", _response("id-1", "OK", AmqpValue({})), {200})

    def test_rejects_a_missing_subject(self):
        with pytest.raises(ProtocolError, match="no subject"):
            validate_response("id-1", Message(properties=Properties(correlation_id="id-1")), {200})

    def test_rejects_a_response_with_no_properties_at_all(self):
        with pytest.raises(ProtocolError, match="no subject"):
            validate_response("id-1", Message(), {200})

    def test_a_malformed_subject_beats_a_correlation_mismatch(self):
        response = _response("id-1", "not-a-code", correlation_id="other")
        with pytest.raises(ProtocolError, match="not an integer status code"):
            validate_response("id-1", response, {200})

    @pytest.mark.parametrize(
        ("code", "condition"),
        [(400, "bad request"), (404, "not found"), (409, "precondition failed")],
    )
    def test_known_error_codes_raise_their_own_condition(self, code, condition):
        with pytest.raises(ManagementError, match=condition) as failure:
            validate_response("id-1", _response("id-1", str(code)), {200})
        assert failure.value.status_code == code

    @pytest.mark.parametrize("code", [400, 404, 409])
    def test_known_error_codes_win_even_when_the_caller_expects_them(self, code):
        with pytest.raises(ManagementError) as failure:
            validate_response("id-1", _response("id-1", str(code)), {200, 201, code})
        assert failure.value.status_code == code

    @pytest.mark.parametrize("code", [400, 404, 409])
    def test_known_error_codes_beat_a_correlation_mismatch(self, code):
        response = _response("id-1", str(code), correlation_id="somebody-else")
        with pytest.raises(ManagementError) as failure:
            validate_response("id-1", response, {200})
        assert failure.value.status_code == code

    def test_rejects_a_correlation_id_that_does_not_echo_the_message_id(self):
        response = _response("id-1", "200", AmqpValue({"name": "q"}), correlation_id="id-2")
        with pytest.raises(ProtocolError, match="does not match request message-id"):
            validate_response("id-1", response, {200})

    def test_rejects_a_missing_correlation_id(self):
        with pytest.raises(ProtocolError, match="does not match request message-id"):
            validate_response("id-1", _response("id-1", "200", correlation_id=None), {200})

    def test_a_correlation_mismatch_beats_an_unexpected_code(self):
        with pytest.raises(ProtocolError, match="does not match request message-id"):
            validate_response("id-1", _response("id-1", "201", correlation_id="id-2"), {200})

    def test_rejects_an_otherwise_valid_but_unexpected_code(self):
        with pytest.raises(ManagementError, match="answered 201, expected one of 200") as failure:
            validate_response("id-1", _response("id-1", "201"), {200})
        assert failure.value.status_code == 201

    def test_accepts_a_binary_correlation_id_that_matches(self):
        assert validate_response("id-1", _response("id-1", "200", correlation_id=b"id-1"), {200}) is None


class TestValidateResponseBody:
    def test_decodes_an_amqp_value_map(self):
        assert validate_response("id", _response("id", "200", AmqpValue({"a": 1})), {200}) == {"a": 1}

    def test_decodes_an_amqp_sequence_list(self):
        assert validate_response("id", _response("id", "200", AmqpSequence([{"a": 1}])), {200}) == [{"a": 1}]

    def test_treats_an_absent_body_as_none(self):
        assert validate_response("id", _response("id", "204"), {204}) is None

    def test_treats_an_empty_data_section_as_none(self):
        assert validate_response("id", _response("id", "204", Data(b"")), {204}) is None

    def test_rejects_a_body_shape_the_management_api_never_uses(self):
        with pytest.raises(ProtocolError, match="unexpected shape"):
            validate_response("id", _response("id", "200", Data(b"payload")), {200})


class TestManagementLinkPair:
    """§2: one session, two links with one name, paired properties, initial credit."""

    def test_attaches_a_sender_and_a_receiver_sharing_one_name(self, connect):
        broker, connection = connect()
        management = connection.management()
        attaches = _attaches(broker)
        assert [item.name for item in attaches] == [MANAGEMENT_LINK_NAME, MANAGEMENT_LINK_NAME]
        assert [item.role for item in attaches] == [False, True]
        assert management.is_open

    def test_uses_handle_zero_for_the_sender_and_one_for_the_receiver(self, connect):
        broker, connection = connect()
        connection.management()
        assert [item.handle for item in _attaches(broker)] == [0, RECEIVER_HANDLE]

    def test_marks_both_links_as_paired(self, connect):
        broker, connection = connect()
        connection.management()
        for attach in _attaches(broker):
            assert attach.properties == {"paired": True}

    def test_pre_settles_requests_and_settles_responses_on_first(self, connect):
        broker, connection = connect()
        connection.management()
        for attach in _attaches(broker):
            assert attach.snd_settle_mode == SND_SETTLE_MODE_SETTLED
            assert attach.rcv_settle_mode == RCV_SETTLE_MODE_FIRST

    def test_points_both_termini_at_the_management_node(self, connect):
        broker, connection = connect()
        connection.management()
        for attach in _attaches(broker):
            assert attach.source is not None
            assert attach.target is not None
            assert attach.source.address == MANAGEMENT_NODE_ADDRESS
            assert attach.target.address == MANAGEMENT_NODE_ADDRESS
            assert attach.source.expiry_policy == EXPIRY_POLICY_LINK_DETACH
            assert attach.target.expiry_policy == EXPIRY_POLICY_SESSION_END
            assert attach.source.timeout == 0
            assert attach.target.timeout == 0
            assert attach.source.dynamic is False
            assert attach.target.dynamic is False

    def test_grants_generous_credit_on_the_receiver(self, connect):
        broker, connection = connect()
        connection.management()
        _channel, flow, _payload = broker.wait_for(Flow)
        assert flow.handle == RECEIVER_HANDLE
        assert flow.link_credit == MANAGEMENT_LINK_CREDIT

    def test_opens_a_session_of_its_own(self, connect):
        broker, connection = connect()
        connection.open_session()
        connection.management()
        assert len(broker.all_received(Begin)) == 2

    def test_open_is_idempotent(self, connect):
        broker, connection = connect()
        management = connection.management()
        management.open()
        assert management.is_open
        assert len(broker.all_received(Attach)) == 2

    def test_close_is_idempotent(self, connect):
        _broker, connection = connect()
        management = connection.management()
        management.close()
        management.close()
        assert not management.is_open


class TestConnectionManagement:
    def test_is_a_lazy_singleton(self, connect):
        _broker, connection = connect()
        assert connection.management() is connection.management()

    def test_replaces_an_explicitly_closed_endpoint(self, connect):
        _broker, connection = connect()
        first = connection.management()
        first.close()
        second = connection.management()
        assert second is not first
        assert second.is_open

    def test_connection_close_tears_the_pair_down(self, connect):
        _broker, connection = connect()
        management = connection.management()
        connection.close()
        assert not management.is_open

    def test_requests_are_refused_once_closed(self, connect):
        _broker, connection = connect()
        management = connection.management()
        management.close()
        with pytest.raises(ManagementError, match="not open"):
            management.queue_info("q")


class TestCorrelationEngine:
    def test_resolves_a_request_with_the_response_that_echoes_its_id(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 10})
        management = _open_management(connection)
        outcome = _in_background(lambda: management.queue_info("orders"))
        request = _serve_one(broker, "200", {"name": "orders", "type": "classic", "message_count": 7})
        info = outcome.result()
        assert request.properties.subject == "GET"
        assert request.properties.to == "/queues/orders"
        assert info.name == "orders"
        assert info.message_count == 7

    def test_surfaces_a_broker_error_code_to_the_caller(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 10})
        management = _open_management(connection)
        outcome = _in_background(lambda: management.queue_info("missing"))
        _serve_one(broker, "404")
        with pytest.raises(ManagementError, match="not found") as failure:
            outcome.result()
        assert failure.value.status_code == 404

    def test_times_out_when_no_response_ever_arrives(self, connect):
        _broker, connection = connect(broker_kwargs={"initial_credit": 10})
        management = _open_management(connection)
        with pytest.raises(AMQPTimeoutError, match="no management response"):
            management.queue_info("orders")

    def test_ignores_a_response_whose_correlation_id_matches_nothing(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 10})
        management = _open_management(connection)
        outcome = _in_background(lambda: management.queue_info("orders"))
        _serve_one(broker, "200", {"name": "orders"}, correlation_id="not-a-pending-request")
        with pytest.raises(AMQPTimeoutError):
            outcome.result()

    def test_correlates_concurrent_requests_independently(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 10})
        management = _open_management(connection, request_timeout=10.0)
        first = _in_background(lambda: management.queue_info("one"))
        second = _in_background(lambda: management.queue_info("two"))
        requests = [_read_request(broker), _read_request(broker)]
        # Answer in the opposite order, to prove the table matches, not arrival order.
        for channel, request in reversed(requests):
            name = request.properties.to.removeprefix("/queues/")
            _send_response(broker, channel, request.properties.message_id, "200", {"name": name})
        assert {first.result().name, second.result().name} == {"one", "two"}

    def test_a_dead_connection_wakes_every_waiting_caller(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 10})
        management = _open_management(connection, request_timeout=30.0)
        outcome = _in_background(lambda: management.queue_info("orders"))
        _read_request(broker)
        broker.drop_connection()
        with pytest.raises(Exception) as failure:  # noqa: B017 - the transport decides the type
            outcome.result()
        assert not isinstance(failure.value, AMQPTimeoutError), "the caller waited out the request timeout"


# --- helpers ------------------------------------------------------------


def _open_management(connection, request_timeout=REQUEST_TIMEOUT):
    """Open a management endpoint whose requests time out quickly."""
    management = Management(connection, request_timeout=request_timeout)
    management.open()
    return management


def _attaches(broker):
    """Return the two ``attach`` performatives the client sent, in order.

    Both are already queued by the time ``open()`` returns, because the client
    waits for the broker's reply to each one and the fake broker records a frame
    before answering it.
    """
    attaches = broker.all_received(Attach)[:2]
    assert len(attaches) == 2, f"expected two attaches, saw {len(attaches)}"
    return attaches


class _Outcome:
    """The result of a call made on a background thread."""

    def __init__(self, call):
        self._value = None
        self._error = None
        self._thread = threading.Thread(target=self._run, args=(call,), daemon=True)
        self._thread.start()

    def _run(self, call):
        try:
            self._value = call()
        except BaseException as error:  # re-raised on the test thread by result()
            self._error = error

    def result(self, timeout=5.0):
        """Join the worker and return its value, re-raising whatever it raised."""
        self._thread.join(timeout)
        assert not self._thread.is_alive(), "the management call never returned"
        if self._error is not None:
            raise self._error
        return self._value


def _in_background(call):
    """Run ``call`` on a worker thread, so the test can answer its request."""
    return _Outcome(call)


def _read_request(broker):
    """Wait for the next request the client sent; return its channel and message."""
    channel, transfer, payload = broker.wait_for(Transfer)
    assert transfer.settled, "management requests must be pre-settled"
    return channel, Message.decode(payload)


def _send_response(broker, channel, message_id, subject, body=None, *, correlation_id=...):
    """Push one response onto the client's receiver half of the link pair."""
    response = _response(message_id, subject, None if body is None else AmqpValue(body), correlation_id=correlation_id)
    broker.send_transfer(
        channel,
        RECEIVER_HANDLE,
        response.encode(),
        delivery_id=next(_delivery_ids),
        settled=True,
    )


def _serve_one(broker, subject, body=None, *, correlation_id=...):
    """Read one request and answer it; return the decoded request."""
    channel, request = _read_request(broker)
    _send_response(broker, channel, request.properties.message_id, subject, body, correlation_id=correlation_id)
    return request


#: Session-scoped delivery-ids for broker-sent transfers; only monotonicity matters.
_delivery_ids = iter(range(100_000))
