"""Unit tests for session begin/end, frame routing and transfer-id bookkeeping."""

from __future__ import annotations

import queue
import threading
import time

import pytest

from rabbitmq_amqp_python_client.exceptions import (
    AMQPTimeoutError,
    ProtocolError,
)
from rabbitmq_amqp_python_client.link import LinkRole
from rabbitmq_amqp_python_client.session import DEFAULT_WINDOW, Session
from rabbitmq_amqp_python_client.wire import (
    Accepted,
    Attach,
    Begin,
    Disposition,
    End,
    Error,
    Flow,
    Transfer,
)


class _LinkDouble:
    """Records the frames a session routes to it."""

    def __init__(self, role=LinkRole.RECEIVER, name="link-double"):
        self.name = name
        self.role = role
        self.frames = []
        self.lost = []
        self.ended = []

    def handle_frame(self, performative, payload):
        self.frames.append((performative, payload))

    def transport_lost(self, error):
        self.lost.append(error)

    def session_ended(self, error):
        self.ended.append(error)


@pytest.fixture
def session(connect):
    """An open session on a connection backed by the fake broker."""
    broker, connection = connect()
    return broker, connection, connection.open_session()


class TestBegin:
    def test_sends_begin_with_the_declared_windows(self, connect):
        broker, connection = connect()
        connection.open_session()
        channel, performative, _payload = broker.wait_for(Begin)
        assert channel == 0
        assert performative.next_outgoing_id == 0
        assert performative.incoming_window == DEFAULT_WINDOW
        assert performative.outgoing_window == DEFAULT_WINDOW
        assert performative.remote_channel is None

    def test_opens_on_the_next_free_channel(self, connect):
        _broker, connection = connect()
        assert connection.open_session().channel == 0
        assert connection.open_session().channel == 1

    def test_is_open_once_the_broker_replies(self, session):
        _broker, _connection, opened = session
        assert opened.is_open
        assert opened.remote_begin is not None

    def test_adopts_the_brokers_handle_max(self, connect):
        _broker, connection = connect(broker_kwargs={"handle_max": 12})
        assert connection.open_session().handle_max == 12

    def test_times_out_without_a_reply(self, connect):
        _broker, connection = connect(broker_kwargs={"auto_respond": False})
        with pytest.raises(AMQPTimeoutError, match="begin"):
            Session(begin_timeout=0.2).begin(connection)

    def test_a_timed_out_begin_releases_its_channel(self, connect):
        _broker, connection = connect(broker_kwargs={"auto_respond": False})
        with pytest.raises(AMQPTimeoutError):
            Session(begin_timeout=0.2).begin(connection)
        assert connection._sessions == {}

    def test_beginning_twice_is_refused(self, session):
        _broker, connection, opened = session
        with pytest.raises(ProtocolError, match="already begun"):
            opened.begin(connection)

    def test_an_unopened_session_has_no_connection(self):
        with pytest.raises(ProtocolError, match="has not begun"):
            _ = Session().connection


class TestEnd:
    def test_sends_end_and_releases_the_channel(self, session):
        broker, connection, opened = session
        opened.end()
        broker.wait_for(End)
        assert not opened.is_open
        assert connection._sessions == {}

    def test_is_idempotent(self, session):
        broker, _connection, opened = session
        opened.end()
        opened.end()
        assert len(broker.all_received(End)) == 1

    def test_ending_a_session_that_never_begun_is_a_no_op(self):
        Session().end()

    def test_carries_an_error_when_given_one(self, session):
        broker, _connection, opened = session
        opened.end(Error(condition="amqp:internal-error", description="bye"))
        _channel, performative, _payload = broker.wait_for(End)
        assert performative.error.condition == "amqp:internal-error"

    def test_completes_without_a_reply(self, connect):
        broker, connection = connect()
        opened = Session(end_timeout=0.2)
        opened.begin(connection)
        broker.auto_respond = False
        opened.end()
        assert not opened.is_open

    def test_a_broker_initiated_end_is_echoed_and_unregisters(self, session):
        broker, connection, opened = session
        broker.auto_respond = False
        broker.send(opened.channel, End(error=Error(condition="amqp:internal-error", description="boom")))
        _wait(lambda: not opened.is_open)
        assert not opened.is_open
        assert connection._sessions == {}
        _channel, performative, _payload = broker.wait_for(End)
        assert performative.error is None

    def test_a_broker_initiated_end_detaches_its_links(self, session):
        broker, _connection, opened = session
        link = _LinkDouble()
        opened.allocate_handle(link)
        broker.auto_respond = False
        broker.send(opened.channel, End())
        _wait(lambda: bool(link.ended))
        assert isinstance(link.ended[0], ProtocolError)


class TestHandleAllocation:
    def test_allocates_the_lowest_free_handle(self, session):
        _broker, _connection, opened = session
        first, second = _LinkDouble(name="a"), _LinkDouble(name="b")
        assert opened.allocate_handle(first) == 0
        assert opened.allocate_handle(second) == 1

    def test_a_released_handle_is_reused(self, session):
        _broker, _connection, opened = session
        first = _LinkDouble(name="a")
        opened.allocate_handle(first)
        opened.allocate_handle(_LinkDouble(name="b"))
        opened.unregister_link(first)
        assert opened.allocate_handle(_LinkDouble(name="c")) == 0

    def test_unregistering_clears_every_index(self, session):
        _broker, _connection, opened = session
        link = _LinkDouble(name="a")
        handle = opened.allocate_handle(link)
        opened.handle_frame(Attach(name="a", handle=handle, role=True), b"")
        opened.unregister_link(link)
        assert opened._links == {}
        assert opened._links_by_name == {}
        assert opened._links_by_remote_handle == {}


class TestFrameRouting:
    def test_attach_is_routed_by_link_name(self, session):
        _broker, _connection, opened = session
        link = _LinkDouble(name="named-link")
        opened.allocate_handle(link)
        performative = Attach(name="named-link", handle=7, role=True)
        opened.handle_frame(performative, b"")
        assert link.frames == [(performative, b"")]
        assert opened._links_by_remote_handle[7] is link

    def test_later_frames_use_the_handle_the_broker_chose(self, session):
        _broker, _connection, opened = session
        link = _LinkDouble(name="named-link")
        opened.allocate_handle(link)
        opened.handle_frame(Attach(name="named-link", handle=7, role=True), b"")
        transfer = Transfer(handle=7, delivery_id=0, delivery_tag=b"t")
        opened.handle_frame(transfer, b"payload")
        assert link.frames[-1] == (transfer, b"payload")

    def test_a_link_flow_reaches_the_link(self, session):
        _broker, _connection, opened = session
        link = _LinkDouble()
        handle = opened.allocate_handle(link)
        performative = Flow(incoming_window=1, next_outgoing_id=0, outgoing_window=1, handle=handle, link_credit=5)
        opened.handle_frame(performative, b"")
        assert link.frames == [(performative, b"")]

    def test_a_session_flow_asking_for_an_echo_is_answered(self, session):
        broker, _connection, opened = session
        opened.handle_frame(Flow(incoming_window=1, next_outgoing_id=3, outgoing_window=1, echo=True), b"")
        _channel, performative, _payload = broker.wait_for(Flow)
        assert performative.next_incoming_id == 3

    def test_a_receiver_disposition_only_reaches_sender_links(self, session):
        _broker, _connection, opened = session
        sender, receiver = _LinkDouble(LinkRole.SENDER, "s"), _LinkDouble(LinkRole.RECEIVER, "r")
        opened.allocate_handle(sender)
        opened.allocate_handle(receiver)
        performative = Disposition(role=True, first=0, last=1, settled=True, state=Accepted())
        opened.handle_frame(performative, b"")
        assert sender.frames == [(performative, b"")]
        assert receiver.frames == []

    def test_a_sender_disposition_only_reaches_receiver_links(self, session):
        _broker, _connection, opened = session
        sender, receiver = _LinkDouble(LinkRole.SENDER, "s"), _LinkDouble(LinkRole.RECEIVER, "r")
        opened.allocate_handle(sender)
        opened.allocate_handle(receiver)
        performative = Disposition(role=False, first=0)
        opened.handle_frame(performative, b"")
        assert receiver.frames == [(performative, b"")]
        assert sender.frames == []

    def test_frames_for_an_unknown_handle_are_dropped(self, session, caplog):
        _broker, _connection, opened = session
        opened.handle_frame(Transfer(handle=9, delivery_id=0), b"")
        assert "unknown link handle 9" in caplog.text


class TestWindows:
    def test_incoming_transfers_advance_next_incoming_id(self, session):
        _broker, _connection, opened = session
        link = _LinkDouble()
        handle = opened.allocate_handle(link)
        opened.handle_frame(Transfer(handle=handle, delivery_id=4, delivery_tag=b"t"), b"body")
        assert opened._next_incoming_id == 5

    def test_the_incoming_window_is_replenished_once_half_consumed(self, connect):
        broker, connection = connect()
        opened = Session(incoming_window=4)
        opened.begin(connection)
        broker.all_received(Flow)
        link = _LinkDouble()
        handle = opened.allocate_handle(link)
        for delivery_id in range(2):
            opened.handle_frame(Transfer(handle=handle, delivery_id=delivery_id, delivery_tag=b"t"), b"body")
        _channel, performative, _payload = broker.wait_for(Flow)
        assert performative.incoming_window == 4
        assert performative.next_incoming_id == 2

    def test_send_flow_fills_in_the_session_fields(self, session):
        broker, _connection, opened = session
        opened.send_flow(handle=3, delivery_count=1, link_credit=10, drain=True)
        _channel, performative, _payload = broker.wait_for(Flow)
        assert performative.incoming_window == DEFAULT_WINDOW
        assert performative.outgoing_window == DEFAULT_WINDOW
        assert performative.next_outgoing_id == 0
        assert (performative.handle, performative.delivery_count, performative.link_credit) == (3, 1, 10)
        assert performative.drain is True


class TestSendDelivery:
    def test_one_transfer_per_delivery_when_the_payload_fits(self, session):
        broker, _connection, opened = session
        delivery_id = opened.send_delivery(
            handle=0,
            delivery_tag=b"tag",
            payload=b"body",
            settled=False,
            max_fragment=1000,
        )
        assert delivery_id == 0
        transfers = _collect(broker, Transfer)
        assert len(transfers) == 1
        assert transfers[0][0].more is False
        assert transfers[0][1] == b"body"

    def test_a_large_payload_is_fragmented(self, session):
        broker, _connection, opened = session
        opened.send_delivery(
            handle=0,
            delivery_tag=b"tag",
            payload=b"0123456789",
            settled=False,
            max_fragment=4,
        )
        transfers = _collect(broker, Transfer)
        assert [performative.more for performative, _ in transfers] == [True, True, False]
        assert b"".join(payload for _, payload in transfers) == b"0123456789"
        assert transfers[0][0].delivery_tag == b"tag"
        assert transfers[0][0].delivery_id == 0
        assert transfers[1][0].delivery_tag is None
        assert transfers[1][0].delivery_id is None

    def test_the_transfer_id_advances_once_per_delivery(self, session):
        _broker, _connection, opened = session
        first = opened.send_delivery(handle=0, delivery_tag=b"a", payload=b"x" * 10, settled=False, max_fragment=4)
        second = opened.send_delivery(handle=0, delivery_tag=b"b", payload=b"y", settled=False, max_fragment=4)
        assert (first, second) == (0, 1)
        assert opened.next_outgoing_id == 2

    def test_the_delivery_id_hook_runs_before_the_first_frame(self, session):
        broker, _connection, opened = session
        seen = []

        def hook(delivery_id):
            already_received = [performative for _channel, performative, _payload in list(broker.received.queue)]
            seen.append((delivery_id, any(isinstance(item, Transfer) for item in already_received)))

        opened.send_delivery(
            handle=0,
            delivery_tag=b"tag",
            payload=b"body",
            settled=False,
            max_fragment=1000,
            on_delivery_id=hook,
        )
        assert seen == [(0, 0)]

    def test_refuses_to_send_on_an_ended_session(self, session):
        _broker, _connection, opened = session
        opened.end()
        with pytest.raises(ProtocolError, match="not open"):
            opened.send_delivery(handle=0, delivery_tag=b"t", payload=b"x", settled=False, max_fragment=10)


class TestTransportLost:
    def test_a_lost_connection_fails_the_session_and_its_links(self, session):
        broker, connection, opened = session
        link = _LinkDouble()
        opened.allocate_handle(link)
        broker.drop_connection()
        _wait(lambda: bool(link.lost))
        assert isinstance(link.lost[0], BaseException)
        with pytest.raises(ProtocolError, match="no longer usable"):
            opened.send_flow()
        assert connection.state.value == "closed"

    def test_a_waiter_is_woken_when_the_connection_dies(self, connect):
        broker, connection = connect(broker_kwargs={"auto_respond": False})
        opened = Session(begin_timeout=5.0)
        failures = []

        def run():
            try:
                opened.begin(connection)
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        _wait(lambda: opened.channel is not None)
        broker.drop_connection()
        thread.join(3.0)
        assert not thread.is_alive()
        assert failures and isinstance(failures[0], BaseException)


def _collect(broker, performative_type, timeout=0.5):
    """Return every ``performative_type`` frame the broker received, in order."""
    collected = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _channel, performative, payload = broker.received.get(timeout=0.05)
        except queue.Empty:
            if collected:
                break
            continue
        if isinstance(performative, performative_type):
            collected.append((performative, payload))
    return collected


def _wait(predicate, timeout=3.0):
    """Poll ``predicate`` until it holds or ``timeout`` elapses."""
    waited = 0.0
    while waited < timeout:
        if predicate():
            return True
        threading.Event().wait(0.02)
        waited += 0.02
    return False
