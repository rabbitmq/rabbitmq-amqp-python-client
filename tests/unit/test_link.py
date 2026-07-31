"""Unit tests for link attach/detach, credit, transfers and settlement."""

from __future__ import annotations

import queue
import threading
import time

import pytest

from src.exceptions import (
    AMQPTimeoutError,
    ConsumerError,
    ProtocolError,
    PublisherError,
)
from src.link import (
    LinkRole,
    ReceiverLink,
    SenderLink,
)
from src.wire import (
    Accepted,
    Attach,
    Detach,
    Disposition,
    Error,
    Flow,
    Message,
    Rejected,
    Source,
    Target,
    Transfer,
)


@pytest.fixture
def opened(connect):
    """Return ``(broker, connection, session)`` with the session already begun."""
    broker, connection = connect()
    return broker, connection, connection.open_session()


def _attached_sender(broker, session, **kwargs):
    """Attach a sender link, letting the broker grant its configured credit."""
    sender = SenderLink(**kwargs)
    sender.attach(session, target=Target(address="/queues/q"))
    return sender


def _attached_receiver(broker, session, **kwargs):
    """Attach a receiver link."""
    receiver = ReceiverLink(**kwargs)
    receiver.attach(session, source=Source(address="/queues/q"))
    return receiver


class TestAttach:
    def test_sender_attach_fields(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session)
        _channel, performative, _payload = broker.wait_for(Attach)
        assert performative.name == sender.name
        assert performative.handle == 0
        assert performative.role is LinkRole.SENDER.value
        assert performative.initial_delivery_count == 0
        assert performative.target.address == "/queues/q"
        assert sender.is_attached

    def test_receiver_attach_fields(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        _channel, performative, _payload = broker.wait_for(Attach)
        assert performative.role is LinkRole.RECEIVER.value
        assert performative.initial_delivery_count is None
        assert performative.source.address == "/queues/q"
        assert receiver.is_attached

    def test_generated_names_are_unique(self):
        assert SenderLink().name != SenderLink().name

    def test_extra_attach_fields_are_forwarded(self, opened):
        broker, _connection, session = opened
        sender = SenderLink()
        sender.attach(
            session,
            target=Target(address="/queues/q"),
            snd_settle_mode=1,
            properties={"x-opt": "value"},
        )
        _channel, performative, _payload = broker.wait_for(Attach)
        assert performative.snd_settle_mode == 1
        assert performative.properties == {"x-opt": "value"}

    def test_two_links_get_distinct_handles(self, opened):
        broker, _connection, session = opened
        first = _attached_sender(broker, session)
        second = _attached_sender(broker, session)
        assert (first.handle, second.handle) == (0, 1)

    def test_times_out_without_a_reply(self, opened):
        broker, _connection, session = opened
        broker.auto_respond = False
        with pytest.raises(AMQPTimeoutError, match="attach"):
            SenderLink(attach_timeout=0.2).attach(session, target=Target(address="/queues/q"))

    def test_a_timed_out_attach_frees_its_handle(self, opened):
        broker, _connection, session = opened
        broker.auto_respond = False
        with pytest.raises(AMQPTimeoutError):
            SenderLink(attach_timeout=0.2).attach(session, target=Target(address="/queues/q"))
        assert session._links == {}

    def test_attaching_twice_is_refused(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session)
        with pytest.raises(ProtocolError, match="already been attached"):
            sender.attach(session, target=Target(address="/queues/q"))

    def test_an_unattached_link_has_no_handle(self):
        with pytest.raises(ProtocolError, match="has not been attached"):
            _ = SenderLink().handle


class TestAttachRefusal:
    def test_a_null_target_refuses_a_sender(self, connect):
        broker, connection = connect(broker_kwargs={"refuse_attach": True})
        session = connection.open_session()
        sender = SenderLink()
        with pytest.raises(ProtocolError, match="amqp:not-found"):
            sender.attach(session, target=Target(address="/queues/missing"))
        assert sender.refused
        assert not sender.is_attached
        assert session._links == {}

    def test_a_null_source_refuses_a_receiver(self, connect):
        broker, connection = connect(broker_kwargs={"refuse_attach": True})
        session = connection.open_session()
        receiver = ReceiverLink()
        with pytest.raises(ProtocolError, match="refused"):
            receiver.attach(session, source=Source(address="/queues/missing"))
        assert receiver.refused

    def test_a_bare_detach_is_also_a_refusal(self, connect):
        broker, connection = connect(
            broker_kwargs={"refuse_attach": True, "refusal_sends_attach": False},
        )
        session = connection.open_session()
        sender = SenderLink()
        with pytest.raises(ProtocolError, match="amqp:not-found"):
            sender.attach(session, target=Target(address="/queues/missing"))
        assert sender.remote_attach is None

    def test_the_refusal_reason_is_reported(self, connect):
        broker, connection = connect(
            broker_kwargs={"refuse_attach": True, "refusal_condition": "amqp:unauthorized-access"},
        )
        session = connection.open_session()
        with pytest.raises(ProtocolError, match="amqp:unauthorized-access: the broker refused this link"):
            SenderLink().attach(session, target=Target(address="/queues/q"))

    def test_the_caller_chooses_the_exception(self, connect):
        broker, connection = connect(broker_kwargs={"refuse_attach": True})
        session = connection.open_session()
        with pytest.raises(PublisherError, match="amqp:not-found"):
            SenderLink().attach(
                session,
                target=Target(address="/queues/q"),
                on_refused=lambda refusal: PublisherError(refusal.describe()),
            )
        with pytest.raises(ConsumerError):
            ReceiverLink().attach(
                session,
                source=Source(address="/queues/q"),
                on_refused=lambda refusal: ConsumerError(refusal.describe()),
            )

    def test_the_refusal_exposes_the_wire_detail(self, connect):
        broker, connection = connect(broker_kwargs={"refuse_attach": True})
        session = connection.open_session()
        seen = []
        with pytest.raises(ProtocolError):
            SenderLink().attach(
                session,
                target=Target(address="/queues/q"),
                on_refused=lambda refusal: seen.append(refusal) or ProtocolError("nope"),
            )
        assert seen[0].error.condition == "amqp:not-found"
        assert seen[0].remote_attach is not None
        assert seen[0].link_name


class TestDetach:
    def test_sends_a_closing_detach_and_unregisters(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session)
        sender.detach()
        _channel, performative, _payload = _wait_for(broker, Detach)
        assert performative.handle == 0
        assert performative.closed is True
        assert not sender.is_attached
        assert session._links == {}

    def test_is_idempotent(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session)
        sender.detach()
        sender.detach()
        assert len(_all(broker, Detach)) == 1

    def test_detaching_an_unattached_link_is_a_no_op(self):
        SenderLink().detach()

    def test_carries_an_error_when_given_one(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session)
        sender.detach(Error(condition="amqp:internal-error", description="bye"))
        _channel, performative, _payload = _wait_for(broker, Detach)
        assert performative.error.description == "bye"

    def test_completes_without_a_reply(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session, detach_timeout=0.2)
        broker.auto_respond = False
        sender.detach()
        assert not sender.is_attached

    def test_a_broker_initiated_detach_fails_the_link(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session)
        broker.auto_respond = False
        broker.send(
            session.channel,
            Detach(handle=sender.handle, closed=True, error=Error(condition="amqp:link:stolen")),
        )
        _poll(lambda: not sender.is_attached)
        assert not sender.is_attached
        with pytest.raises(ProtocolError, match="amqp:link:stolen"):
            sender.send_transfer(b"tag", Message("x"))
        assert session._links == {}


class TestSenderCredit:
    def test_no_credit_times_out(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session, credit_timeout=0.2)
        assert sender.link_credit == 0
        with pytest.raises(AMQPTimeoutError, match="no credit"):
            sender.send_transfer(b"tag", Message("hello"))

    def test_an_inbound_flow_grants_credit(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 5})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 5)
        assert sender.link_credit == 5

    def test_credit_is_computed_from_the_receivers_delivery_count(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 2})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 2)
        sender.send_transfer(b"a", Message("1"))
        assert sender.link_credit == 1
        assert sender.delivery_count == 1
        broker.grant_credit(session.channel, sender.handle, 4)
        _poll(lambda: sender.link_credit == 3)
        assert sender.link_credit == 3

    def test_a_blocked_send_resumes_when_credit_arrives(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session, credit_timeout=5.0)
        results = []

        def send():
            results.append(sender.send_transfer(b"tag", Message("hello")))

        thread = threading.Thread(target=send, daemon=True)
        thread.start()
        time.sleep(0.1)
        assert not results
        broker.grant_credit(session.channel, sender.handle, 1)
        thread.join(3.0)
        assert results == [0]


class TestSendTransfer:
    def test_sends_one_transfer_and_consumes_credit(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 3})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 3)
        message = Message("hello")
        delivery_id = sender.send_transfer(b"tag-1", message)
        _channel, performative, payload = _wait_for(broker, Transfer)
        assert delivery_id == 0
        assert performative.delivery_tag == b"tag-1"
        assert performative.settled is False
        assert performative.more is False
        assert payload == message.encode()
        assert sender.link_credit == 2

    def test_fragments_a_message_larger_than_one_frame(self, connect):
        broker, connection = connect(
            max_frame_size=1024,
            broker_kwargs={"initial_credit": 3, "max_frame_size": 1024, "auto_settle": False},
        )
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 3)
        message = Message(b"x" * 4000)
        sender.send_transfer(b"tag-1", message)
        transfers = _all(broker, Transfer, with_payload=True)
        assert len(transfers) > 1
        assert [performative.more for performative, _ in transfers[:-1]] == [True] * (len(transfers) - 1)
        assert transfers[-1][0].more is False
        assert b"".join(payload for _, payload in transfers) == message.encode()
        assert all(len(payload) <= 1024 for _, payload in transfers)

    def test_refuses_to_send_on_a_detached_link(self, opened):
        broker, _connection, session = opened
        sender = _attached_sender(broker, session)
        sender.detach()
        with pytest.raises(ProtocolError, match="not attached"):
            sender.send_transfer(b"tag", Message("x"))


class TestPendingDeliveries:
    def test_the_disposition_resolves_the_waiter(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 3})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 3)
        pending = sender.register_pending(b"tag-1")
        sender.send_transfer(b"tag-1", Message("hello"))
        assert pending.wait(5.0) == Accepted()
        assert pending.delivery_id == 0
        assert pending.settled is True

    def test_a_range_disposition_resolves_every_delivery_in_it(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 5, "auto_settle": False})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 5)
        pendings = []
        for index in range(3):
            tag = f"tag-{index}".encode()
            pendings.append(sender.register_pending(tag))
            sender.send_transfer(tag, Message("x"))
        broker.send(
            session.channel,
            Disposition(role=True, first=0, last=2, settled=True, state=Rejected(error=Error(condition="amqp:x"))),
        )
        for pending in pendings:
            outcome = pending.wait(5.0)
            assert isinstance(outcome, Rejected)
        assert sender._pending_by_id == {}
        assert sender._pending_by_tag == {}

    def test_a_disposition_outside_the_range_is_ignored(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 5, "auto_settle": False})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 5)
        pending = sender.register_pending(b"tag-0")
        sender.send_transfer(b"tag-0", Message("x"))
        broker.send(session.channel, Disposition(role=True, first=7, last=9, settled=True, state=Accepted()))
        time.sleep(0.2)
        assert not pending.is_resolved

    def test_waiting_times_out_when_no_disposition_arrives(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 2, "auto_settle": False})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 2)
        pending = sender.register_pending(b"tag-1")
        sender.send_transfer(b"tag-1", Message("hello"))
        with pytest.raises(AMQPTimeoutError, match="no disposition"):
            pending.wait(0.2)

    def test_a_presettled_send_resolves_immediately(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 2})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 2)
        pending = sender.register_pending(b"tag-1")
        sender.send_transfer(b"tag-1", Message("hello"), settled=True)
        assert pending.is_resolved
        assert pending.wait(0.0) == Accepted()
        _channel, performative, _payload = _wait_for(broker, Transfer)
        assert performative.settled is True

    def test_a_lost_connection_fails_every_waiter(self, connect):
        broker, connection = connect(broker_kwargs={"initial_credit": 2, "auto_settle": False})
        session = connection.open_session()
        sender = _attached_sender(broker, session)
        _poll(lambda: sender.link_credit == 2)
        pending = sender.register_pending(b"tag-1")
        sender.send_transfer(b"tag-1", Message("hello"))
        broker.drop_connection()
        with pytest.raises(ProtocolError):
            pending.wait(5.0)


class TestReceiverFlow:
    def test_flow_grants_credit_on_this_link(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(10)
        _channel, performative, _payload = _wait_for(broker, Flow)
        assert performative.handle == receiver.handle
        assert performative.link_credit == 10
        assert performative.delivery_count == 0
        assert performative.drain is False
        assert receiver.credit == 10

    def test_drain_is_forwarded(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(1, drain=True)
        _channel, performative, _payload = _wait_for(broker, Flow)
        assert performative.drain is True

    def test_flow_needs_an_attached_link(self):
        with pytest.raises(ProtocolError, match="not attached"):
            ReceiverLink().flow(1)


class TestReceive:
    def test_a_single_frame_delivery_is_returned(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(5)
        message = Message("hello")
        broker.send_transfer(session.channel, receiver.handle, message.encode(), delivery_id=3)
        delivery = receiver.receive(timeout=5.0)
        assert delivery is not None
        assert delivery.delivery_id == 3
        assert delivery.message.body_as_string() == "hello"
        assert delivery.settled is False
        assert receiver.credit == 4
        assert receiver.delivery_count == 1

    def test_a_presettled_delivery_reports_it(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(1)
        broker.send_transfer(session.channel, receiver.handle, Message("x").encode(), settled=True)
        delivery = receiver.receive(timeout=5.0)
        assert delivery.settled is True

    def test_a_multi_frame_delivery_is_reassembled(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(5)
        encoded = Message(b"y" * 500).encode()
        head, tail = encoded[:100], encoded[100:]
        broker.send_transfer(session.channel, receiver.handle, head, delivery_id=1, more=True)
        broker.send_transfer(session.channel, receiver.handle, tail, delivery_id=None, delivery_tag=None)
        delivery = receiver.receive(timeout=5.0)
        assert delivery.delivery_id == 1
        assert delivery.message.body_as_bytes() == b"y" * 500
        assert receiver.delivery_count == 1

    def test_an_aborted_delivery_is_discarded(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(5)
        broker.send_transfer(session.channel, receiver.handle, b"partial", delivery_id=1, more=True)
        broker.send_transfer(session.channel, receiver.handle, b"", delivery_id=None, aborted=True, more=False)
        assert receiver.receive(timeout=0.3) is None
        broker.send_transfer(session.channel, receiver.handle, Message("ok").encode(), delivery_id=2)
        delivery = receiver.receive(timeout=5.0)
        assert delivery.message.body_as_string() == "ok"

    def test_returns_none_on_timeout(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        assert receiver.receive(timeout=0.1) is None

    def test_raises_once_the_connection_is_lost(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        broker.drop_connection()
        with pytest.raises(ProtocolError):
            receiver.receive(timeout=5.0)

    def test_already_received_deliveries_are_drained_before_failing(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(5)
        broker.send_transfer(session.channel, receiver.handle, Message("first").encode(), delivery_id=0)
        _poll(lambda: not receiver._deliveries.empty())
        broker.drop_connection()
        assert receiver.receive(timeout=1.0).message.body_as_string() == "first"
        with pytest.raises(ProtocolError):
            receiver.receive(timeout=1.0)


class TestSettle:
    def test_sends_a_settled_disposition(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.flow(1)
        broker.send_transfer(session.channel, receiver.handle, Message("x").encode(), delivery_id=6)
        delivery = receiver.receive(timeout=5.0)
        receiver.settle(delivery.delivery_id, Accepted())
        _channel, performative, _payload = _wait_for(broker, Disposition)
        assert performative.role is LinkRole.RECEIVER.value
        assert (performative.first, performative.last) == (6, 6)
        assert performative.settled is True
        assert performative.state == Accepted()

    def test_settling_needs_an_attached_link(self):
        with pytest.raises(ProtocolError, match="not attached"):
            ReceiverLink().settle(0, Accepted())


class TestFlowProperties:
    def test_properties_seen_before_a_handler_is_registered_are_replayed(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        broker.grant_credit(session.channel, receiver.handle, 1, properties={"rabbitmq:active": True})
        _poll(lambda: bool(receiver._flow_properties))
        seen = []
        receiver.on_flow_properties(seen.append)
        assert seen == [{"rabbitmq:active": True}]

    def test_later_properties_reach_the_handler_directly(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        seen = queue.Queue()
        receiver.on_flow_properties(seen.put)
        broker.grant_credit(session.channel, receiver.handle, 1, properties={"rabbitmq:active": False})
        assert seen.get(timeout=5.0) == {"rabbitmq:active": False}

    def test_the_buffer_is_bounded(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session, flow_properties_buffer=2)
        for index in range(5):
            broker.grant_credit(session.channel, receiver.handle, 1, properties={"n": index})
        _poll(lambda: {"n": 4} in receiver._flow_properties)
        seen = []
        receiver.on_flow_properties(seen.append)
        assert seen == [{"n": 3}, {"n": 4}]

    def test_a_flow_without_properties_is_not_buffered(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        broker.grant_credit(session.channel, receiver.handle, 7)
        _poll(lambda: receiver.credit == 7)
        assert list(receiver._flow_properties) == []

    def test_a_raising_handler_does_not_break_the_reader(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        calls = []

        def handler(properties):
            calls.append(properties)
            raise RuntimeError("boom")

        receiver.on_flow_properties(handler)
        broker.grant_credit(session.channel, receiver.handle, 1, properties={"n": 1})
        _poll(lambda: bool(calls))
        broker.grant_credit(session.channel, receiver.handle, 2, properties={"n": 2})
        _poll(lambda: len(calls) == 2)
        assert len(calls) == 2

    def test_unregistering_resumes_buffering(self, opened):
        broker, _connection, session = opened
        receiver = _attached_receiver(broker, session)
        receiver.on_flow_properties(lambda _properties: None)
        receiver.on_flow_properties(None)
        broker.grant_credit(session.channel, receiver.handle, 1, properties={"n": 1})
        _poll(lambda: bool(receiver._flow_properties))
        assert list(receiver._flow_properties) == [{"n": 1}]


def _wait_for(broker, performative_type, timeout=5.0):
    """Return the first frame of ``performative_type`` the broker received."""
    return broker.wait_for(performative_type, timeout)


def _all(broker, performative_type, timeout=0.5, with_payload=False):
    """Drain and return every ``performative_type`` frame the broker received."""
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
            collected.append((performative, payload) if with_payload else performative)
    return collected


def _poll(predicate, timeout=3.0):
    """Poll ``predicate`` until it holds or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False
