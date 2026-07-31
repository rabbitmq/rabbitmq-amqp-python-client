"""Session and link behaviour against a live RabbitMQ broker."""

from __future__ import annotations

import uuid

import pytest

from src import (
    Connection,
    ConnectionParameters,
    ConsumerError,
    ProtocolError,
    PublisherError,
    ReceiverLink,
    SenderLink,
)
from src.wire import (
    Accepted,
    ApplicationProperties,
    Message,
    Properties,
    Rejected,
    Released,
    Source,
    Target,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def connection():
    """An open connection to the local broker, closed on teardown."""
    opened = Connection(ConnectionParameters())
    try:
        yield opened
    finally:
        opened.close()


@pytest.fixture
def session(connection):
    """An open session on the live connection."""
    return connection.open_session()


@pytest.fixture
def queue_address(queue_factory):
    """A freshly created classic queue, deleted after the test."""
    return queue_factory(f"link-it-{uuid.uuid4().hex[:12]}")


def _sender(session, address, **kwargs):
    """Attach a sender link to ``address``."""
    link = SenderLink(**kwargs)
    link.attach(session, target=Target(address=address))
    return link


def _receiver(session, address, **kwargs):
    """Attach a receiver link to ``address``."""
    link = ReceiverLink(**kwargs)
    link.attach(session, source=Source(address=address))
    return link


class TestSessionLifecycle:
    def test_begin_and_end(self, connection):
        opened = connection.open_session()
        assert opened.is_open
        assert opened.channel == 0
        assert opened.remote_begin is not None
        assert opened.handle_max > 0
        opened.end()
        assert not opened.is_open

    def test_many_sessions_on_one_connection(self, connection):
        sessions = [connection.open_session() for _ in range(5)]
        assert [item.channel for item in sessions] == [0, 1, 2, 3, 4]
        for item in sessions:
            item.end()

    def test_using_an_ended_session_is_refused(self, connection):
        opened = connection.open_session()
        opened.end()
        with pytest.raises(ProtocolError):
            SenderLink().attach(opened, target=Target(address="/queues/whatever"))


class TestAttach:
    def test_sender_and_receiver_attach(self, session, queue_address):
        sender = _sender(session, queue_address)
        receiver = _receiver(session, queue_address)
        assert sender.is_attached
        assert receiver.is_attached
        assert sender.remote_attach.target.address == queue_address
        assert receiver.remote_attach.source.address == queue_address
        assert (sender.handle, receiver.handle) == (0, 1)
        receiver.detach()
        sender.detach()

    def test_the_broker_grants_credit_to_a_new_sender(self, session, queue_address):
        sender = _sender(session, queue_address)
        assert _eventually(lambda: sender.link_credit > 0), "the broker granted no credit"
        sender.detach()

    def test_attaching_to_a_missing_queue_is_refused(self, session):
        sender = SenderLink()
        with pytest.raises(PublisherError, match="amqp:not-found"):
            sender.attach(
                session,
                target=Target(address="/queues/does-not-exist-42"),
                on_refused=lambda refusal: PublisherError(refusal.describe()),
            )
        assert sender.refused
        assert session._links == {}

    def test_a_refused_receiver_reports_the_reason(self, session):
        receiver = ReceiverLink()
        with pytest.raises(ConsumerError, match="amqp:not-found"):
            receiver.attach(
                session,
                source=Source(address="/queues/does-not-exist-42"),
                on_refused=lambda refusal: ConsumerError(refusal.describe()),
            )

    def test_a_refused_attach_frees_its_handle(self, session, queue_address):
        with pytest.raises(ProtocolError):
            SenderLink().attach(session, target=Target(address="/queues/does-not-exist-42"))
        sender = _sender(session, queue_address)
        assert sender.handle == 0
        sender.detach()


class TestRoundTrip:
    def test_send_and_receive_one_message(self, session, queue_address):
        sender = _sender(session, queue_address)
        pending = sender.register_pending(b"tag-1")
        message = Message(
            "hello integration",
            properties=Properties(subject="greeting", message_id="m-1"),
            application_properties=ApplicationProperties(value={"kind": "test", "count": 3}),
        )
        delivery_id = sender.send_transfer(b"tag-1", message)
        assert delivery_id == 0
        assert pending.wait(10.0) == Accepted()

        receiver = _receiver(session, queue_address)
        receiver.flow(10)
        delivery = receiver.receive(timeout=10.0)
        assert delivery is not None
        assert delivery.message.body_as_string() == "hello integration"
        assert delivery.message.properties.subject == "greeting"
        assert delivery.message.application_properties.value["kind"] == "test"
        receiver.settle(delivery.delivery_id, Accepted())
        receiver.detach()
        sender.detach()

    def test_transfer_ids_increase_per_delivery(self, session, queue_address):
        sender = _sender(session, queue_address)
        ids = [sender.send_transfer(f"tag-{index}".encode(), Message(f"m{index}")) for index in range(5)]
        assert ids == [0, 1, 2, 3, 4]
        assert sender.delivery_count == 5
        assert session.next_outgoing_id == 5
        sender.detach()

    def test_a_message_larger_than_one_frame_round_trips(self, session, queue_address):
        sender = _sender(session, queue_address)
        body = bytes(index % 251 for index in range(3 * session.connection.max_frame_size))
        pending = sender.register_pending(b"big")
        sender.send_transfer(b"big", Message(body))
        assert pending.wait(10.0) == Accepted()

        receiver = _receiver(session, queue_address)
        receiver.flow(1)
        delivery = receiver.receive(timeout=10.0)
        assert delivery is not None
        assert delivery.message.body_as_bytes() == body
        receiver.settle(delivery.delivery_id, Accepted())
        receiver.detach()
        sender.detach()

    def test_a_presettled_message_needs_no_disposition(self, session, queue_address):
        sender = _sender(session, queue_address)
        pending = sender.register_pending(b"presettled")
        sender.send_transfer(b"presettled", Message("fire and forget"), settled=True)
        assert pending.is_resolved
        assert pending.wait(0.0) == Accepted()

        receiver = _receiver(session, queue_address)
        receiver.flow(1)
        delivery = receiver.receive(timeout=10.0)
        assert delivery is not None
        assert delivery.message.body_as_string() == "fire and forget"
        receiver.settle(delivery.delivery_id, Accepted())
        receiver.detach()
        sender.detach()

    def test_many_messages_keep_their_order(self, session, queue_address):
        sender = _sender(session, queue_address)
        pendings = []
        for index in range(20):
            tag = f"tag-{index}".encode()
            pendings.append(sender.register_pending(tag))
            sender.send_transfer(tag, Message(f"message-{index}"))
        for pending in pendings:
            assert pending.wait(10.0) == Accepted()

        receiver = _receiver(session, queue_address)
        receiver.flow(20)
        bodies = []
        for _ in range(20):
            delivery = receiver.receive(timeout=10.0)
            assert delivery is not None
            bodies.append(delivery.message.body_as_string())
            receiver.settle(delivery.delivery_id, Accepted())
        assert bodies == [f"message-{index}" for index in range(20)]
        receiver.detach()
        sender.detach()


class TestSettlement:
    def test_a_released_message_is_redelivered(self, session, queue_address):
        sender = _sender(session, queue_address)
        sender.send_transfer(b"tag-1", Message("release me"), settled=True)
        sender.detach()

        first = _receiver(session, queue_address)
        first.flow(1)
        delivery = first.receive(timeout=10.0)
        assert delivery is not None
        first.settle(delivery.delivery_id, Released())
        first.detach()

        second = _receiver(session, queue_address)
        second.flow(1)
        redelivered = second.receive(timeout=10.0)
        assert redelivered is not None
        assert redelivered.message.body_as_string() == "release me"
        second.settle(redelivered.delivery_id, Accepted())
        second.detach()

    def test_a_rejected_message_is_not_redelivered(self, session, queue_address):
        from src.wire import Error

        sender = _sender(session, queue_address)
        sender.send_transfer(b"tag-1", Message("reject me"), settled=True)
        sender.detach()

        first = _receiver(session, queue_address)
        first.flow(1)
        delivery = first.receive(timeout=10.0)
        assert delivery is not None
        first.settle(
            delivery.delivery_id,
            Rejected(error=Error(condition="amqp:precondition-failed", description="nope")),
        )
        first.detach()

        second = _receiver(session, queue_address)
        second.flow(1)
        assert second.receive(timeout=1.0) is None
        second.detach()


class TestCredit:
    def test_a_receiver_only_gets_as_many_messages_as_it_grants(self, session, queue_address):
        sender = _sender(session, queue_address)
        for index in range(5):
            sender.send_transfer(f"tag-{index}".encode(), Message(f"m{index}"), settled=True)
        sender.detach()

        receiver = _receiver(session, queue_address)
        receiver.flow(2)
        first = receiver.receive(timeout=10.0)
        second = receiver.receive(timeout=10.0)
        assert first is not None and second is not None
        receiver.settle(first.delivery_id, Accepted())
        receiver.settle(second.delivery_id, Accepted())
        assert receiver.receive(timeout=1.0) is None

        receiver.flow(3)
        third = receiver.receive(timeout=10.0)
        assert third is not None
        receiver.settle(third.delivery_id, Accepted())
        receiver.detach()


class TestDetach:
    def test_detaching_frees_the_handle_for_reuse(self, session, queue_address):
        first = _sender(session, queue_address)
        assert first.handle == 0
        first.detach()
        second = _sender(session, queue_address)
        assert second.handle == 0
        second.detach()

    def test_sending_after_detach_is_refused(self, session, queue_address):
        sender = _sender(session, queue_address)
        sender.detach()
        with pytest.raises(ProtocolError, match="not attached"):
            sender.send_transfer(b"tag", Message("x"))

    def test_ending_the_session_detaches_its_links(self, session, queue_address):
        sender = _sender(session, queue_address)
        session.end()
        assert not sender.is_attached


def _eventually(predicate, timeout=10.0):
    """Poll ``predicate`` until it holds or ``timeout`` elapses."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False
