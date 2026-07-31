"""Publisher behaviour: address resolution, attach shape, outcomes, rejection details.

Address resolution and the ``Error.Info`` → :class:`RejectionDetails` mapping are
pure functions and are tested directly. Everything else drives a real
``Connection`` against the in-process :class:`~tests.unit.fake_broker.FakeBroker`,
which grants link credit and is then told, per test, which ``disposition`` to
send back.
"""

from __future__ import annotations

import random
import threading

import pytest

from src import (
    AMQPTimeoutError,
    InvalidAddressError,
    OutcomeState,
    PublisherError,
    RejectionDetails,
)
from src.management import ExchangeSpecification, QueueSpecification
from src.publisher import (
    PublisherBuilder,
    exchange_address,
    outcome_from_delivery_state,
    queue_address,
    rejection_details_from_error,
)
from src.wire import (
    EXPIRY_POLICY_LINK_DETACH,
    EXPIRY_POLICY_SESSION_END,
    RCV_SETTLE_MODE_FIRST,
    SND_SETTLE_MODE_UNSETTLED,
    TERMINUS_DURABILITY_NONE,
    Accepted,
    Attach,
    Detach,
    Error,
    Message,
    Modified,
    Properties,
    Received,
    Rejected,
    Released,
    Symbol,
    Transfer,
)

#: Credit the fake broker grants a publisher's link, ample for every test here.
BROKER_CREDIT = 50

#: Bound on a publish a test expects to succeed.
PUBLISH_TIMEOUT = 5.0

#: How many threads the concurrency test publishes from.
CONCURRENT_PUBLISHERS = 20


@pytest.fixture
def publishing(connect):
    """Return ``make(**broker_kwargs) -> (broker, connection)`` with credit granted.

    Settlement is left to each test: the broker answers ``attach`` and grants
    credit, but sends no ``disposition`` of its own unless asked to.
    """

    def make(**broker_kwargs):
        kwargs = {"initial_credit": BROKER_CREDIT, "auto_settle": False}
        kwargs.update(broker_kwargs)
        return connect(broker_kwargs=kwargs)

    return make


def _builder():
    """A builder with no connection: :meth:`address` never touches one."""
    return PublisherBuilder(None)


def _publish_later(publisher, message, timeout=PUBLISH_TIMEOUT):
    """Run one ``publish`` on its own thread, so the test can settle it.

    Returns:
        ``(thread, results, failures)``; exactly one of the two lists ends up
        holding a single item once the thread finishes.
    """
    results = []
    failures = []

    def run():
        try:
            results.append(publisher.publish(message, timeout=timeout))
        except BaseException as error:  # noqa: BLE001 - reported through ``failures``
            failures.append(error)

    thread = threading.Thread(target=run, name="publish", daemon=True)
    thread.start()
    return thread, results, failures


def _settle_next(broker, state):
    """Wait for the next ``transfer`` and settle it with ``state``."""
    channel, transfer, payload = broker.wait_for(Transfer)
    broker.settle(channel, transfer.delivery_id, state)
    return transfer, payload


def _publish_with_state(publisher, broker, state, message=None):
    """Publish, settle the resulting transfer with ``state``, and return the result."""
    thread, results, failures = _publish_later(publisher, message if message is not None else Message("hello"))
    _settle_next(broker, state)
    thread.join(PUBLISH_TIMEOUT)
    if failures:
        raise failures[0]
    assert results, "publish did not complete"
    return results[0]


class TestAddressResolution:
    """step_020 §2.1: the six cases the reference ``AddressBuilder`` distinguishes."""

    def test_queue_only(self):
        assert _builder().queue("orders").address() == "/queues/orders"

    def test_exchange_only(self):
        assert _builder().exchange("events").address() == "/exchanges/events"

    def test_exchange_with_key(self):
        assert _builder().exchange("events").key("order.created").address() == "/exchanges/events/order.created"

    def test_anonymous_has_no_address(self):
        assert _builder().address() is None

    def test_queue_and_exchange_together_are_refused(self):
        with pytest.raises(InvalidAddressError, match="cannot be set together"):
            _builder().queue("orders").exchange("events").address()

    def test_a_key_without_an_exchange_is_refused(self):
        with pytest.raises(InvalidAddressError, match="must be set"):
            _builder().key("order.created").address()

    def test_a_key_alongside_a_queue_is_refused(self):
        with pytest.raises(InvalidAddressError, match="must be set"):
            _builder().queue("orders").key("order.created").address()

    @pytest.mark.parametrize(
        ("name", "address"),
        [
            ("my queue", "/queues/my%20queue"),
            ("a/b", "/queues/a%2Fb"),
            ("café", "/queues/caf%C3%A9"),
        ],
    )
    def test_queue_names_are_percent_encoded(self, name, address):
        assert _builder().queue(name).address() == address

    def test_exchange_and_key_are_encoded_independently(self):
        assert _builder().exchange("my ex").key("a/b").address() == "/exchanges/my%20ex/a%2Fb"

    def test_a_queue_specification_supplies_its_name(self):
        assert _builder().queue(QueueSpecification(None, "orders")).address() == "/queues/orders"

    def test_an_exchange_specification_supplies_its_name(self):
        assert _builder().exchange(ExchangeSpecification(None, "events")).address() == "/exchanges/events"

    @pytest.mark.parametrize("kind", ["queue", "exchange"])
    def test_an_empty_name_is_refused(self, kind):
        with pytest.raises(InvalidAddressError, match="non-empty"):
            getattr(_builder(), kind)("")

    def test_the_address_helpers_match_the_builder(self):
        assert queue_address("orders") == _builder().queue("orders").address()
        assert exchange_address("events") == _builder().exchange("events").address()
        assert exchange_address("events", "k") == _builder().exchange("events").key("k").address()


class TestOutcomeMapping:
    """step_020 §4: three modelled outcomes, everything else a protocol error."""

    def test_accepted(self):
        assert outcome_from_delivery_state(Accepted()).state is OutcomeState.ACCEPTED

    def test_released(self):
        assert outcome_from_delivery_state(Released()).state is OutcomeState.RELEASED

    def test_rejected_carries_the_raw_error(self):
        error = Error(condition="amqp:resource-limit-exceeded", description="full")
        outcome = outcome_from_delivery_state(Rejected(error=error))
        assert outcome.state is OutcomeState.REJECTED
        assert outcome.error is error

    def test_rejected_without_an_error(self):
        outcome = outcome_from_delivery_state(Rejected())
        assert outcome.state is OutcomeState.REJECTED
        assert outcome.error is None

    @pytest.mark.parametrize("state", [Modified(), Modified(delivery_failed=True), Received(0, 0)])
    def test_anything_else_is_a_publisher_error(self, state):
        with pytest.raises(PublisherError, match="does not model"):
            outcome_from_delivery_state(state)

    @pytest.mark.parametrize("state", [Accepted(), Released()])
    def test_non_rejected_outcomes_never_carry_rejection_details(self, state):
        assert outcome_from_delivery_state(state).rejection_details is None


class TestRejectionDetails:
    """step_070 §3: every population rule, applied per field and never raising."""

    def test_no_error_at_all(self):
        assert rejection_details_from_error(None) is None

    def test_error_without_info(self):
        assert rejection_details_from_error(Error(condition="amqp:internal-error")) is None

    def test_info_without_either_key(self):
        error = Error(condition="amqp:internal-error", info={"other": "value"})
        assert rejection_details_from_error(error) is None

    def test_empty_info(self):
        assert rejection_details_from_error(Error(condition="amqp:internal-error", info={})) is None

    def test_both_keys(self):
        error = Error(condition="amqp:internal-error", info={"reason": "max_length", "queue": "orders"})
        assert rejection_details_from_error(error) == RejectionDetails(reason="max_length", rejected_by_queue="orders")

    def test_only_the_reason(self):
        error = Error(condition="amqp:internal-error", info={"reason": "max_length"})
        assert rejection_details_from_error(error) == RejectionDetails(reason="max_length", rejected_by_queue=None)

    def test_only_the_queue(self):
        error = Error(condition="amqp:internal-error", info={"queue": "orders"})
        assert rejection_details_from_error(error) == RejectionDetails(reason=None, rejected_by_queue="orders")

    def test_a_non_string_value_degrades_only_its_own_field(self):
        error = Error(condition="amqp:internal-error", info={"reason": 42, "queue": "orders"})
        assert rejection_details_from_error(error) == RejectionDetails(reason=None, rejected_by_queue="orders")

    def test_non_string_values_on_both_keys_still_populate_the_object(self):
        error = Error(condition="amqp:internal-error", info={"reason": 42, "queue": b"orders"})
        assert rejection_details_from_error(error) == RejectionDetails()

    def test_symbol_keys_are_matched_like_strings(self):
        error = Error(condition="amqp:internal-error", info={Symbol("queue"): "orders"})
        assert rejection_details_from_error(error) == RejectionDetails(rejected_by_queue="orders")

    def test_reached_through_the_outcome_mapping(self):
        error = Error(condition="amqp:internal-error", info={"reason": "max_length", "queue": "orders"})
        outcome = outcome_from_delivery_state(Rejected(error=error))
        assert outcome.rejection_details == RejectionDetails(reason="max_length", rejected_by_queue="orders")


class TestBuild:
    """step_020 §2/§3.1: a fresh builder per call, one sender link per publisher."""

    def test_every_call_returns_a_new_builder(self, publishing):
        _broker, connection = publishing()
        assert connection.publisher_builder() is not connection.publisher_builder()

    def test_build_refuses_an_inconsistent_address_before_attaching(self, publishing):
        broker, connection = publishing()
        with pytest.raises(InvalidAddressError):
            connection.publisher_builder().queue("orders").exchange("events").build()
        assert broker.all_received(Attach) == []

    def test_sender_attach_fields(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        _channel, attach, _payload = broker.wait_for(Attach)
        assert attach.name == publisher.id
        assert attach.role is False
        assert attach.snd_settle_mode == SND_SETTLE_MODE_UNSETTLED
        assert attach.rcv_settle_mode == RCV_SETTLE_MODE_FIRST
        assert attach.initial_delivery_count == 0
        assert attach.source.address == "/queues/orders"
        assert attach.source.expiry_policy == EXPIRY_POLICY_LINK_DETACH
        assert attach.source.timeout == 0
        assert attach.source.dynamic is False
        assert attach.target.address == "/queues/orders"
        assert attach.target.expiry_policy == EXPIRY_POLICY_SESSION_END
        assert attach.target.durable == TERMINUS_DURABILITY_NONE
        assert attach.target.dynamic is False
        publisher.close()

    def test_an_anonymous_publisher_attaches_without_an_address(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().build()
        _channel, attach, _payload = broker.wait_for(Attach)
        assert attach.source.address is None
        assert attach.target.address is None
        assert publisher.is_anonymous
        assert publisher.address is None
        publisher.close()

    def test_a_refused_attach_raises_a_publisher_error(self, publishing):
        _broker, connection = publishing(refuse_attach=True)
        with pytest.raises(PublisherError, match="refused"):
            connection.publisher_builder().queue("missing").build()
        assert connection._publishers == {}

    def test_link_names_are_unique_per_publisher(self, publishing):
        _broker, connection = publishing()
        first = connection.publisher_builder().queue("a").build()
        second = connection.publisher_builder().queue("b").build()
        assert first.id != second.id
        first.close()
        second.close()


class TestSharedPubSubSession:
    """step_020 §1/§6: one lazily-opened session hosts every publisher."""

    def test_the_session_is_opened_once_and_reused(self, publishing):
        _broker, connection = publishing()
        first = connection.publisher_builder().queue("a").build()
        second = connection.publisher_builder().queue("b").build()
        assert connection._pub_sub_session() is connection._shared_session
        assert first._session is second._session
        assert list(connection._sessions) == [0]
        first.close()
        second.close()

    def test_nothing_is_opened_before_the_first_build(self, publishing):
        _broker, connection = publishing()
        connection.publisher_builder().queue("orders")
        assert connection._shared_session is None
        assert connection._sessions == {}

    def test_the_management_session_stays_separate(self, publishing):
        _broker, connection = publishing()
        management = connection.management()
        publisher = connection.publisher_builder().queue("orders").build()
        assert publisher._session is not management._session
        assert sorted(connection._sessions) == [0, 1]
        publisher.close()

    def test_closing_a_publisher_leaves_the_session_open(self, publishing):
        _broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        session = publisher._session
        publisher.close()
        assert session.is_open
        assert connection._shared_session is session


class TestPublish:
    """step_020 §3.2: an unsettled transfer, resolved by the broker's disposition."""

    def test_an_accepted_publish(self, publishing):
        broker, connection = publishing(auto_settle=True)
        publisher = connection.publisher_builder().queue("orders").build()
        message = Message("hello")
        result = publisher.publish(message, timeout=PUBLISH_TIMEOUT)
        assert result.message is message
        assert result.outcome.state is OutcomeState.ACCEPTED
        assert result.outcome.rejection_details is None
        publisher.close()

    def test_the_transfer_is_unsettled_and_carries_a_fresh_tag(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        message = Message("hello")
        thread, results, failures = _publish_later(publisher, message)
        transfer, payload = _settle_next(broker, Accepted())
        thread.join(PUBLISH_TIMEOUT)
        assert not failures and results
        assert transfer.settled is False
        assert transfer.delivery_tag
        assert payload == message.encode()
        publisher.close()

    def test_delivery_tags_differ_between_publishes(self, publishing):
        broker, connection = publishing(auto_settle=True)
        publisher = connection.publisher_builder().queue("orders").build()
        publisher.publish(Message("one"), timeout=PUBLISH_TIMEOUT)
        publisher.publish(Message("two"), timeout=PUBLISH_TIMEOUT)
        tags = {transfer.delivery_tag for transfer in broker.all_received(Transfer)}
        assert len(tags) == 2
        publisher.close()

    def test_a_released_publish(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().exchange("events").build()
        result = _publish_with_state(publisher, broker, Released())
        assert result.outcome.state is OutcomeState.RELEASED
        assert result.outcome.rejection_details is None
        publisher.close()

    def test_a_rejected_publish_surfaces_its_details(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        error = Error(
            condition="amqp:resource-limit-exceeded",
            description="rejected",
            info={"reason": "max_length", "queue": "orders"},
        )
        result = _publish_with_state(publisher, broker, Rejected(error=error))
        assert result.outcome.state is OutcomeState.REJECTED
        assert result.outcome.error.condition == "amqp:resource-limit-exceeded"
        assert result.outcome.rejection_details == RejectionDetails(reason="max_length", rejected_by_queue="orders")
        publisher.close()

    def test_a_rejection_without_metadata_leaves_the_details_unset(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        result = _publish_with_state(publisher, broker, Rejected(error=Error(condition="amqp:internal-error")))
        assert result.outcome.state is OutcomeState.REJECTED
        assert result.outcome.rejection_details is None
        publisher.close()

    def test_an_unmodelled_outcome_raises(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        thread, _results, failures = _publish_later(publisher, Message("hello"))
        _settle_next(broker, Modified(delivery_failed=True))
        thread.join(PUBLISH_TIMEOUT)
        assert failures and isinstance(failures[0], PublisherError)
        publisher.close()

    def test_no_disposition_times_out_and_drops_the_waiter(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        with pytest.raises(AMQPTimeoutError):
            publisher.publish(Message("hello"), timeout=0.2)
        broker.wait_for(Transfer)
        assert publisher._link._pending_by_tag == {}
        assert publisher._link._pending_by_id == {}
        publisher.close()

    def test_no_credit_times_out(self, publishing):
        _broker, connection = publishing(initial_credit=0)
        publisher = connection.publisher_builder().queue("orders").build()
        with pytest.raises(AMQPTimeoutError, match="credit"):
            publisher.publish(Message("hello"), timeout=0.2)
        assert publisher._link._pending_by_tag == {}
        publisher.close()


class TestAnonymousPublisher:
    """step_020 §3.3: the destination comes from each message's ``to``."""

    def test_a_message_without_a_destination_is_refused_before_sending(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().build()
        with pytest.raises(PublisherError, match="properties.to"):
            publisher.publish(Message("hello"))
        with pytest.raises(PublisherError, match="properties.to"):
            publisher.publish(Message("hello", properties=Properties(subject="x")))
        with pytest.raises(PublisherError, match="properties.to"):
            publisher.publish(Message("hello", properties=Properties(to="")))
        assert broker.all_received(Transfer) == []
        publisher.close()

    def test_each_message_carries_its_own_destination(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().build()
        addresses = [queue_address("first"), exchange_address("events", "order.created")]
        sent = []
        for address in addresses:
            message = Message("hello", properties=Properties(to=address))
            thread, results, failures = _publish_later(publisher, message)
            _transfer, payload = _settle_next(broker, Accepted())
            thread.join(PUBLISH_TIMEOUT)
            assert not failures and results
            assert results[0].outcome.state is OutcomeState.ACCEPTED
            sent.append(Message.decode(payload))
        assert [message.properties.to for message in sent] == addresses
        publisher.close()

    def test_a_bound_publisher_does_not_require_a_destination(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        result = _publish_with_state(publisher, broker, Accepted(), message=Message("hello"))
        assert result.outcome.state is OutcomeState.ACCEPTED
        publisher.close()


class TestConcurrentPublishing:
    """step_020 §3.2: correlation is per delivery, not per publisher."""

    def test_many_threads_publish_on_one_publisher(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        results: dict[int, object] = {}
        failures: list[BaseException] = []
        guard = threading.Lock()

        def publish(index):
            try:
                result = publisher.publish(Message(f"m-{index}"), timeout=10.0)
            except BaseException as error:  # noqa: BLE001 - reported through ``failures``
                with guard:
                    failures.append(error)
                return
            with guard:
                results[index] = result.outcome

        threads = [
            threading.Thread(target=publish, args=(index,), daemon=True) for index in range(CONCURRENT_PUBLISHERS)
        ]
        for thread in threads:
            thread.start()

        # Settle out of order, so a result can only be right if it was matched by
        # delivery-id rather than by arrival order.
        pending = []
        for _ in range(CONCURRENT_PUBLISHERS):
            channel, transfer, payload = broker.wait_for(Transfer)
            index = int(Message.decode(payload).body_as_string().removeprefix("m-"))
            pending.append((channel, transfer.delivery_id, index))
        random.shuffle(pending)
        for channel, delivery_id, index in pending:
            state = Accepted() if index % 2 == 0 else Rejected(error=Error(condition="x", info={"queue": f"q-{index}"}))
            broker.settle(channel, delivery_id, state)

        for thread in threads:
            thread.join(15.0)
        assert not failures
        assert len(results) == CONCURRENT_PUBLISHERS
        for index, outcome in results.items():
            if index % 2 == 0:
                assert outcome.state is OutcomeState.ACCEPTED
                assert outcome.rejection_details is None
            else:
                assert outcome.state is OutcomeState.REJECTED
                assert outcome.rejection_details == RejectionDetails(rejected_by_queue=f"q-{index}")
        publisher.close()


class TestClose:
    """step_020 §3.4/§6: idempotent, unregisters, leaves the session alone."""

    def test_close_detaches_the_link_and_unregisters(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        assert connection._publishers == {publisher.id: publisher}
        publisher.close()
        _channel, detach, _payload = broker.wait_for(Detach)
        assert detach.closed is True
        assert not publisher.is_open
        assert connection._publishers == {}

    def test_close_is_idempotent(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        publisher.close()
        publisher.close()
        assert len(broker.all_received(Detach)) == 1

    def test_publishing_after_close_is_refused(self, publishing):
        broker, connection = publishing()
        publisher = connection.publisher_builder().queue("orders").build()
        publisher.close()
        broker.all_received(Transfer)
        with pytest.raises(PublisherError, match="closed"):
            publisher.publish(Message("hello"))
        assert broker.all_received(Transfer) == []

    def test_connection_close_closes_every_publisher(self, publishing):
        broker, connection = publishing()
        publishers = [connection.publisher_builder().queue(f"q-{index}").build() for index in range(3)]
        connection.close()
        assert all(not publisher.is_open for publisher in publishers)
        assert connection._publishers == {}
        assert connection._shared_session is None
        assert len(broker.all_received(Detach)) == 3

    def test_connection_close_survives_a_publisher_that_cannot_detach(self, publishing):
        broker, connection = publishing()
        connection.publisher_builder().queue("orders").build()
        broker.drop_connection()
        connection.close()
        assert connection._publishers == {}
