"""Consumer behaviour: builder validation, attach shape, settlement, credit, lifecycle.

Every test drives a real ``Connection`` against the in-process
:class:`~tests.unit.fake_broker.FakeBroker`, which answers ``attach`` and is then
told, per test, which ``transfer`` frames to deliver. That keeps the delivery
loop, the disposition encoding and the ``flow`` accounting on the real code path;
only the broker's own decisions are scripted.
"""

from __future__ import annotations

import queue as queue_module
import threading
import time
from datetime import datetime, timezone

import pytest

from src import (
    ConsumerError,
    ConsumerSettleStrategy,
    InvalidAddressError,
    ProtocolError,
    StreamFilterOptions,
    StreamOffsetSpecification,
    StreamOptions,
    ValidationError,
)
from src.constants import (
    AMQP_APPLICATION_PROPERTIES_FILTER,
    AMQP_PROPERTIES_FILTER,
    AMQP_SQL_FILTER,
    DIRECT_REPLY_TO_CAPABILITY,
    RABBITMQ_ACTIVE_PROPERTY,
    SQL_FILTER_NAME,
    STREAM_FILTER_VALUES_FILTER,
    STREAM_MATCH_UNFILTERED_FILTER,
    STREAM_OFFSET_ANNOTATION,
    STREAM_OFFSET_SPEC_FILTER,
)
from src.consumer import (
    DEFAULT_INITIAL_CREDITS,
    ConsumerBuilder,
    QuorumConsumerOptions,
    parse_active_flag,
)
from src.link import ReceiverLink
from src.management import QueueSpecification
from src.wire import (
    EXPIRY_POLICY_LINK_DETACH,
    RCV_SETTLE_MODE_FIRST,
    SND_SETTLE_MODE_SETTLED,
    SND_SETTLE_MODE_UNSETTLED,
    Accepted,
    Attach,
    Described,
    Detach,
    Disposition,
    Flow,
    Message,
    MessageAnnotations,
    Modified,
    Rejected,
    Released,
    Symbol,
    Timestamp,
)
from src.wire.encoding import (
    Byte,
    Int,
    Long,
    Short,
    Ubyte,
    Uint,
    Ulong,
    Ushort,
)

#: Bound on anything a test expects to happen promptly.
HANDLER_TIMEOUT = 5.0

#: How long a test waits to conclude that no frame is coming.
QUIET_PERIOD = 0.3

#: Credit most tests grant, small enough to read in an assertion.
CREDITS = 5


class RecordingHandler:
    """A message handler that records every call and can act on each delivery.

    Attributes:
        action: Optional ``action(context, message)`` run after recording, so a
            test can settle, block or raise from inside the handler.
    """

    def __init__(self, action=None):
        self.action = action
        self._lock = threading.Lock()
        self._calls = []

    def __call__(self, context, message):
        with self._lock:
            self._calls.append((context, message))
        if self.action is not None:
            self.action(context, message)

    @property
    def call_count(self):
        """How many times the handler has been invoked."""
        with self._lock:
            return len(self._calls)

    @property
    def bodies(self):
        """The string body of every message the handler saw, in arrival order."""
        with self._lock:
            return [message.body_as_string() for _context, message in self._calls]

    @property
    def contexts(self):
        """The context of every delivery the handler saw, in arrival order."""
        with self._lock:
            return [context for context, _message in self._calls]

    def wait(self, count=1, timeout=HANDLER_TIMEOUT):
        """Block until the handler has been called ``count`` times.

        Raises:
            AssertionError: If it has not been, within ``timeout``.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.call_count >= count:
                return
            time.sleep(0.02)
        raise AssertionError(f"handler was called {self.call_count} times, expected {count}")


class Harness:
    """One fake broker plus the bookkeeping a test needs to feed a consumer."""

    def __init__(self, broker, connection):
        self.broker = broker
        self.connection = connection
        self.attach = None
        self.channel = 0
        self.handle = 0
        self._next_delivery_id = 0

    def build(
        self,
        handler,
        *,
        queue="orders",
        credits=None,
        settle_strategy=ConsumerSettleStrategy.EXPLICIT_SETTLE,
        on_state_changed=None,
        stream=None,
    ):
        """Build one consumer and remember the channel/handle its link got.

        Args:
            handler: The message handler to register.
            queue: Queue to consume from. Ignored (never set on the builder)
                when ``settle_strategy`` is ``DIRECT_REPLY_TO``, whose address is
                broker-generated (step_060_consumer_strategy.md §3.3).
            credits: Initial credits, when not the default.
            settle_strategy: How deliveries on the built consumer are settled.
            on_state_changed: Single-active-consumer handler to register.
            stream: Called as ``stream(stream_options)`` to configure the stream
                sub-builder; whatever it returns is ignored, since every view
                writes to the same builder.
        """
        builder = self.connection.consumer_builder()
        if settle_strategy is not ConsumerSettleStrategy.DIRECT_REPLY_TO:
            builder = builder.queue(queue)
        builder = builder.message_handler(handler)
        if credits is not None:
            builder.initial_credits(credits)
        if settle_strategy is not ConsumerSettleStrategy.EXPLICIT_SETTLE:
            builder.settle_strategy(settle_strategy)
        if on_state_changed is not None:
            builder.quorum().single_active_consumer_state_changed(on_state_changed).builder()
        if stream is not None:
            stream(builder.stream())
        consumer = builder.build()
        self.channel, self.attach, _payload = self.broker.wait_for(Attach)
        self.handle = self.attach.handle
        return consumer

    def build_stream(self, handler, stream, **options):
        """Build one stream consumer and return its ``source.filter`` map."""
        consumer = self.build(handler, stream=stream, **options)
        return consumer, filter_set_of(self.attach)

    def deliver(self, body="hello", *, settled=False, annotations=None):
        """Send one ``transfer`` carrying ``body`` and return its delivery-id."""
        delivery_id = self._next_delivery_id
        self._next_delivery_id += 1
        message = Message(body, message_annotations=None if annotations is None else MessageAnnotations(annotations))
        self.broker.send_transfer(
            self.channel,
            self.handle,
            message.encode(),
            delivery_id=delivery_id,
            delivery_tag=f"tag-{delivery_id}".encode(),
            settled=settled,
        )
        return delivery_id

    def next_flow(self, timeout=HANDLER_TIMEOUT):
        """Return the next ``flow`` the client sends."""
        _channel, flow, _payload = self.broker.wait_for(Flow, timeout=timeout)
        return flow

    def next_disposition(self, timeout=HANDLER_TIMEOUT):
        """Return the next ``disposition`` the client sends."""
        _channel, disposition, _payload = self.broker.wait_for(Disposition, timeout=timeout)
        return disposition

    def expect_no_flow(self, within=QUIET_PERIOD):
        """Assert the client sends no ``flow`` for ``within`` seconds."""
        with pytest.raises(AssertionError):
            self.broker.wait_for(Flow, timeout=within)

    def expect_no_disposition(self, within=QUIET_PERIOD):
        """Assert the client sends no ``disposition`` for ``within`` seconds."""
        with pytest.raises(AssertionError):
            self.broker.wait_for(Disposition, timeout=within)


@pytest.fixture
def harness(connect):
    """Return ``make(**broker_kwargs) -> Harness`` over a connected fake broker."""

    def make(**broker_kwargs):
        broker, connection = connect(broker_kwargs=broker_kwargs)
        return Harness(broker, connection)

    return make


@pytest.fixture
def consuming(harness):
    """A harness whose broker accepts every attach — what most tests want."""
    return harness()


def annotations_of(state):
    """Return a ``modified`` outcome's annotations with plain string keys."""
    return {str(key): value for key, value in (state.message_annotations or {}).items()}


def filter_set_of(attach):
    """Return an ``attach``'s decoded ``source.filter`` map with plain string keys."""
    if attach.source.filter is None:
        return None
    return {str(key): value for key, value in attach.source.filter.items()}


class TestBuilderValidation:
    """step_030 §2/§3.1: mandatory settings are checked before the network is touched."""

    def test_every_call_returns_a_new_builder(self, consuming):
        assert consuming.connection.consumer_builder() is not consuming.connection.consumer_builder()

    def test_a_missing_handler_is_refused_before_attaching(self, consuming):
        with pytest.raises(ConsumerError, match="message handler"):
            consuming.connection.consumer_builder().queue("orders").build()
        assert consuming.broker.all_received(Attach) == []
        assert consuming.connection._shared_session is None

    def test_a_missing_queue_is_refused_before_attaching(self, consuming):
        with pytest.raises(ConsumerError, match="queue"):
            consuming.connection.consumer_builder().message_handler(RecordingHandler()).build()
        assert consuming.broker.all_received(Attach) == []
        assert consuming.connection._shared_session is None

    def test_an_empty_queue_name_is_refused(self, consuming):
        with pytest.raises(InvalidAddressError, match="non-empty"):
            consuming.connection.consumer_builder().queue("")

    @pytest.mark.parametrize("credits", [0, -1])
    def test_non_positive_initial_credits_are_refused(self, consuming, credits):
        with pytest.raises(ValidationError, match="must be > 0"):
            consuming.connection.consumer_builder().initial_credits(credits)

    def test_a_queue_specification_supplies_its_name(self):
        builder = ConsumerBuilder(None).queue(QueueSpecification(None, "orders"))
        assert builder._queue == "orders"

    def test_the_setters_are_chainable(self):
        builder = ConsumerBuilder(None)
        assert builder.queue("orders") is builder
        assert builder.message_handler(RecordingHandler()) is builder
        assert builder.initial_credits(7) is builder
        assert builder.settle_strategy(ConsumerSettleStrategy.PRESETTLED) is builder


class TestAttach:
    """step_030 §3.1 / step_060_consumer_strategy.md §1/§3.2/§3.3: one receiver link per consumer, credited at attach."""

    def test_receiver_attach_fields(self, consuming):
        consumer = consuming.build(RecordingHandler(), queue="orders")
        attach = consuming.attach
        assert attach.name == consumer.id
        assert attach.role is True
        assert attach.snd_settle_mode == SND_SETTLE_MODE_UNSETTLED
        assert attach.rcv_settle_mode == RCV_SETTLE_MODE_FIRST
        assert attach.target is None
        assert attach.source.address == "/queues/orders"
        assert attach.source.expiry_policy == EXPIRY_POLICY_LINK_DETACH
        assert attach.source.timeout == 0
        assert attach.source.dynamic is False
        assert consumer.queue == "orders"
        assert consumer.address == "/queues/orders"
        assert consumer.is_open
        consumer.close()

    def test_a_presettled_consumer_asks_the_broker_to_settle(self, consuming):
        consumer = consuming.build(RecordingHandler(), settle_strategy=ConsumerSettleStrategy.PRESETTLED)
        assert consuming.attach.snd_settle_mode == SND_SETTLE_MODE_SETTLED
        assert consumer.is_presettled
        consumer.close()

    def test_a_direct_reply_to_consumer_attaches_dynamic_and_settled(self, consuming):
        consumer = consuming.build(RecordingHandler(), settle_strategy=ConsumerSettleStrategy.DIRECT_REPLY_TO)
        attach = consuming.attach
        assert attach.snd_settle_mode == SND_SETTLE_MODE_SETTLED
        assert attach.source.address is None
        assert attach.source.dynamic is True
        assert attach.source.capabilities == [DIRECT_REPLY_TO_CAPABILITY]
        assert consumer.is_presettled
        consumer.close()

    def test_a_direct_reply_to_consumer_reads_back_the_broker_generated_address(self, consuming):
        consumer = consuming.build(RecordingHandler(), settle_strategy=ConsumerSettleStrategy.DIRECT_REPLY_TO)
        assert consumer.queue is not None
        assert consumer.queue.startswith("/queues/amq.rabbitmq.reply-to.")
        assert consumer.address == consumer.queue
        consumer.close()

    def test_direct_reply_to_cannot_be_combined_with_a_queue(self, consuming):
        builder = (
            consuming.connection.consumer_builder()
            .queue("orders")
            .message_handler(RecordingHandler())
            .settle_strategy(ConsumerSettleStrategy.DIRECT_REPLY_TO)
        )
        with pytest.raises(ConsumerError, match="queue"):
            builder.build()
        assert consuming.broker.all_received(Attach) == []

    def test_direct_reply_to_cannot_be_combined_with_single_active_consumer_state_changed(self, consuming):
        builder = consuming.connection.consumer_builder().queue("orders")
        builder.quorum().single_active_consumer_state_changed(lambda consumer, is_active: None).builder()
        # quorum() requires a queue to already be set (step_090 §2), but
        # DIRECT_REPLY_TO forbids one (§5) — clearing it here isolates the
        # single-active-consumer-only conflict from the queue conflict above,
        # which build() would otherwise report first.
        builder._queue = None
        builder.message_handler(RecordingHandler()).settle_strategy(ConsumerSettleStrategy.DIRECT_REPLY_TO)
        with pytest.raises(ConsumerError, match="single_active_consumer_state_changed"):
            builder.build()
        assert consuming.broker.all_received(Attach) == []

    def test_the_queue_name_is_percent_encoded(self, consuming):
        consumer = consuming.build(RecordingHandler(), queue="my orders")
        assert consuming.attach.source.address == "/queues/my%20orders"
        assert consumer.queue == "my orders"
        consumer.close()

    def test_the_initial_credit_is_granted_right_after_attach(self, consuming):
        consumer = consuming.build(RecordingHandler(), credits=CREDITS)
        flow = consuming.next_flow()
        assert flow.handle == consuming.handle
        assert flow.link_credit == CREDITS
        assert flow.delivery_count == 0
        assert consumer.initial_credits == CREDITS
        consumer.close()

    def test_the_default_credit_is_a_hundred(self, consuming):
        consumer = consuming.build(RecordingHandler())
        assert consuming.next_flow().link_credit == DEFAULT_INITIAL_CREDITS
        assert consumer.initial_credits == DEFAULT_INITIAL_CREDITS
        consumer.close()

    def test_a_refused_attach_raises_a_consumer_error(self, harness):
        refusing = harness(refuse_attach=True)
        with pytest.raises(ConsumerError, match="refused"):
            refusing.build(RecordingHandler(), queue="missing")
        assert refusing.connection._consumers == {}

    def test_link_names_are_unique_per_consumer(self, consuming):
        first = consuming.build(RecordingHandler(), queue="a")
        second = consuming.build(RecordingHandler(), queue="b")
        assert first.id != second.id
        assert consuming.connection._consumers == {first.id: first, second.id: second}
        first.close()
        second.close()

    def test_consumers_share_the_pub_sub_session_with_publishers(self, consuming):
        publisher = consuming.connection.publisher_builder().queue("orders").build()
        consumer = consuming.build(RecordingHandler())
        assert consumer._session is publisher._session
        assert list(consuming.connection._sessions) == [0]
        consumer.close()
        publisher.close()

    def test_closing_a_consumer_leaves_the_session_open(self, consuming):
        consumer = consuming.build(RecordingHandler())
        session = consumer._session
        consumer.close()
        assert session.is_open
        assert consuming.connection._shared_session is session


class TestDelivery:
    """step_030 §3.2: a background loop pushes each delivery to the handler."""

    def test_a_delivery_reaches_the_handler(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        delivery_id = consuming.deliver("hello")
        handler.wait()
        assert handler.bodies == ["hello"]
        assert handler.contexts[0].delivery_id == delivery_id
        assert handler.contexts[0].is_settled is False
        consumer.close()

    def test_several_deliveries_arrive_in_order(self, consuming):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(handler, credits=CREDITS)
        for index in range(3):
            consuming.deliver(f"m-{index}")
        handler.wait(3)
        assert handler.bodies == ["m-0", "m-1", "m-2"]
        assert consumer.unsettled_message_count == 0
        consumer.close()

    def test_the_delivery_loop_runs_off_the_frame_reader_thread(self, consuming):
        threads = []
        handler = RecordingHandler(action=lambda context, message: threads.append(threading.current_thread().name))
        consumer = consuming.build(handler)
        consuming.deliver()
        handler.wait()
        assert threads == [f"amqp-{consumer.id}"]
        consumer.close()

    def test_a_handler_exception_does_not_stop_the_loop(self, consuming, caplog):
        def explode(context, message):
            if message.body_as_string() == "bad":
                raise RuntimeError("handler is broken")
            context.accept()

        handler = RecordingHandler(action=explode)
        consumer = consuming.build(handler, credits=CREDITS)
        with caplog.at_level("ERROR", logger="src.consumer"):
            consuming.deliver("bad")
            handler.wait(1)
            consuming.deliver("good")
            handler.wait(2)
        assert handler.bodies == ["bad", "good"]
        assert "handler is broken" in caplog.text
        assert consuming.next_disposition().state == Accepted()
        consumer.close()

    def test_an_unsettled_delivery_is_counted(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver()
        handler.wait()
        assert consumer.unsettled_message_count == 1
        handler.contexts[0].accept()
        assert consumer.unsettled_message_count == 0
        consumer.close()


class TestSettlement:
    """step_030 §4: each outcome, and the one-settlement-per-delivery rule."""

    def _settle_one(self, harness, settle):
        """Deliver one message, run ``settle(context)``, and return the disposition."""
        handler = RecordingHandler()
        consumer = harness.build(handler, credits=CREDITS)
        delivery_id = harness.deliver()
        handler.wait()
        settle(handler.contexts[0])
        disposition = harness.next_disposition()
        assert disposition.role is True
        assert disposition.first == delivery_id
        assert disposition.last == delivery_id
        assert disposition.settled is True
        consumer.close()
        return disposition

    def test_accept_sends_accepted(self, consuming):
        disposition = self._settle_one(consuming, lambda context: context.accept())
        assert disposition.state == Accepted()

    def test_discard_sends_rejected(self, consuming):
        disposition = self._settle_one(consuming, lambda context: context.discard())
        assert disposition.state == Rejected()

    def test_discard_with_annotations_sends_modified_undeliverable_here(self, consuming):
        disposition = self._settle_one(consuming, lambda context: context.discard({"x-reason": "unparseable"}))
        state = disposition.state
        assert isinstance(state, Modified)
        assert state.delivery_failed is True
        assert state.undeliverable_here is True
        assert annotations_of(state) == {"x-reason": "unparseable"}

    def test_requeue_sends_released(self, consuming):
        disposition = self._settle_one(consuming, lambda context: context.requeue())
        assert disposition.state == Released()

    def test_requeue_with_annotations_sends_modified_deliverable_here(self, consuming):
        disposition = self._settle_one(consuming, lambda context: context.requeue({"x-retry": 1}))
        state = disposition.state
        assert isinstance(state, Modified)
        assert state.delivery_failed is False
        assert state.undeliverable_here is False
        assert annotations_of(state) == {"x-retry": 1}

    def test_requeue_can_count_the_attempt_as_failed(self, consuming):
        disposition = self._settle_one(consuming, lambda context: context.requeue(delivery_failed=True))
        state = disposition.state
        assert isinstance(state, Modified)
        assert state.delivery_failed is True
        assert state.undeliverable_here is False
        assert state.message_annotations is None

    @pytest.mark.parametrize("annotations", [{"reason": "x"}, {"x-ok": 1, "bad": 2}, {"X-Reason": "x"}])
    @pytest.mark.parametrize("method", ["discard", "requeue"])
    def test_annotation_keys_must_start_with_x(self, consuming, method, annotations):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver()
        handler.wait()
        context = handler.contexts[0]
        with pytest.raises(ValidationError, match="must start with"):
            getattr(context, method)(annotations)
        consuming.expect_no_disposition()
        assert context.is_settled is False
        assert consumer.unsettled_message_count == 1

        # Nothing was sent, so the delivery is still settleable.
        context.accept()
        assert consuming.next_disposition().state == Accepted()
        consumer.close()

    def test_settling_twice_is_refused(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver()
        handler.wait()
        context = handler.contexts[0]
        context.accept()
        assert context.is_settled is True
        with pytest.raises(ConsumerError, match="already been settled"):
            context.accept()
        consumer.close()

    @pytest.mark.parametrize("first", ["accept", "discard", "requeue"])
    @pytest.mark.parametrize("second", ["accept", "discard", "requeue"])
    def test_a_different_outcome_after_settling_is_refused(self, consuming, first, second):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver()
        handler.wait()
        context = handler.contexts[0]
        getattr(context, first)()
        with pytest.raises(ConsumerError, match="already been settled"):
            getattr(context, second)()
        assert consumer.unsettled_message_count == 0
        consumer.close()

    def test_settling_after_close_is_refused(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver()
        handler.wait()
        context = handler.contexts[0]
        consumer.close()
        with pytest.raises(ConsumerError, match="closed"):
            context.accept()


class TestPresettled:
    """step_060_consumer_strategy.md §3.2/§3.3/§5: nothing is ever settled by this client."""

    def test_every_context_method_is_refused(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS, settle_strategy=ConsumerSettleStrategy.PRESETTLED)
        consuming.deliver(settled=True)
        handler.wait()
        context = handler.contexts[0]
        assert context.is_presettled is True
        for settle in (context.accept, context.discard, context.requeue):
            with pytest.raises(ConsumerError, match="presettled"):
                settle()
        with pytest.raises(ConsumerError, match="presettled"):
            context.discard({"x-reason": "unparseable"})
        with pytest.raises(ConsumerError, match="presettled"):
            context.requeue({"x-retry": 1})
        consuming.expect_no_disposition()
        consumer.close()

    def test_every_context_method_is_refused_under_direct_reply_to(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS, settle_strategy=ConsumerSettleStrategy.DIRECT_REPLY_TO)
        consuming.deliver(settled=True)
        handler.wait()
        context = handler.contexts[0]
        assert context.is_presettled is True
        for settle in (context.accept, context.discard, context.requeue):
            with pytest.raises(ConsumerError, match="presettled"):
                settle()
        consuming.expect_no_disposition()
        consumer.close()

    def test_nothing_is_ever_counted_as_unsettled(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS, settle_strategy=ConsumerSettleStrategy.PRESETTLED)
        for index in range(3):
            consuming.deliver(f"m-{index}", settled=True)
        handler.wait(3)
        assert consumer.unsettled_message_count == 0
        consuming.expect_no_disposition()
        consumer.close()


class TestCreditReplenishment:
    """step_030 §3.3: credit follows settlement, or handoff when presettled."""

    def test_credit_is_replenished_only_once_the_delivery_is_settled(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        assert consuming.next_flow().link_credit == CREDITS
        consuming.deliver()
        handler.wait()
        consuming.expect_no_flow()

        handler.contexts[0].accept()
        flow = consuming.next_flow()
        assert flow.link_credit == CREDITS
        # The grant is against a delivery-count that advanced by one, which is
        # what makes it a +1 rather than a re-grant of the same credit.
        assert flow.delivery_count == 1
        consumer.close()

    def test_every_settlement_grants_exactly_one_more(self, consuming):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(handler, credits=CREDITS)
        assert consuming.next_flow().delivery_count == 0
        for index in range(3):
            consuming.deliver(f"m-{index}")
            handler.wait(index + 1)
            flow = consuming.next_flow()
            assert flow.link_credit == CREDITS
            assert flow.delivery_count == index + 1
        consumer.close()

    def test_a_presettled_consumer_replenishes_before_the_handler_returns(self, consuming):
        blocked = threading.Event()
        handler = RecordingHandler(action=lambda context, message: blocked.wait(HANDLER_TIMEOUT))
        consumer = consuming.build(handler, credits=CREDITS, settle_strategy=ConsumerSettleStrategy.PRESETTLED)
        assert consuming.next_flow().link_credit == CREDITS
        consuming.deliver(settled=True)
        handler.wait()

        flow = consuming.next_flow()
        assert flow.link_credit == CREDITS
        assert flow.delivery_count == 1
        assert handler.call_count == 1  # still inside the handler
        blocked.set()
        consumer.close()


class TestPause:
    """step_030 §3.4: credit is held at zero while paused, restored on unpause."""

    def test_pause_zeroes_the_credit(self, consuming):
        consumer = consuming.build(RecordingHandler(), credits=CREDITS)
        assert consuming.next_flow().link_credit == CREDITS
        consumer.pause()
        assert consumer.is_paused
        assert consuming.next_flow().link_credit == 0
        consumer.close()

    def test_pause_is_idempotent(self, consuming):
        consumer = consuming.build(RecordingHandler(), credits=CREDITS)
        consuming.next_flow()
        consumer.pause()
        consuming.next_flow()
        consumer.pause()
        consuming.expect_no_flow()
        assert consumer.is_paused
        consumer.close()

    def test_unpause_restores_the_credit(self, consuming):
        consumer = consuming.build(RecordingHandler(), credits=CREDITS)
        consuming.next_flow()
        consumer.pause()
        consuming.next_flow()
        consumer.unpause()
        assert not consumer.is_paused
        assert consuming.next_flow().link_credit == CREDITS
        consumer.close()

    def test_unpause_on_a_running_consumer_is_idempotent(self, consuming):
        consumer = consuming.build(RecordingHandler(), credits=CREDITS)
        consuming.next_flow()
        consumer.unpause()
        consuming.expect_no_flow()
        assert not consumer.is_paused
        consumer.close()

    def test_settling_while_paused_grants_nothing(self, consuming):
        handler = RecordingHandler()
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.next_flow()
        consuming.deliver()
        handler.wait()
        consumer.pause()
        assert consuming.next_flow().link_credit == 0

        handler.contexts[0].accept()
        consuming.expect_no_flow()
        assert consumer.unsettled_message_count == 0

        consumer.unpause()
        assert consuming.next_flow().link_credit == CREDITS
        consumer.close()

    def test_an_in_flight_delivery_still_reaches_the_handler_while_paused(self, consuming):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(handler, credits=CREDITS)
        consumer.pause()
        consuming.deliver("already on the wire")
        handler.wait()
        assert handler.bodies == ["already on the wire"]
        consumer.close()

    @pytest.mark.parametrize("method", ["pause", "unpause"])
    def test_pausing_a_closed_consumer_is_refused(self, consuming, method):
        consumer = consuming.build(RecordingHandler())
        consumer.close()
        with pytest.raises(ConsumerError, match="closed"):
            getattr(consumer, method)()


class TestClose:
    """step_030 §3.5/§6: idempotent, unregisters, stops dispatching."""

    def test_close_detaches_the_link_and_unregisters(self, consuming):
        consumer = consuming.build(RecordingHandler())
        assert consuming.connection._consumers == {consumer.id: consumer}
        consumer.close()
        _channel, detach, _payload = consuming.broker.wait_for(Detach)
        assert detach.closed is True
        assert not consumer.is_open
        assert consuming.connection._consumers == {}

    def test_close_stops_the_delivery_loop(self, consuming):
        consumer = consuming.build(RecordingHandler())
        loop = consumer._delivery_loop
        consumer.close()
        assert not loop.is_alive()

    def test_close_is_idempotent(self, consuming):
        consumer = consuming.build(RecordingHandler())
        consumer.close()
        consumer.close()
        assert len(consuming.broker.all_received(Detach)) == 1

    def test_a_delivery_arriving_during_teardown_is_not_dispatched(self, consuming):
        blocked = threading.Event()
        handler = RecordingHandler(action=lambda context, message: blocked.wait(HANDLER_TIMEOUT))
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver("first")
        handler.wait()
        # Queued on the link while the handler is still busy with the first one.
        consuming.deliver("second")

        closing = threading.Thread(target=consumer.close, name="close-consumer", daemon=True)
        closing.start()
        time.sleep(0.1)
        blocked.set()
        closing.join(HANDLER_TIMEOUT)
        assert not closing.is_alive()
        assert handler.bodies == ["first"]

    def test_close_from_inside_the_handler_does_not_deadlock(self, consuming):
        holder = []
        handler = RecordingHandler(action=lambda context, message: holder[0].close())
        consumer = consuming.build(handler)
        holder.append(consumer)
        consuming.deliver()
        handler.wait()
        deadline = time.monotonic() + HANDLER_TIMEOUT
        while consumer.is_open and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not consumer.is_open
        assert consuming.connection._consumers == {}

    def test_connection_close_closes_every_consumer(self, consuming):
        consumers = [consuming.build(RecordingHandler(), queue=f"q-{index}") for index in range(3)]
        consuming.connection.close()
        assert all(not consumer.is_open for consumer in consumers)
        assert all(not consumer._delivery_loop.is_alive() for consumer in consumers)
        assert consuming.connection._consumers == {}
        assert consuming.connection._shared_session is None

    def test_connection_close_survives_a_consumer_that_cannot_detach(self, consuming):
        consumer = consuming.build(RecordingHandler())
        consuming.broker.drop_connection()
        consuming.connection.close()
        assert consuming.connection._consumers == {}
        assert not consumer._delivery_loop.is_alive()


def _poll(predicate, description, timeout=HANDLER_TIMEOUT):
    """Poll ``predicate`` until it holds.

    Raises:
        AssertionError: If it does not hold within ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out after {timeout:g}s waiting for {description}")


class StateRecorder:
    """A single-active-consumer handler that records every status it is told.

    Attributes:
        action: Optional ``action(consumer, is_active)`` run after recording, so a
            test can raise from inside the handler.
    """

    def __init__(self, action=None):
        self.action = action
        self.calls = queue_module.Queue()
        self.threads = []

    def __call__(self, consumer, is_active):
        self.threads.append(threading.current_thread().name)
        self.calls.put((consumer, is_active))
        if self.action is not None:
            self.action(consumer, is_active)

    def next_status(self, timeout=HANDLER_TIMEOUT):
        """Return the next ``(consumer, is_active)`` pair the handler was given.

        Raises:
            AssertionError: If nothing arrives within ``timeout``.
        """
        try:
            return self.calls.get(timeout=timeout)
        except queue_module.Empty:
            raise AssertionError(f"no single-active-consumer notification within {timeout:g}s") from None

    def expect_nothing(self, within=QUIET_PERIOD):
        """Assert the handler is not called for ``within`` seconds."""
        with pytest.raises(AssertionError):
            self.next_status(timeout=within)

    @property
    def count(self):
        """How many notifications are recorded but not yet read."""
        return self.calls.qsize()


class TestParseActiveFlag:
    """step_090 §1 point 2: booleans as-is, any integer type by zero/non-zero."""

    @pytest.mark.parametrize(
        "value",
        [
            True,
            Ubyte(1),
            Ushort(1),
            Uint(1),
            Ulong(2**40),
            Byte(-1),
            Short(7),
            Int(-5),
            Long(1),
            1,
        ],
    )
    def test_a_non_zero_value_means_active(self, value):
        assert parse_active_flag(value) is True

    @pytest.mark.parametrize(
        "value",
        [
            False,
            Ubyte(0),
            Ushort(0),
            Uint(0),
            Ulong(0),
            Byte(0),
            Short(0),
            Int(0),
            Long(0),
            0,
        ],
    )
    def test_a_zero_value_means_standby(self, value):
        assert parse_active_flag(value) is False

    @pytest.mark.parametrize("value", ["true", None, 1.0, b"\x01", [1]])
    def test_a_value_of_another_type_is_refused(self, value):
        with pytest.raises(ProtocolError, match="unusable"):
            parse_active_flag(value)


class TestQuorumConsumerOptions:
    """step_090 §2: a sub-builder that is a view over the same ConsumerBuilder."""

    def test_the_options_are_a_view_over_the_parent_builder(self):
        builder = ConsumerBuilder(None).queue("orders")
        options = builder.quorum()
        assert isinstance(options, QuorumConsumerOptions)
        assert options.builder() is builder

    def test_registering_a_handler_returns_the_same_view(self):
        options = ConsumerBuilder(None).queue("orders").quorum()
        assert options.single_active_consumer_state_changed(StateRecorder()) is options

    def test_the_options_need_a_queue_first(self):
        with pytest.raises(ConsumerError, match="call queue\\(\\) before quorum\\(\\)"):
            ConsumerBuilder(None).quorum()

    def test_the_last_registered_handler_wins(self):
        builder = ConsumerBuilder(None).queue("orders")
        first, second = StateRecorder(), StateRecorder()
        builder.quorum().single_active_consumer_state_changed(first)
        builder.quorum().single_active_consumer_state_changed(second)
        assert builder._single_active_consumer_handler is second

    def test_a_consumer_without_a_handler_watches_nothing(self, consuming):
        consumer = consuming.build(RecordingHandler())
        assert consumer._link._flow_handler is None
        assert consumer._notification_loop is None
        consumer.close()


class TestSingleActiveConsumerNotifications:
    """step_090 §1/§3: ``rabbitmq:active`` on an inbound ``flow`` reaches the handler."""

    def test_the_status_sent_right_after_attach_is_reported(self, harness):
        consuming = harness(receiver_flow_properties={RABBITMQ_ACTIVE_PROPERTY: True})
        states = StateRecorder()
        consumer = consuming.build(RecordingHandler(), on_state_changed=states)
        assert states.next_status() == (consumer, True)
        consumer.close()

    def test_a_status_that_lands_before_the_handler_is_registered_is_replayed(self, harness, monkeypatch):
        """step_090 §3's race: the first ``flow`` can be processed before build() wires the handler."""
        consuming = harness(receiver_flow_properties={RABBITMQ_ACTIVE_PROPERTY: Uint(1)})
        register = ReceiverLink.on_flow_properties

        def register_late(link, handler):
            """Register only once the broker's ``flow`` has really been processed."""
            if handler is not None:
                _poll(lambda: bool(link._flow_properties), "the broker's flow to be buffered")
            register(link, handler)

        monkeypatch.setattr(ReceiverLink, "on_flow_properties", register_late)
        states = StateRecorder()
        consumer = consuming.build(RecordingHandler(), on_state_changed=states)
        assert states.next_status() == (consumer, True)

        # A later flow reaches the now-registered handler directly, and the
        # replayed one is not delivered a second time.
        consuming.broker.grant_credit(consuming.channel, consuming.handle, 0, properties={RABBITMQ_ACTIVE_PROPERTY: 0})
        assert states.next_status() == (consumer, False)
        states.expect_nothing()
        consumer.close()

    def test_every_later_promotion_and_demotion_is_reported_in_order(self, consuming):
        states = StateRecorder()
        consumer = consuming.build(RecordingHandler(), on_state_changed=states)
        for value in (Ubyte(0), True, Long(0)):
            consuming.broker.grant_credit(
                consuming.channel, consuming.handle, 0, properties={RABBITMQ_ACTIVE_PROPERTY: value}
            )
        assert [states.next_status()[1] for _ in range(3)] == [False, True, False]
        consumer.close()

    def test_the_handler_runs_off_the_frame_reader_thread(self, harness):
        consuming = harness(receiver_flow_properties={RABBITMQ_ACTIVE_PROPERTY: True})
        states = StateRecorder()
        consumer = consuming.build(RecordingHandler(), on_state_changed=states)
        states.next_status()
        assert states.threads == [f"amqp-{consumer.id}-sac"]
        consumer.close()

    def test_a_flow_without_the_property_reports_nothing(self, consuming):
        """A classic or stream queue never sends it, which is not an error."""
        states = StateRecorder()
        deliveries = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(deliveries, credits=CREDITS, on_state_changed=states)
        consuming.broker.grant_credit(consuming.channel, consuming.handle, CREDITS)
        consuming.broker.grant_credit(consuming.channel, consuming.handle, CREDITS, properties={"x-other": 1})
        states.expect_nothing()

        # The link is still perfectly usable.
        consuming.deliver("still working")
        deliveries.wait()
        assert deliveries.bodies == ["still working"]
        consumer.close()

    def test_an_unusable_value_is_dropped_with_a_warning(self, consuming, caplog):
        states = StateRecorder()
        consumer = consuming.build(RecordingHandler(), on_state_changed=states)
        with caplog.at_level("WARNING", logger="src.consumer"):
            consuming.broker.grant_credit(
                consuming.channel, consuming.handle, 0, properties={RABBITMQ_ACTIVE_PROPERTY: "yes"}
            )
            _poll(lambda: "unusable" in caplog.text, "the unusable value to be logged")
        states.expect_nothing()
        consumer.close()

    def test_a_raising_handler_is_logged_and_the_consumer_keeps_working(self, consuming, caplog):
        def explode(consumer, is_active):
            raise RuntimeError("state handler is broken")

        states = StateRecorder(action=explode)
        deliveries = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(deliveries, credits=CREDITS, on_state_changed=states)
        with caplog.at_level("ERROR", logger="src.consumer"):
            for value in (True, False):
                consuming.broker.grant_credit(
                    consuming.channel, consuming.handle, 0, properties={RABBITMQ_ACTIVE_PROPERTY: value}
                )
            assert [states.next_status()[1] for _ in range(2)] == [True, False]
        assert "state handler is broken" in caplog.text

        consuming.deliver("after the failures")
        deliveries.wait()
        assert deliveries.bodies == ["after the failures"]
        assert consumer.is_open
        consumer.close()

    def test_close_stops_the_notification_loop(self, consuming):
        states = StateRecorder()
        consumer = consuming.build(RecordingHandler(), on_state_changed=states)
        loop = consumer._notification_loop
        consumer.close()
        assert not loop.is_alive()

    def test_a_status_arriving_after_close_is_not_reported(self, consuming):
        states = StateRecorder()
        consumer = consuming.build(RecordingHandler(), on_state_changed=states)
        consumer.close()
        consuming.broker.grant_credit(
            consuming.channel, consuming.handle, 0, properties={RABBITMQ_ACTIVE_PROPERTY: True}
        )
        states.expect_nothing()


class TestStreamSubBuilders:
    """step_080 §1/§3: two sub-builders, both views over the same ConsumerBuilder."""

    def test_the_options_are_a_view_over_the_parent_builder(self):
        builder = ConsumerBuilder(None).queue("events")
        options = builder.stream()
        assert isinstance(options, StreamOptions)
        assert options.builder() is builder

    def test_the_options_need_a_queue_first(self):
        with pytest.raises(ConsumerError, match="call queue\\(\\) before stream\\(\\)"):
            ConsumerBuilder(None).stream()

    def test_the_setters_return_the_same_view(self):
        options = ConsumerBuilder(None).queue("events").stream()
        assert options.offset(StreamOffsetSpecification.FIRST) is options
        assert options.filter_values("emea") is options
        assert options.filter_match_unfiltered(True) is options

    def test_the_filter_options_hop_back_to_the_stream_options(self):
        options = ConsumerBuilder(None).queue("events").stream()
        filters = options.filter()
        assert isinstance(filters, StreamFilterOptions)
        assert filters.subject("orders") is filters
        assert filters.property("region", "emea") is filters
        assert filters.sql("region = 'emea'") is filters
        assert filters.stream() is options
        assert filters.stream().builder() is options.builder()

    def test_two_views_write_to_the_same_configuration(self):
        builder = ConsumerBuilder(None).queue("events")
        builder.stream().offset(StreamOffsetSpecification.LAST)
        builder.stream().filter_values("emea")
        assert builder._stream.offset is StreamOffsetSpecification.LAST
        assert builder._stream.filter_values == ("emea",)

    def test_stream_and_quorum_options_do_not_interfere(self):
        builder = ConsumerBuilder(None).queue("events")
        states = StateRecorder()
        builder.quorum().single_active_consumer_state_changed(states)
        builder.stream().offset(StreamOffsetSpecification.FIRST).filter().subject("orders")
        assert builder._single_active_consumer_handler is states
        assert builder._stream.offset is StreamOffsetSpecification.FIRST
        assert builder._stream.subject == "orders"


class TestStreamOffsetSpecification:
    """step_080 §1.1: one ``rabbitmq:stream-offset-spec`` entry, six ways to fill it."""

    @pytest.mark.parametrize(
        "specification,expected",
        [
            (StreamOffsetSpecification.FIRST, "first"),
            (StreamOffsetSpecification.LAST, "last"),
            (StreamOffsetSpecification.NEXT, "next"),
        ],
    )
    def test_a_named_position_is_sent_by_name(self, consuming, specification, expected):
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.offset(specification), queue="events"
        )
        assert filter_set == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, expected)}
        consumer.close()

    def test_an_absolute_offset_is_sent_as_a_long(self, consuming):
        consumer, filter_set = consuming.build_stream(RecordingHandler(), lambda stream: stream.offset(2**33))
        assert filter_set == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, 2**33)}
        consumer.close()

    def test_a_zero_offset_is_sent_too(self, consuming):
        """The falsy end of the range: offset 0 is the whole stream, not "unset"."""
        consumer, filter_set = consuming.build_stream(RecordingHandler(), lambda stream: stream.offset(0))
        assert filter_set == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, 0)}
        consumer.close()

    def test_a_datetime_is_sent_as_a_timestamp_in_milliseconds(self, consuming):
        moment = datetime(2026, 7, 30, 12, 0, 0, tzinfo=timezone.utc)
        consumer, filter_set = consuming.build_stream(RecordingHandler(), lambda stream: stream.offset(moment))
        described = filter_set[STREAM_OFFSET_SPEC_FILTER]
        assert isinstance(described.value, Timestamp)
        assert described.value == int(moment.timestamp() * 1000)
        consumer.close()

    @pytest.mark.parametrize("interval", ["7Y", "3M", "7D", "12h", "30m", "10s", "0s", "1000000D"])
    def test_a_valid_interval_is_sent_unmodified(self, consuming, interval):
        consumer, filter_set = consuming.build_stream(RecordingHandler(), lambda stream: stream.offset(interval))
        assert filter_set == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, interval)}
        consumer.close()

    @pytest.mark.parametrize("interval", ["", "7", "D", "7d", "7 D", "7Days", "-7D", "1.5h", "7DD", "first"])
    def test_an_invalid_interval_is_refused_before_anything_is_attached(self, consuming, interval):
        builder = (
            consuming.connection.consumer_builder()
            .queue("events")
            .message_handler(RecordingHandler())
            .stream()
            .offset(interval)
            .builder()
        )
        with pytest.raises(ConsumerError, match="is not a stream offset interval"):
            builder.build()
        assert consuming.broker.all_received(Attach) == []
        assert consuming.connection._shared_session is None

    def test_a_negative_absolute_offset_is_refused_before_anything_is_attached(self, consuming):
        builder = (
            consuming.connection.consumer_builder()
            .queue("events")
            .message_handler(RecordingHandler())
            .stream()
            .offset(-1)
            .builder()
        )
        with pytest.raises(ConsumerError, match="must be >= 0"):
            builder.build()
        assert consuming.broker.all_received(Attach) == []
        assert consuming.connection._shared_session is None

    def test_the_last_offset_call_wins(self, consuming):
        def configure(stream):
            stream.offset(StreamOffsetSpecification.FIRST)
            stream.offset("7D")
            return stream.offset(17)

        consumer, filter_set = consuming.build_stream(RecordingHandler(), configure)
        assert filter_set == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, 17)}
        consumer.close()

    def test_without_an_offset_call_the_broker_decides(self, consuming):
        consumer = consuming.build(RecordingHandler())
        assert consuming.attach.source.filter is None
        consumer.close()

    def test_an_invalid_offset_only_bites_at_build_time(self, consuming):
        """§1.1 puts the check at build(), so a builder can be fixed up before it."""
        options = consuming.connection.consumer_builder().queue("events").stream().offset("nonsense")
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.offset(StreamOffsetSpecification.LAST)
        )
        assert options.builder()._stream.offset == "nonsense"
        assert filter_set == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, "last")}
        consumer.close()


class TestStreamBloomFilter:
    """step_080 §2.2: the two entries the segment-level bloom filter is driven by."""

    def test_filter_values_are_sent_as_a_list_of_strings(self, consuming):
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.filter_values("emea", "apac")
        )
        assert filter_set == {STREAM_FILTER_VALUES_FILTER: Described(STREAM_FILTER_VALUES_FILTER, ["emea", "apac"])}
        consumer.close()

    def test_a_second_filter_values_call_replaces_the_whole_set(self, consuming):
        def configure(stream):
            stream.filter_values("emea")
            return stream.filter_values("apac", "amer")

        consumer, filter_set = consuming.build_stream(RecordingHandler(), configure)
        assert filter_set == {STREAM_FILTER_VALUES_FILTER: Described(STREAM_FILTER_VALUES_FILTER, ["apac", "amer"])}
        consumer.close()

    @pytest.mark.parametrize("match_unfiltered,expected", [(True, True), (False, False)])
    def test_match_unfiltered_is_sent_as_a_boolean(self, consuming, match_unfiltered, expected):
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(),
            lambda stream: stream.filter_values("emea").filter_match_unfiltered(match_unfiltered),
        )
        assert filter_set[STREAM_MATCH_UNFILTERED_FILTER] == Described(STREAM_MATCH_UNFILTERED_FILTER, expected)
        consumer.close()

    def test_match_unfiltered_defaults_to_true(self, consuming):
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.filter_match_unfiltered()
        )
        assert filter_set == {STREAM_MATCH_UNFILTERED_FILTER: Described(STREAM_MATCH_UNFILTERED_FILTER, True)}
        consumer.close()

    def test_match_unfiltered_without_any_filter_values_is_not_refused(self, consuming):
        """§2.2: a no-op filter that delivers everything is legal, not an error."""
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.filter_match_unfiltered(True)
        )
        assert STREAM_FILTER_VALUES_FILTER not in filter_set
        assert consumer.is_open
        consumer.close()


class TestStreamAmqpFilterExpressions:
    """step_080 §3: broker-side matching against the message's own sections."""

    def test_a_subject_is_a_properties_filter_map(self, consuming):
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.filter().subject("orders").stream()
        )
        assert filter_set == {AMQP_PROPERTIES_FILTER: Described(AMQP_PROPERTIES_FILTER, {"subject": "orders"})}
        consumer.close()

    def test_the_subject_key_is_a_symbol(self, consuming):
        """The broker only reads properties-filter keys that arrive as symbols."""
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.filter().subject("orders").stream()
        )
        (key,) = filter_set[AMQP_PROPERTIES_FILTER].value
        assert isinstance(key, Symbol)
        consumer.close()

    def test_a_second_subject_call_overwrites_the_first(self, consuming):
        def configure(stream):
            filters = stream.filter().subject("orders")
            return filters.subject("invoices")

        consumer, filter_set = consuming.build_stream(RecordingHandler(), configure)
        assert filter_set == {AMQP_PROPERTIES_FILTER: Described(AMQP_PROPERTIES_FILTER, {"subject": "invoices"})}
        consumer.close()

    def test_properties_accumulate_one_entry_per_key(self, consuming):
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(),
            lambda stream: stream.filter().property("region", "emea").property("tier", 2).stream(),
        )
        assert filter_set == {
            AMQP_APPLICATION_PROPERTIES_FILTER: Described(
                AMQP_APPLICATION_PROPERTIES_FILTER, {"region": "emea", "tier": 2}
            )
        }
        consumer.close()

    def test_the_property_keys_stay_strings(self, consuming):
        """The broker only reads application-properties-filter keys as strings."""
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.filter().property("region", "emea").stream()
        )
        (key,) = filter_set[AMQP_APPLICATION_PROPERTIES_FILTER].value
        assert type(key) is str
        consumer.close()

    def test_repeating_a_property_key_overwrites_just_that_entry(self, consuming):
        def configure(stream):
            filters = stream.filter().property("region", "emea").property("tier", 2)
            return filters.property("region", "apac")

        consumer, filter_set = consuming.build_stream(RecordingHandler(), configure)
        assert filter_set == {
            AMQP_APPLICATION_PROPERTIES_FILTER: Described(
                AMQP_APPLICATION_PROPERTIES_FILTER, {"region": "apac", "tier": 2}
            )
        }
        consumer.close()

    def test_sql_is_sent_as_the_raw_expression(self, consuming):
        expression = "properties.subject LIKE 'orders%' AND region = 'emea'"
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(), lambda stream: stream.filter().sql(expression).stream()
        )
        assert filter_set == {SQL_FILTER_NAME: Described(AMQP_SQL_FILTER, expression)}
        consumer.close()

    def test_a_second_sql_call_overwrites_the_first(self, consuming):
        def configure(stream):
            filters = stream.filter().sql("region = 'emea'")
            return filters.sql("region = 'apac'")

        consumer, filter_set = consuming.build_stream(RecordingHandler(), configure)
        assert filter_set == {SQL_FILTER_NAME: Described(AMQP_SQL_FILTER, "region = 'apac'")}
        consumer.close()

    def test_every_filter_type_lands_in_the_same_map(self, consuming):
        """§3: the broker ANDs across descriptors, so combining them is one attach."""

        def configure(stream):
            return (
                stream.offset(StreamOffsetSpecification.FIRST)
                .filter_values("emea")
                .filter_match_unfiltered(True)
                .filter()
                .subject("orders")
                .property("region", "emea")
                .sql("region = 'emea'")
                .stream()
            )

        consumer, filter_set = consuming.build_stream(RecordingHandler(), configure)
        assert filter_set == {
            STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, "first"),
            STREAM_FILTER_VALUES_FILTER: Described(STREAM_FILTER_VALUES_FILTER, ["emea"]),
            STREAM_MATCH_UNFILTERED_FILTER: Described(STREAM_MATCH_UNFILTERED_FILTER, True),
            AMQP_PROPERTIES_FILTER: Described(AMQP_PROPERTIES_FILTER, {"subject": "orders"}),
            AMQP_APPLICATION_PROPERTIES_FILTER: Described(AMQP_APPLICATION_PROPERTIES_FILTER, {"region": "emea"}),
            SQL_FILTER_NAME: Described(AMQP_SQL_FILTER, "region = 'emea'"),
        }
        consumer.close()

    def test_the_filter_entries_survive_the_round_trip_as_described_values(self, consuming):
        """Every entry must be a described type, or RabbitMQ ignores it outright."""
        consumer, filter_set = consuming.build_stream(
            RecordingHandler(),
            lambda stream: stream.offset("7D").filter_values("emea").filter().subject("orders").stream(),
        )
        assert all(isinstance(value, Described) for value in filter_set.values())
        consumer.close()


class TestStreamOffsetTracking:
    """The offset a re-attach resumes from, read off the deliveries themselves."""

    def test_a_stream_delivery_updates_the_last_offset(self, consuming):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(handler, credits=CREDITS)
        assert consumer.last_stream_offset is None

        consuming.deliver("first", annotations={STREAM_OFFSET_ANNOTATION: 41})
        consuming.deliver("second", annotations={STREAM_OFFSET_ANNOTATION: 42})
        handler.wait(2)
        assert consumer.last_stream_offset == 42
        consumer.close()

    def test_a_lower_offset_does_not_move_it_backwards(self, consuming):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver("high", annotations={STREAM_OFFSET_ANNOTATION: 9})
        consuming.deliver("low", annotations={STREAM_OFFSET_ANNOTATION: 1})
        handler.wait(2)
        assert consumer.last_stream_offset == 9
        consumer.close()

    @pytest.mark.parametrize("annotations", [None, {"x-other": 3}, {STREAM_OFFSET_ANNOTATION: "not-an-offset"}])
    def test_a_delivery_without_a_usable_offset_changes_nothing(self, consuming, annotations):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(handler, credits=CREDITS)
        consuming.deliver("plain", annotations=annotations)
        handler.wait(1)
        assert consumer.last_stream_offset is None
        consumer.close()

    def test_a_re_attach_resumes_one_past_the_last_offset_seen(self, consuming):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(
            handler, credits=CREDITS, stream=lambda stream: stream.offset(StreamOffsetSpecification.FIRST)
        )
        consuming.deliver("replayed", annotations={STREAM_OFFSET_ANNOTATION: 7})
        handler.wait(1)
        assert consumer._effective_stream_filter() == {
            STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, 8)
        }
        consumer.close()

    def test_a_re_attach_keeps_the_other_filter_entries(self, consuming):
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = consuming.build(
            handler,
            credits=CREDITS,
            stream=lambda stream: stream.offset(StreamOffsetSpecification.FIRST).filter_values("emea"),
        )
        consuming.deliver("replayed", annotations={STREAM_OFFSET_ANNOTATION: 0})
        handler.wait(1)
        assert consumer._effective_stream_filter() == {
            STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, 1),
            STREAM_FILTER_VALUES_FILTER: Described(STREAM_FILTER_VALUES_FILTER, ["emea"]),
        }
        consumer.close()

    def test_a_consumer_that_saw_no_offset_re_attaches_unchanged(self, consuming):
        consumer = consuming.build(RecordingHandler(), stream=lambda stream: stream.offset("7D"))
        assert consumer._effective_stream_filter() == {
            STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, "7D")
        }
        consumer.close()

    def test_a_plain_consumer_re_attaches_without_a_filter_set(self, consuming):
        consumer = consuming.build(RecordingHandler())
        assert consumer._effective_stream_filter() is None
        consumer.close()
