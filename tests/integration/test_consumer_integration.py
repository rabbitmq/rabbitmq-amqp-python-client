"""Consumers against a live RabbitMQ broker.

Covers what only a real broker can show: that credit really does gate delivery
while a consumer is paused, that a ``released``/``rejected`` disposition really
does requeue or drop the message, that a presettled link really is settled by
the broker before the client sees the ``transfer``, and that direct-reply-to
really hands back a usable, broker-generated pseudo-queue address
(step_060_consumer_strategy.md §7), that a single-active-consumer quorum queue
really does report which consumer it feeds (step_090 §6), and which of a
stream's offset specifications and filters the broker really honours
(step_080 §6).
"""

from __future__ import annotations

import contextlib
import itertools
import queue as queue_module
import threading
import time
import uuid

import pytest

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    ConsumerError,
    ConsumerSettleStrategy,
    OutcomeState,
    StreamOffsetSpecification,
)
from rabbitmq_amqp_python_client.constants import STREAM_FILTER_VALUE_ANNOTATION
from rabbitmq_amqp_python_client.wire import (
    ApplicationProperties,
    Message,
    MessageAnnotations,
    Properties,
    Symbol,
)

pytestmark = pytest.mark.integration

#: Bound on anything a test expects the broker to do promptly.
WAIT_TIMEOUT_SECONDS = 15.0

#: How long a test waits to conclude the broker is sending nothing.
QUIET_PERIOD_SECONDS = 2.0

#: Messages a batch test publishes.
BATCH = 20


def _name(prefix: str) -> str:
    """A unique name for one test's topology."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _wait_until(predicate, description, timeout=WAIT_TIMEOUT_SECONDS):
    """Poll ``predicate`` until it holds.

    Raises:
        AssertionError: If it does not hold within ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out after {timeout:g}s waiting for {description}")


class RecordingHandler:
    """Records every delivery, and runs an optional action on each one."""

    def __init__(self, action=None):
        self.action = action
        self._lock = threading.Lock()
        self._bodies = []
        self.errors = []

    def __call__(self, context, message):
        with self._lock:
            self._bodies.append(message.body_as_string())
        if self.action is None:
            return
        try:
            self.action(context, message)
        except ConsumerError as error:
            with self._lock:
                self.errors.append(error)

    @property
    def bodies(self):
        """Bodies seen so far, in arrival order."""
        with self._lock:
            return list(self._bodies)

    @property
    def count(self):
        """How many deliveries have been seen."""
        with self._lock:
            return len(self._bodies)

    def wait(self, count, timeout=WAIT_TIMEOUT_SECONDS):
        """Block until ``count`` deliveries have been seen."""
        _wait_until(lambda: self.count >= count, f"{count} deliveries", timeout)


@pytest.fixture
def connection():
    """An open connection to the local broker, closed on teardown."""
    opened = Connection(ConnectionParameters())
    try:
        yield opened
    finally:
        opened.close()


@pytest.fixture
def management(connection):
    """The connection's management endpoint."""
    return connection.management()


@pytest.fixture
def queue(management):
    """Return ``make(prefix, **kind) -> name``, deleting whatever it declared."""
    declared = []

    def make(prefix, quorum=False, single_active_consumer=False, stream=False):
        specification = management.queue(_name(prefix))
        if single_active_consumer:
            specification = specification.single_active_consumer(True)
        if quorum or single_active_consumer:
            specification = specification.quorum().queue()
        if stream:
            specification = specification.stream().queue()
        info = specification.declare()
        declared.append(info.name)
        return info.name

    yield make

    for name in declared:
        with contextlib.suppress(Exception):  # cleanup must not mask a failure
            management.queue(name).delete()


@pytest.fixture
def consumers():
    """Collects consumers and closes them before the connection goes away."""
    created = []
    try:
        yield created
    finally:
        for consumer in created:
            with contextlib.suppress(Exception):  # cleanup must not mask a failure
                consumer.close()


@pytest.fixture
def publish(connection):
    """Return ``send(queue_name, messages)``, publishing each one and asserting it landed.

    A plain string stands for a message with just that body, so the many tests
    that only care about bodies stay readable.
    """
    publishers = {}

    def send(queue_name, messages):
        publisher = publishers.get(queue_name)
        if publisher is None:
            publisher = connection.publisher_builder().queue(queue_name).build()
            publishers[queue_name] = publisher
        for message in messages:
            payload = Message(message) if isinstance(message, str) else message
            result = publisher.publish(payload, timeout=WAIT_TIMEOUT_SECONDS)
            assert result.outcome.state is OutcomeState.ACCEPTED, result.outcome

    yield send

    for publisher in publishers.values():
        with contextlib.suppress(Exception):  # cleanup must not mask a failure
            publisher.close()


def _consume(connection, consumers, queue_name, handler, **options):
    """Build a consumer on ``queue_name`` and register it for teardown.

    Args:
        connection: Connection to build on.
        consumers: List every built consumer is appended to, for teardown.
        queue_name: Queue to consume from, or ``None`` for
            ``settle_strategy=ConsumerSettleStrategy.DIRECT_REPLY_TO``, whose
            address is broker-generated (step_060_consumer_strategy.md §3.3).
        handler: The message handler.
        **options: ``credits``, ``settle_strategy``, ``on_state_changed``, and
            ``stream``, a ``stream(stream_options)`` callable configuring the
            stream sub-builder.
    """
    builder = connection.consumer_builder()
    if queue_name is not None:
        builder = builder.queue(queue_name)
    builder = builder.message_handler(handler)
    if "credits" in options:
        builder.initial_credits(options["credits"])
    if options.get("settle_strategy") is not None:
        builder.settle_strategy(options["settle_strategy"])
    if options.get("on_state_changed") is not None:
        builder.quorum().single_active_consumer_state_changed(options["on_state_changed"]).builder()
    if options.get("stream") is not None:
        options["stream"](builder.stream())
    consumer = builder.build()
    consumers.append(consumer)
    return consumer


class TestConsumeAndAccept:
    """step_030 §3.2/§3.3/§3.6: every message reaches the handler and is settled."""

    def test_all_published_messages_are_delivered_and_accepted(self, connection, management, queue, publish, consumers):
        name = queue("con-it-accept")
        publish(name, [f"m-{index}" for index in range(BATCH)])

        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = _consume(connection, consumers, name, handler)
        handler.wait(BATCH)

        assert handler.bodies == [f"m-{index}" for index in range(BATCH)]
        _wait_until(lambda: consumer.unsettled_message_count == 0, "every delivery to be settled")
        _wait_until(lambda: management.queue_info(name).message_count == 0, "the queue to drain")
        assert consumer.queue == name

    def test_messages_published_after_the_consumer_attached_are_delivered(self, connection, queue, publish, consumers):
        name = queue("con-it-live")
        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(connection, consumers, name, handler, credits=3)
        publish(name, ["one", "two", "three", "four", "five"])
        handler.wait(5)
        assert sorted(handler.bodies) == ["five", "four", "one", "three", "two"]

    def test_two_consumers_share_the_queue_and_the_session(self, connection, queue, publish, consumers):
        name = queue("con-it-two")
        first = RecordingHandler(action=lambda context, message: context.accept())
        second = RecordingHandler(action=lambda context, message: context.accept())
        one = _consume(connection, consumers, name, first, credits=1)
        two = _consume(connection, consumers, name, second, credits=1)
        assert one._session is two._session

        publish(name, [f"m-{index}" for index in range(10)])
        _wait_until(lambda: first.count + second.count == 10, "both consumers to drain the queue")
        assert set(first.bodies + second.bodies) == {f"m-{index}" for index in range(10)}

    def test_a_missing_queue_is_refused_at_attach(self, connection):
        with pytest.raises(ConsumerError):
            connection.consumer_builder().queue(_name("con-it-absent")).message_handler(
                lambda context, message: None
            ).build()


class TestPause:
    """step_030 §3.4: a paused consumer receives nothing new until unpaused."""

    def test_nothing_arrives_while_paused(self, connection, queue, publish, consumers):
        name = queue("con-it-pause")
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = _consume(connection, consumers, name, handler)

        publish(name, ["before-1", "before-2"])
        handler.wait(2)
        assert sorted(handler.bodies) == ["before-1", "before-2"]

        # Pause before publishing, so nothing can already be in flight.
        consumer.pause()
        assert consumer.is_paused
        publish(name, [f"while-paused-{index}" for index in range(3)])
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.count == 2, f"deliveries arrived while paused: {handler.bodies}"

        consumer.unpause()
        assert not consumer.is_paused
        handler.wait(5)
        assert sorted(handler.bodies[2:]) == [f"while-paused-{index}" for index in range(3)]


class TestPresettled:
    """step_060_consumer_strategy.md §3.2/§7: the broker settles every delivery itself."""

    def test_all_messages_arrive_and_nothing_is_ever_unsettled(self, connection, management, queue, publish, consumers):
        name = queue("con-it-presettled", quorum=True)
        publish(name, [f"m-{index}" for index in range(BATCH)])

        refusals = []

        def try_to_settle(context, message):
            """Prove the broker settled it: every outcome the client offers is refused."""
            for settle in (context.accept, context.discard, context.requeue):
                try:
                    settle()
                    refusals.append(None)
                except ConsumerError as error:
                    refusals.append(error)

        handler = RecordingHandler(action=try_to_settle)
        consumer = _consume(connection, consumers, name, handler, settle_strategy=ConsumerSettleStrategy.PRESETTLED)

        # Sampled from this thread while deliveries are in flight, so "always 0"
        # is checked throughout rather than only at the end.
        deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS
        while handler.count < BATCH and time.monotonic() < deadline:
            assert consumer.unsettled_message_count == 0
            time.sleep(0.01)
        handler.wait(BATCH)

        assert handler.bodies == [f"m-{index}" for index in range(BATCH)]
        assert consumer.unsettled_message_count == 0
        assert len(refusals) == 3 * BATCH
        assert all(isinstance(refusal, ConsumerError) for refusal in refusals)
        assert all("presettled" in str(refusal) for refusal in refusals)
        _wait_until(lambda: management.queue_info(name).message_count == 0, "the queue to drain")


class TestDirectReplyTo:
    """step_060_consumer_strategy.md §3.3/§7: request/reply over the broker-generated pseudo-queue."""

    def test_a_reply_reaches_the_requester_and_is_presettled(self, connection, queue, consumers):
        request_queue = queue("con-it-direct-reply-to")

        reply_handler = RecordingHandler()
        requester = _consume(
            connection, consumers, None, reply_handler, settle_strategy=ConsumerSettleStrategy.DIRECT_REPLY_TO
        )
        reply_to = requester.queue
        assert reply_to is not None
        assert reply_to.startswith("/queues/amq.rabbitmq.reply-to.")

        def respond(context, message):
            responder_publisher = connection.publisher_builder().build()
            try:
                responder_publisher.publish(
                    Message("pong", properties=Properties(to=message.properties.reply_to)),
                    timeout=WAIT_TIMEOUT_SECONDS,
                )
            finally:
                responder_publisher.close()
            context.accept()

        request_handler = RecordingHandler(action=respond)
        _consume(connection, consumers, request_queue, request_handler)

        requester_publisher = connection.publisher_builder().build()
        try:
            requester_publisher.publish(
                Message(
                    "ping",
                    properties=Properties(to=f"/queues/{request_queue}", reply_to=reply_to),
                ),
                timeout=WAIT_TIMEOUT_SECONDS,
            )
        finally:
            requester_publisher.close()

        reply_handler.wait(1)
        assert reply_handler.bodies == ["pong"]

    def test_context_methods_are_refused_on_a_direct_reply_to_delivery(self, connection, queue, consumers):
        request_queue = queue("con-it-direct-reply-to-context")
        contexts = []

        def capture(context, message):
            contexts.append(context)

        requester = _consume(
            connection, consumers, None, capture, settle_strategy=ConsumerSettleStrategy.DIRECT_REPLY_TO
        )
        reply_to = requester.queue
        assert reply_to is not None

        def respond(context, message):
            responder_publisher = connection.publisher_builder().build()
            try:
                responder_publisher.publish(
                    Message("pong", properties=Properties(to=message.properties.reply_to)),
                    timeout=WAIT_TIMEOUT_SECONDS,
                )
            finally:
                responder_publisher.close()
            context.accept()

        request_handler = RecordingHandler(action=respond)
        _consume(connection, consumers, request_queue, request_handler)

        requester_publisher = connection.publisher_builder().build()
        try:
            requester_publisher.publish(
                Message("ping", properties=Properties(to=f"/queues/{request_queue}", reply_to=reply_to)),
                timeout=WAIT_TIMEOUT_SECONDS,
            )
        finally:
            requester_publisher.close()

        _wait_until(lambda: len(contexts) == 1, "the reply to arrive")
        context = contexts[0]
        assert context.is_presettled is True
        for settle in (context.accept, context.discard, context.requeue):
            with pytest.raises(ConsumerError, match="presettled"):
                settle()
        assert requester.unsettled_message_count == 0


class TestRequeueAndDiscard:
    """step_030 §4: the outcome the client reports decides the message's fate."""

    def test_a_requeued_message_reaches_a_fresh_consumer(self, connection, queue, publish, consumers):
        name = queue("con-it-requeue")
        publish(name, ["retry me"])
        attempts = itertools.count()

        def requeue_once(context, message):
            if next(attempts) == 0:
                context.requeue()
            # A redelivery is left unsettled, so closing releases it again rather
            # than bouncing the message around this link forever.

        requeued = RecordingHandler(action=requeue_once)
        first = _consume(connection, consumers, name, requeued, credits=1)
        requeued.wait(1)
        first.close()

        accepted = RecordingHandler(action=lambda context, message: context.accept())
        _consume(connection, consumers, name, accepted, credits=1)
        accepted.wait(1)
        assert accepted.bodies == ["retry me"]

    def test_a_requeue_counted_as_a_failed_attempt_is_redelivered(self, connection, queue, publish, consumers):
        name = queue("con-it-requeue-annotated", quorum=True)
        publish(name, ["annotate me"])
        attempts = itertools.count()

        def requeue_once(context, message):
            if next(attempts) == 0:
                context.requeue({"x-attempt": 1}, delivery_failed=True)
            else:
                context.accept()

        handler = RecordingHandler(action=requeue_once)
        consumer = _consume(connection, consumers, name, handler, credits=1)
        handler.wait(2)
        assert handler.bodies == ["annotate me", "annotate me"]
        _wait_until(lambda: consumer.unsettled_message_count == 0, "the second delivery to be accepted")

    def test_a_discarded_message_is_dropped(self, connection, management, queue, publish, consumers):
        name = queue("con-it-discard")
        publish(name, ["drop me"])

        handler = RecordingHandler(action=lambda context, message: context.discard())
        consumer = _consume(connection, consumers, name, handler, credits=1)
        handler.wait(1)
        _wait_until(lambda: consumer.unsettled_message_count == 0, "the discard to be sent")
        _wait_until(lambda: management.queue_info(name).message_count == 0, "the queue to drain")

        # A dropped message must not come back: nothing else is delivered.
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.count == 1

    def test_a_discard_with_annotations_is_dropped_too(self, connection, management, queue, publish, consumers):
        name = queue("con-it-discard-annotated", quorum=True)
        publish(name, ["drop me too"])

        handler = RecordingHandler(action=lambda context, message: context.discard({"x-reason": "unparseable"}))
        consumer = _consume(connection, consumers, name, handler, credits=1)
        handler.wait(1)
        _wait_until(lambda: consumer.unsettled_message_count == 0, "the discard to be sent")
        _wait_until(lambda: management.queue_info(name).message_count == 0, "the queue to drain")
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.count == 1


class TestLifecycle:
    """step_030 §3.5/§6: closing one consumer, and closing the connection."""

    def test_close_stops_deliveries_and_leaves_the_session_usable(self, connection, queue, publish, consumers):
        name = queue("con-it-close")
        first = RecordingHandler(action=lambda context, message: context.accept())
        consumer = _consume(connection, consumers, name, first, credits=1)
        publish(name, ["before close"])
        first.wait(1)
        consumer.close()
        assert not consumer.is_open
        assert not consumer._delivery_loop.is_alive()

        publish(name, ["after close"])
        time.sleep(QUIET_PERIOD_SECONDS)
        assert first.count == 1

        second = RecordingHandler(action=lambda context, message: context.accept())
        replacement = _consume(connection, consumers, name, second, credits=1)
        assert replacement._session is consumer._session
        second.wait(1)
        assert second.bodies == ["after close"]

    def test_connection_close_closes_every_consumer(self, queue, publish):
        name = queue("con-it-conn-close")
        own_connection = Connection(ConnectionParameters())
        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = own_connection.consumer_builder().queue(name).message_handler(handler).initial_credits(1).build()
        publish(name, ["one"])
        handler.wait(1)

        own_connection.close()
        assert not consumer.is_open
        assert not consumer._delivery_loop.is_alive()
        assert own_connection._consumers == {}


class StateRecorder:
    """Records every single-active-consumer status the broker reports."""

    def __init__(self):
        self.statuses = queue_module.Queue()

    def __call__(self, consumer, is_active):
        self.statuses.put(is_active)

    def next_status(self, timeout=WAIT_TIMEOUT_SECONDS):
        """Return the next status reported.

        Raises:
            AssertionError: If none is reported within ``timeout``.
        """
        try:
            return self.statuses.get(timeout=timeout)
        except queue_module.Empty:
            raise AssertionError(f"no single-active-consumer notification within {timeout:g}s") from None

    def expect_nothing(self, within=QUIET_PERIOD_SECONDS):
        """Assert no status is reported for ``within`` seconds."""
        with pytest.raises(AssertionError):
            self.next_status(timeout=within)


class TestSingleActiveConsumer:
    """step_090 §6: the broker tells each consumer of a SAC quorum queue where it stands."""

    def test_a_lone_consumer_is_told_it_is_active(self, connection, queue, consumers):
        name = queue("con-it-sac-alone", single_active_consumer=True)
        states = StateRecorder()
        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(connection, consumers, name, handler, on_state_changed=states)

        assert states.next_status() is True
        states.expect_nothing()

    def test_the_second_consumer_stands_by_and_receives_nothing(self, connection, queue, publish, consumers):
        name = queue("con-it-sac-standby", single_active_consumer=True)
        active_states, standby_states = StateRecorder(), StateRecorder()
        active_handler = RecordingHandler(action=lambda context, message: context.accept())
        standby_handler = RecordingHandler(action=lambda context, message: context.accept())

        _consume(connection, consumers, name, active_handler, credits=1, on_state_changed=active_states)
        assert active_states.next_status() is True
        _consume(connection, consumers, name, standby_handler, credits=1, on_state_changed=standby_states)
        assert standby_states.next_status() is False

        publish(name, [f"m-{index}" for index in range(5)])
        active_handler.wait(5)
        assert active_handler.bodies == [f"m-{index}" for index in range(5)]
        time.sleep(QUIET_PERIOD_SECONDS)
        assert standby_handler.count == 0, f"the standby consumer received {standby_handler.bodies}"

    def test_closing_the_active_consumer_promotes_the_standby_one(self, connection, queue, publish, consumers):
        name = queue("con-it-sac-promote", single_active_consumer=True)
        active_states, standby_states = StateRecorder(), StateRecorder()
        active_handler = RecordingHandler(action=lambda context, message: context.accept())
        standby_handler = RecordingHandler(action=lambda context, message: context.accept())

        active = _consume(connection, consumers, name, active_handler, credits=1, on_state_changed=active_states)
        assert active_states.next_status() is True
        _consume(connection, consumers, name, standby_handler, credits=1, on_state_changed=standby_states)
        assert standby_states.next_status() is False

        active.close()
        assert standby_states.next_status() is True, "the standby consumer was never promoted"

        publish(name, ["after the promotion"])
        standby_handler.wait(1)
        assert standby_handler.bodies == ["after the promotion"]
        assert active_handler.count == 0


#: Subjects and regions the stream tests alternate between, so every filter has
#: a clearly-distinguishable subset to select.
SUBJECTS = ("orders", "invoices")
REGIONS = ("emea", "apac")


def _stream_messages(count):
    """Build ``count`` tagged messages, alternating subject and region.

    Each one carries ``properties.subject``, an application property ``region``,
    and the ``x-stream-filter-value`` annotation the bloom filter reads
    (step_080 §2.1) — set here directly on the message, since tagging needs no
    publisher API of its own. The annotation key has to be a symbol: RabbitMQ
    refuses a message-annotations map with string keys outright.
    """
    messages = []
    for index in range(count):
        subject = SUBJECTS[index % len(SUBJECTS)]
        region = REGIONS[index % len(REGIONS)]
        messages.append(
            Message(
                f"m-{index}-{subject}-{region}",
                properties=Properties(subject=subject),
                application_properties=ApplicationProperties({"region": region}),
                message_annotations=MessageAnnotations({Symbol(STREAM_FILTER_VALUE_ANNOTATION): subject}),
            )
        )
    return messages


def _bodies_matching(count, subject=None, region=None):
    """The bodies of :func:`_stream_messages` that match ``subject``/``region``."""
    return [
        message.body_as_string()
        for message in _stream_messages(count)
        if (subject is None or message.properties.subject == subject)
        and (region is None or message.application_properties.value["region"] == region)
    ]


class TestStreamOffsets:
    """step_080 §1.1/§6.1: where in the stream a consumer starts reading."""

    def test_first_replays_the_whole_retained_stream(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-first", stream=True)
        publish(name, [f"m-{index}" for index in range(BATCH)])

        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: stream.offset(StreamOffsetSpecification.FIRST),
        )
        handler.wait(BATCH)

        assert handler.bodies == [f"m-{index}" for index in range(BATCH)]
        # Every stream delivery is annotated with its offset, which is what a
        # re-attach after a reconnect resumes past.
        assert consumer.last_stream_offset == BATCH - 1

    def test_next_skips_the_history_and_delivers_only_what_follows(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-next", stream=True)
        publish(name, [f"old-{index}" for index in range(BATCH)])

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: stream.offset(StreamOffsetSpecification.NEXT),
        )
        # Nothing retained may be replayed, so give the broker a moment to prove
        # it sends nothing before publishing the one message that must arrive.
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.count == 0, f"next replayed {handler.bodies}"

        publish(name, ["brand new"])
        handler.wait(1)
        assert handler.bodies == ["brand new"]

    def test_an_absolute_offset_starts_exactly_there(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-offset", stream=True)
        publish(name, [f"m-{index}" for index in range(BATCH)])

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(connection, consumers, name, handler, stream=lambda stream: stream.offset(BATCH - 2))
        handler.wait(2)
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.bodies == [f"m-{BATCH - 2}", f"m-{BATCH - 1}"]

    def test_an_invalid_interval_never_reaches_the_broker(self, connection, queue):
        name = queue("con-it-stream-bad-interval", stream=True)
        builder = (
            connection.consumer_builder()
            .queue(name)
            .message_handler(lambda context, message: context.accept())
            .stream()
            .offset("7 days")
            .builder()
        )
        with pytest.raises(ConsumerError, match="is not a stream offset interval"):
            builder.build()


class TestStreamAmqpFilterExpressions:
    """step_080 §3/§6.2: the broker matches properties and application properties exactly."""

    def test_a_subject_and_a_property_filter_select_exactly_the_matching_subset(
        self, connection, queue, publish, consumers
    ):
        name = queue("con-it-stream-prop-filter", stream=True)
        publish(name, _stream_messages(BATCH))
        expected = _bodies_matching(BATCH, subject="orders", region="emea")
        assert expected, "the fixture must produce at least one matching message"

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: (
                stream.offset(StreamOffsetSpecification.FIRST)
                .filter()
                .subject("orders")
                .property("region", "emea")
                .stream()
            ),
        )
        handler.wait(len(expected))
        # The filters are ANDed and evaluated broker-side, so nothing else may
        # arrive however long we wait.
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.bodies == expected

    def test_a_property_filter_alone_selects_exactly_that_property(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-app-prop", stream=True)
        publish(name, _stream_messages(BATCH))
        expected = _bodies_matching(BATCH, region="apac")

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: (
                stream.offset(StreamOffsetSpecification.FIRST).filter().property("region", "apac").stream()
            ),
        )
        handler.wait(len(expected))
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.bodies == expected


class TestStreamBloomFilter:
    """step_080 §2/§6.3: no false negatives; false positives are allowed."""

    def test_every_tagged_message_is_delivered(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-bloom", stream=True)
        publish(name, _stream_messages(BATCH))
        tagged = _bodies_matching(BATCH, subject="orders")

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: stream.offset(StreamOffsetSpecification.FIRST).filter_values("orders"),
        )
        handler.wait(len(tagged))
        time.sleep(QUIET_PERIOD_SECONDS)
        # A bloom filter may hand over a non-matching message, so only the
        # absence of false negatives can be asserted (§2).
        assert set(tagged) <= set(handler.bodies)

    def test_match_unfiltered_also_delivers_untagged_messages(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-unfiltered", stream=True)
        publish(name, [*_stream_messages(BATCH), Message("untagged", properties=Properties(subject="none"))])
        tagged = _bodies_matching(BATCH, subject="orders")

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: (
                stream.offset(StreamOffsetSpecification.FIRST).filter_values("orders").filter_match_unfiltered(True)
            ),
        )
        handler.wait(len(tagged) + 1)
        assert {*tagged, "untagged"} <= set(handler.bodies)

    def test_without_match_unfiltered_an_untagged_message_is_left_out(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-filtered-only", stream=True)
        publish(name, [*_stream_messages(BATCH), Message("untagged", properties=Properties(subject="none"))])
        tagged = _bodies_matching(BATCH, subject="orders")

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: stream.offset(StreamOffsetSpecification.FIRST).filter_values("orders"),
        )
        handler.wait(len(tagged))
        time.sleep(QUIET_PERIOD_SECONDS)
        assert "untagged" not in handler.bodies


class TestStreamSqlFilter:
    """step_080 §3/§6.4: one broker-evaluated expression (RabbitMQ 4.2+)."""

    def test_a_sql_expression_selects_exactly_the_matching_subset(self, connection, queue, publish, consumers):
        """The strong assertion of §6.4: this broker really does enforce the expression.

        §6.4 allows the weaker "the attach succeeds and messages still flow"
        assertion for brokers that silently ignore the filter, which is what
        happens when the filter is named ``amqp:sql-filter`` in the filter set
        instead of ``sql-filter``. Named correctly, RabbitMQ 4.3.2 — the broker
        this suite runs against — filters as precisely as the property filters
        above, so nothing weaker is asserted here.
        """
        name = queue("con-it-stream-sql", stream=True)
        publish(name, _stream_messages(BATCH))
        expected = _bodies_matching(BATCH, subject="orders", region="emea")

        handler = RecordingHandler(action=lambda context, message: context.accept())
        _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: (
                stream.offset(StreamOffsetSpecification.FIRST)
                .filter()
                .sql("properties.subject LIKE 'orders%' AND region = 'emea'")
                .stream()
            ),
        )
        handler.wait(len(expected))
        time.sleep(QUIET_PERIOD_SECONDS)
        assert handler.bodies == expected

    def test_an_expression_matching_nothing_delivers_nothing(self, connection, queue, publish, consumers):
        name = queue("con-it-stream-sql-empty", stream=True)
        publish(name, _stream_messages(BATCH))

        handler = RecordingHandler(action=lambda context, message: context.accept())
        consumer = _consume(
            connection,
            consumers,
            name,
            handler,
            stream=lambda stream: (
                stream.offset(StreamOffsetSpecification.FIRST)
                .filter()
                .sql("properties.subject = 'nothing-matches-this'")
                .stream()
            ),
        )
        time.sleep(QUIET_PERIOD_SECONDS)
        assert consumer.is_open, "the broker must accept the attach even when nothing matches"
        assert handler.count == 0, f"the expression was not enforced: {handler.bodies}"
