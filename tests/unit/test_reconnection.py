"""Unit tests for the back-off policy, the topology recorder and the recovery loop."""

from __future__ import annotations

import queue
import threading
import time

import pytest

from rabbitmq_amqp_python_client.connection import (
    Connection,
    ConnectionParameters,
    ConnectionState,
)
from rabbitmq_amqp_python_client.constants import (
    RABBITMQ_ACTIVE_PROPERTY,
    STREAM_OFFSET_ANNOTATION,
    STREAM_OFFSET_SPEC_FILTER,
)
from rabbitmq_amqp_python_client.consumer import StreamOffsetSpecification
from rabbitmq_amqp_python_client.exceptions import AMQPError, ManagementError
from rabbitmq_amqp_python_client.management import (
    ExchangeSpecification,
    QueueInfo,
    QueueSpecification,
)
from rabbitmq_amqp_python_client.reconnection import (
    MULTIPLIER_PERIOD,
    DefaultBackOffDelayPolicy,
    RecordingTopologyListener,
    RecoveryConfiguration,
)
from rabbitmq_amqp_python_client.wire import Attach, Described, Message, MessageAnnotations

STATE_POLL_INTERVAL_SECONDS = 0.005


def wait_for_state(connection: Connection, state: ConnectionState, timeout: float = 5.0) -> bool:
    """Whether ``connection`` reaches ``state`` within ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if connection.state is state:
            return True
        time.sleep(STATE_POLL_INTERVAL_SECONDS)
    return False


def wait_for_reconnect(connection: Connection, broker_farm, *, dials: int = 2, timeout: float = 10.0) -> bool:
    """Whether ``connection`` is back up *after* the farm answered ``dials`` dials.

    Polling only for ``OPEN`` right after a forced drop is the pitfall step_040
    §8.2 warns about: the state still reads ``OPEN`` from before the drop until
    the frame reader notices it. Counting the dials the recovery loop actually
    made makes "recovered" unambiguous.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if broker_farm.dials >= dials and connection.state is ConnectionState.OPEN:
            return True
        time.sleep(STATE_POLL_INTERVAL_SECONDS)
    return False


class FixedDelayPolicy:
    """A minimal custom :class:`BackOffDelayPolicy`, for predictable tests.

    Also exercises the spec's promise that any object with the policy shape is
    accepted in place of the default one.
    """

    def __init__(self, delay: float = 0.05, max_attempts: int = 10):
        self.delay = delay
        self.max_attempts = max_attempts
        self.resets = 0
        self._attempt = 0

    @property
    def current_attempt(self) -> int:
        return self._attempt

    def next_delay(self) -> float:
        self._attempt += 1
        return self.delay

    def reset(self) -> None:
        self._attempt = 0
        self.resets += 1

    def is_active(self) -> bool:
        return self._attempt <= self.max_attempts


class ReplaySpy:
    """Stands in for the topology listener, counting replays instead of sending."""

    def __init__(self):
        self.replays: list[object] = []

    def replay(self, management) -> None:
        self.replays.append(management)


class ManagementRecorder:
    """Records the declares a replay makes, in order, without any wire traffic."""

    def __init__(self, fail_on: tuple[str, ...] = ()):
        self.calls: list[tuple] = []
        self.declared: list[QueueSpecification | ExchangeSpecification] = []
        self._fail_on = set(fail_on)

    def queue(self, name: str = "") -> QueueSpecification:
        return QueueSpecification(self, name)

    def exchange(self, name: str = "") -> ExchangeSpecification:
        return ExchangeSpecification(self, name)

    def _declare_queue(self, specification: QueueSpecification) -> QueueInfo:
        self.calls.append(("queue", specification.queue_name))
        self.declared.append(specification)
        self._maybe_fail(specification.queue_name)
        return QueueInfo(name=specification.queue_name)

    def _declare_exchange(self, specification: ExchangeSpecification) -> None:
        self.calls.append(("exchange", specification.exchange_name))
        self.declared.append(specification)
        self._maybe_fail(specification.exchange_name)

    def bind(self, *, source, destination, binding_key="", arguments=None, to_queue=True) -> None:
        self.calls.append(("binding", source, destination, binding_key, dict(arguments or {}), to_queue))
        self._maybe_fail(destination)

    def _maybe_fail(self, name: str) -> None:
        if name in self._fail_on:
            raise ManagementError(f"the broker refused {name!r}")


def declared_queue(name: str, *, exclusive: bool = False, auto_delete: bool = False, **arguments) -> QueueSpecification:
    """Return a queue specification as it would look right after a declare."""
    specification = QueueSpecification(ManagementRecorder(), name).exclusive(exclusive).auto_delete(auto_delete)
    if arguments:
        specification.arguments(arguments)
    return specification


def declared_exchange(name: str, *, exchange_type: str = "topic") -> ExchangeSpecification:
    """Return an exchange specification as it would look right after a declare."""
    return ExchangeSpecification(ManagementRecorder(), name).type(exchange_type)


class TestDefaultBackOffDelayPolicy:
    def test_starts_on_its_first_attempt(self):
        assert DefaultBackOffDelayPolicy().current_attempt == 1

    def test_the_first_delay_is_half_a_second_of_jitter(self):
        policy = DefaultBackOffDelayPolicy()
        assert 0.5 <= policy.next_delay() < 1.5

    def test_every_delay_stays_inside_the_jitter_band_times_the_multiplier(self):
        policy = DefaultBackOffDelayPolicy(max_attempts=100)
        for _ in range(20):
            multiplier = policy.current_attempt % MULTIPLIER_PERIOD or MULTIPLIER_PERIOD
            delay = policy.next_delay()
            assert 0.5 * multiplier <= delay < 1.5 * multiplier

    def test_counts_one_attempt_per_delay(self):
        policy = DefaultBackOffDelayPolicy()
        policy.next_delay()
        policy.next_delay()
        assert policy.current_attempt == 3

    def test_the_multiplier_grows_then_falls_back_every_five_attempts(self, monkeypatch):
        monkeypatch.setattr("rabbitmq_amqp_python_client.reconnection.random.random", lambda: 0.0)
        policy = DefaultBackOffDelayPolicy(max_attempts=100)
        multipliers = [round(policy.next_delay() / 0.5) for _ in range(12)]
        assert multipliers == [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2]

    def test_custom_bounds_are_honoured(self, monkeypatch):
        monkeypatch.setattr("rabbitmq_amqp_python_client.reconnection.random.random", lambda: 0.5)
        policy = DefaultBackOffDelayPolicy(min_delay=0.1, max_delay=0.3)
        assert policy.next_delay() == pytest.approx(0.2)

    def test_is_active_until_the_attempt_count_reaches_the_maximum(self):
        policy = DefaultBackOffDelayPolicy(max_attempts=3)
        assert policy.is_active()
        policy.next_delay()
        assert policy.is_active() and policy.current_attempt == 2
        policy.next_delay()
        assert not policy.is_active() and policy.current_attempt == 3

    def test_the_default_budget_is_twelve_attempts(self):
        policy = DefaultBackOffDelayPolicy()
        assert policy.max_attempts == 12
        for _ in range(10):
            policy.next_delay()
        assert policy.is_active()
        policy.next_delay()
        assert not policy.is_active()

    def test_reset_restarts_both_the_count_and_the_multiplier(self, monkeypatch):
        monkeypatch.setattr("rabbitmq_amqp_python_client.reconnection.random.random", lambda: 0.0)
        policy = DefaultBackOffDelayPolicy(max_attempts=3)
        policy.next_delay()
        policy.next_delay()
        assert not policy.is_active()
        policy.reset()
        assert policy.current_attempt == 1
        assert policy.is_active()
        assert policy.next_delay() == pytest.approx(0.5)


class TestRecoveryConfiguration:
    def test_defaults_recover_the_connection_but_not_the_topology(self):
        configuration = RecoveryConfiguration()
        assert configuration.activated is True
        assert configuration.topology is False
        assert isinstance(configuration.back_off_delay_policy, DefaultBackOffDelayPolicy)

    def test_each_configuration_gets_its_own_policy(self):
        assert RecoveryConfiguration().back_off_delay_policy is not RecoveryConfiguration().back_off_delay_policy

    def test_a_custom_policy_is_kept(self):
        policy = FixedDelayPolicy()
        assert RecoveryConfiguration(back_off_delay_policy=policy).back_off_delay_policy is policy

    def test_connection_parameters_default_to_recovery_on(self):
        assert ConnectionParameters().recovery_configuration.activated is True


class TestRecordingTopologyListener:
    def test_records_a_declared_queue_with_its_flags_and_arguments(self):
        listener = RecordingTopologyListener()
        listener.record_queue_declared(declared_queue("orders", exclusive=True, **{"x-message-ttl": 1000}))
        (recorded,) = listener.queues
        assert recorded.name == "orders"
        assert recorded.exclusive is True
        assert recorded.auto_delete is False
        assert recorded.arguments == {"x-message-ttl": 1000}

    def test_redeclaring_the_same_queue_records_it_once(self):
        listener = RecordingTopologyListener()
        listener.record_queue_declared(declared_queue("orders"))
        listener.record_queue_declared(declared_queue("orders", auto_delete=True))
        assert [queue.name for queue in listener.queues] == ["orders"]
        assert listener.queues[0].auto_delete is True

    def test_records_a_declared_exchange_with_its_type(self):
        listener = RecordingTopologyListener()
        listener.record_exchange_declared(declared_exchange("events"))
        (recorded,) = listener.exchanges
        assert (recorded.name, recorded.exchange_type) == ("events", "topic")

    def test_records_a_created_binding(self):
        listener = RecordingTopologyListener()
        listener.record_binding_created(source="events", destination="orders", binding_key="order.#")
        (recorded,) = listener.bindings
        assert (recorded.source, recorded.destination, recorded.binding_key) == ("events", "orders", "order.#")
        assert recorded.to_queue is True

    def test_bindings_that_differ_only_by_arguments_are_both_kept(self):
        listener = RecordingTopologyListener()
        listener.record_binding_created(source="e", destination="q", arguments={"x-match": "all"})
        listener.record_binding_created(source="e", destination="q", arguments={"x-match": "any"})
        assert len(listener.bindings) == 2

    def test_a_deleted_queue_is_forgotten(self):
        listener = RecordingTopologyListener()
        listener.record_queue_declared(declared_queue("orders"))
        listener.record_queue_declared(declared_queue("invoices"))
        listener.record_queue_deleted("orders")
        assert [queue.name for queue in listener.queues] == ["invoices"]

    def test_a_deleted_exchange_is_forgotten(self):
        listener = RecordingTopologyListener()
        listener.record_exchange_declared(declared_exchange("events"))
        listener.record_exchange_deleted("events")
        assert listener.exchanges == ()

    def test_deleting_a_queue_forgets_the_bindings_pointing_at_it(self):
        listener = RecordingTopologyListener()
        listener.record_binding_created(source="events", destination="orders")
        listener.record_binding_created(source="events", destination="invoices")
        listener.record_queue_deleted("orders")
        assert [binding.destination for binding in listener.bindings] == ["invoices"]

    def test_deleting_an_exchange_forgets_the_bindings_at_either_end(self):
        listener = RecordingTopologyListener()
        listener.record_binding_created(source="events", destination="orders")
        listener.record_binding_created(source="upstream", destination="events", to_queue=False)
        listener.record_binding_created(source="other", destination="invoices")
        listener.record_exchange_deleted("events")
        assert [binding.destination for binding in listener.bindings] == ["invoices"]

    def test_an_unbound_binding_is_forgotten(self):
        listener = RecordingTopologyListener()
        listener.record_binding_created(source="events", destination="orders", binding_key="k")
        listener.record_binding_deleted(source="events", destination="orders", binding_key="k")
        assert listener.bindings == ()

    def test_unbinding_something_else_keeps_the_recorded_binding(self):
        listener = RecordingTopologyListener()
        listener.record_binding_created(source="events", destination="orders", binding_key="k")
        listener.record_binding_deleted(source="events", destination="orders", binding_key="other")
        assert len(listener.bindings) == 1


class TestTopologyReplay:
    def _populated(self) -> RecordingTopologyListener:
        listener = RecordingTopologyListener()
        listener.record_queue_declared(declared_queue("orders", exclusive=True))
        listener.record_queue_declared(declared_queue("invoices"))
        listener.record_exchange_declared(declared_exchange("events"))
        listener.record_binding_created(source="events", destination="orders", binding_key="order.#")
        return listener

    def test_replays_queues_then_exchanges_then_bindings(self):
        recorder = ManagementRecorder()
        self._populated().replay(recorder)
        assert [call[0] for call in recorder.calls] == ["queue", "queue", "exchange", "binding"]

    def test_replays_each_entity_with_the_recorded_parameters(self):
        recorder = ManagementRecorder()
        listener = RecordingTopologyListener()
        listener.record_queue_declared(declared_queue("orders", exclusive=True, **{"x-message-ttl": 60}))
        listener.record_exchange_declared(declared_exchange("events"))
        listener.record_binding_created(
            source="events", destination="orders", binding_key="k", arguments={"x-match": "all"}
        )
        listener.replay(recorder)
        queue, exchange = recorder.declared
        assert queue.is_exclusive is True
        assert queue.queue_arguments == {"x-message-ttl": 60}
        assert exchange.exchange_type == "topic"
        assert recorder.calls[-1] == ("binding", "events", "orders", "k", {"x-match": "all"}, True)

    def test_nothing_recorded_replays_nothing(self):
        recorder = ManagementRecorder()
        RecordingTopologyListener().replay(recorder)
        assert recorder.calls == []

    def test_a_deleted_entity_is_not_replayed(self):
        listener = self._populated()
        listener.record_queue_deleted("orders")
        recorder = ManagementRecorder()
        listener.replay(recorder)
        assert recorder.calls == [("queue", "invoices"), ("exchange", "events")]

    def test_one_failing_entity_does_not_stop_the_rest(self, caplog):
        recorder = ManagementRecorder(fail_on=("orders",))
        self._populated().replay(recorder)
        assert [call[0] for call in recorder.calls] == ["queue", "queue", "exchange", "binding"]
        assert "could not recreate queue 'orders'" in caplog.text

    def test_a_failing_binding_is_only_logged(self, caplog):
        listener = RecordingTopologyListener()
        listener.record_binding_created(source="events", destination="gone")
        listener.replay(ManagementRecorder(fail_on=("gone",)))
        assert "could not recreate binding 'events' -> 'gone'" in caplog.text


class TestRecoveryDisabled:
    def test_a_dropped_transport_kills_the_connection_at_once(self, broker_farm):
        fired = threading.Event()
        seen: list[BaseException | None] = []

        def on_unexpected_close(error):
            seen.append(error)
            fired.set()

        connection = Connection(
            ConnectionParameters(
                on_unexpected_close=on_unexpected_close,
                recovery_configuration=RecoveryConfiguration(activated=False),
            )
        )
        broker_farm.latest.drop_connection()
        assert fired.wait(5.0)
        assert connection.state is ConnectionState.CLOSED
        assert len(seen) == 1

    def test_no_reconnecting_state_and_no_redial_are_ever_seen(self, broker_farm):
        observed: list[ConnectionState] = []
        stop = threading.Event()
        connection = Connection(ConnectionParameters(recovery_configuration=RecoveryConfiguration(activated=False)))

        def sample():
            while not stop.is_set():
                observed.append(connection.state)
                time.sleep(0.001)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.CLOSED)
        stop.set()
        sampler.join(2.0)
        assert ConnectionState.RECONNECTING not in observed
        assert broker_farm.dials == 1


class TestRecoverySucceeds:
    def _connect(self, broker_farm, *, policy=None, topology=False, on_unexpected_close=None) -> Connection:
        """Open a connection whose recovery loop redials this farm."""
        return Connection(
            ConnectionParameters(
                on_unexpected_close=on_unexpected_close,
                recovery_configuration=RecoveryConfiguration(
                    back_off_delay_policy=policy if policy is not None else FixedDelayPolicy(),
                    topology=topology,
                ),
            )
        )

    def test_the_state_moves_open_reconnecting_open(self, broker_farm):
        connection = self._connect(broker_farm, policy=FixedDelayPolicy(delay=0.15))
        assert connection.state is ConnectionState.OPEN
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.RECONNECTING), "the drop was never noticed"
        assert wait_for_state(connection, ConnectionState.OPEN), "the connection never came back"
        connection.close()

    def test_a_second_attempt_recovers_after_a_refused_redial(self, broker_farm):
        policy = FixedDelayPolicy(delay=0.05)
        connection = self._connect(broker_farm, policy=policy)
        broker_farm.refuse_next(1)
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm, dials=3)
        assert broker_farm.dials == 3, "expected the bootstrap, one refused redial and one that worked"
        assert policy.resets == 1, "a successful reconnect must reset the back-off policy"
        connection.close()

    def test_the_unexpected_close_callback_stays_silent(self, broker_farm):
        seen: list[BaseException | None] = []
        connection = self._connect(broker_farm, on_unexpected_close=seen.append)
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        time.sleep(0.3)
        assert seen == []
        connection.close()

    def test_the_new_transport_gets_its_own_handshake(self, broker_farm):
        connection = self._connect(broker_farm)
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert len(broker_farm.brokers) == 2
        assert broker_farm.latest.remote_open is not None
        assert broker_farm.latest.remote_open.container_id == connection.container_id
        connection.close()

    def test_the_management_pair_is_reattached_on_the_same_instance(self, broker_farm):
        connection = self._connect(broker_farm)
        management = connection.management()
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert management.is_open
        assert connection.management() is management
        connection.close()

    def test_publishers_and_consumers_are_reattached_without_being_rebuilt(self, broker_farm):
        connection = self._connect(broker_farm)
        publisher = connection.publisher_builder().queue("orders").build()
        consumer = connection.consumer_builder().queue("orders").message_handler(lambda *_: None).build()
        publisher_id, consumer_id = publisher.id, consumer.id

        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        # The re-attach is the last step of recovery, so OPEN already implies it.
        assert publisher.is_open, "the publisher's sender link was left detached"
        assert consumer.is_open, "the consumer's receiver link was left detached"
        assert (publisher.id, consumer.id) == (publisher_id, consumer_id)
        attach_names = {performative.name for performative in broker_farm.latest.all_received(Attach)}
        assert {publisher_id, consumer_id} <= attach_names
        connection.close()

    def test_a_reattached_publisher_can_publish_again(self, broker_farm):
        broker_farm.broker_kwargs["initial_credit"] = 10
        connection = self._connect(broker_farm)
        publisher = connection.publisher_builder().queue("orders").build()
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        result = publisher.publish(Message("after the drop"), timeout=5.0)
        assert result.outcome.state.value == "accepted"
        connection.close()

    def test_a_reattached_consumer_receives_again(self, broker_farm):
        received: list[str] = []
        delivered = threading.Event()

        def handler(context, message):
            received.append(message.body_as_string())
            context.accept()
            delivered.set()

        connection = self._connect(broker_farm)
        connection.consumer_builder().queue("orders").message_handler(handler).build()
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        broker = broker_farm.latest
        _channel, attach, _payload = broker.wait_for(Attach)
        broker.send_transfer(0, attach.handle, Message("after the drop").encode(), delivery_id=0)
        assert delivered.wait(5.0)
        assert received == ["after the drop"]
        connection.close()

    def test_a_paused_consumer_is_reattached_without_credit(self, broker_farm):
        connection = self._connect(broker_farm)
        consumer = connection.consumer_builder().queue("orders").message_handler(lambda *_: None).build()
        consumer.pause()
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert consumer.is_open
        assert consumer.is_paused
        assert consumer._link.credit == 0
        connection.close()

    def test_a_single_active_consumer_handler_is_re_registered_on_the_new_link(self, broker_farm):
        """step_090 §3: a promotion is exactly what can change while the client is away."""
        broker_farm.broker_kwargs["receiver_flow_properties"] = {RABBITMQ_ACTIVE_PROPERTY: True}
        states: queue.Queue = queue.Queue()
        connection = self._connect(broker_farm)
        consumer = (
            connection.consumer_builder()
            .queue("orders")
            .quorum()
            .single_active_consumer_state_changed(lambda consumer, is_active: states.put(is_active))
            .builder()
            .message_handler(lambda *_: None)
            .build()
        )
        assert states.get(timeout=5.0) is True

        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert states.get(timeout=5.0) is True, "the new receiver link reported no status"
        assert consumer._link._flow_handler is not None
        connection.close()

    def test_a_stream_consumer_is_reattached_past_the_offset_it_had_reached(self, broker_farm):
        """step_080 §5's gap: recovery must not replay what the handler already saw."""
        delivered = threading.Event()

        def handler(context, message):
            context.accept()
            delivered.set()

        connection = self._connect(broker_farm)
        consumer = (
            connection.consumer_builder()
            .queue("events")
            .stream()
            .offset(StreamOffsetSpecification.FIRST)
            .builder()
            .message_handler(handler)
            .build()
        )
        broker = broker_farm.latest
        _channel, attach, _payload = broker.wait_for(Attach)
        assert attach.source.filter == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, "first")}
        broker.send_transfer(
            0,
            attach.handle,
            Message("replayed", message_annotations=MessageAnnotations({STREAM_OFFSET_ANNOTATION: 12})).encode(),
            delivery_id=0,
        )
        assert delivered.wait(5.0)

        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        _channel, reattach, _payload = broker_farm.latest.wait_for(Attach)
        assert reattach.source.filter == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, 13)}
        assert consumer.last_stream_offset == 12
        connection.close()

    def test_a_stream_consumer_that_received_nothing_keeps_its_original_offset(self, broker_farm):
        connection = self._connect(broker_farm)
        connection.consumer_builder().queue("events").stream().offset("7D").builder().message_handler(
            lambda *_: None
        ).build()
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        _channel, reattach, _payload = broker_farm.latest.wait_for(Attach)
        assert reattach.source.filter == {STREAM_OFFSET_SPEC_FILTER: Described(STREAM_OFFSET_SPEC_FILTER, "7D")}
        connection.close()

    def test_a_link_the_broker_refuses_is_left_detached_alone(self, broker_farm, caplog):
        seen: list[BaseException | None] = []
        connection = self._connect(broker_farm, on_unexpected_close=seen.append)
        publisher = connection.publisher_builder().queue("orders").build()
        broker_farm.configure_next(refuse_attach=True)

        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert not publisher.is_open
        assert "could not re-attach publisher" in caplog.text
        assert seen == [], "a refused link must not be reported as an unexpected close"
        connection.close()

    def test_a_second_drop_is_recovered_too(self, broker_farm):
        connection = self._connect(broker_farm)
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.RECONNECTING)
        assert wait_for_reconnect(connection, broker_farm)
        assert broker_farm.dials == 3
        connection.close()

    def test_a_call_during_the_gap_fails_with_its_own_error(self, broker_farm):
        connection = self._connect(broker_farm, policy=FixedDelayPolicy(delay=0.5))
        publisher = connection.publisher_builder().queue("orders").build()
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.RECONNECTING)
        with pytest.raises(AMQPError):
            publisher.publish(Message("during the gap"), timeout=1.0)
        connection.close()


class TestTopologyRecoveryGate:
    def _connect(self, *, topology: bool) -> Connection:
        return Connection(
            ConnectionParameters(
                recovery_configuration=RecoveryConfiguration(
                    back_off_delay_policy=FixedDelayPolicy(),
                    topology=topology,
                )
            )
        )

    def test_replays_the_recorded_topology_when_the_flag_is_set(self, broker_farm):
        connection = self._connect(topology=True)
        connection.management()
        spy = ReplaySpy()
        connection._topology_listener = spy
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert len(spy.replays) == 1
        connection.close()

    def test_replays_nothing_when_the_flag_is_left_off(self, broker_farm):
        connection = self._connect(topology=False)
        connection.management()
        spy = ReplaySpy()
        connection._topology_listener = spy
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert spy.replays == []
        connection.close()

    def test_nothing_is_replayed_when_management_was_never_used(self, broker_farm):
        connection = self._connect(topology=True)
        spy = ReplaySpy()
        connection._topology_listener = spy
        broker_farm.latest.drop_connection()
        assert wait_for_reconnect(connection, broker_farm)
        assert spy.replays == []
        connection.close()

    def test_the_listener_records_through_management_whatever_the_flag(self, broker_farm):
        connection = self._connect(topology=False)
        management = connection.management()
        assert management._topology_listener is connection.topology_listener
        connection.close()


class TestGiveUp:
    def test_the_connection_dies_once_the_policy_stops_being_active(self, broker_farm):
        seen: list[BaseException | None] = []
        policy = FixedDelayPolicy(delay=0.02, max_attempts=2)
        connection = Connection(
            ConnectionParameters(
                on_unexpected_close=seen.append,
                recovery_configuration=RecoveryConfiguration(back_off_delay_policy=policy),
            )
        )
        broker_farm.refuse_next(10)
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.CLOSED, timeout=10.0)
        time.sleep(0.2)
        assert len(seen) == 1, "on_unexpected_close must fire exactly once when recovery gives up"
        assert broker_farm.dials == 3, "the policy allowed two redials before giving up"
        connection.close()
        assert len(seen) == 1

    def test_an_unexpected_failure_inside_recovery_still_gives_up(self, broker_farm, monkeypatch):
        seen: list[BaseException | None] = []
        connection = Connection(
            ConnectionParameters(
                on_unexpected_close=seen.append,
                recovery_configuration=RecoveryConfiguration(back_off_delay_policy=FixedDelayPolicy(delay=0.02)),
            )
        )

        def explode() -> None:
            raise RuntimeError("something nobody planned for")

        monkeypatch.setattr(connection, "_recover_endpoints", explode)
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.CLOSED, timeout=10.0), "left stuck in RECONNECTING"
        assert len(seen) == 1

    def test_giving_up_leaves_the_connection_unusable(self, broker_farm):
        connection = Connection(
            ConnectionParameters(
                recovery_configuration=RecoveryConfiguration(
                    back_off_delay_policy=FixedDelayPolicy(delay=0.02, max_attempts=1)
                )
            )
        )
        broker_farm.refuse_next(10)
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.CLOSED, timeout=10.0)
        assert not connection.is_open
        with pytest.raises(AMQPError):
            connection.open_session()


class TestCloseDuringRecovery:
    def test_close_cancels_the_back_off_wait_instead_of_waiting_it_out(self, broker_farm):
        connection = Connection(
            ConnectionParameters(
                recovery_configuration=RecoveryConfiguration(
                    back_off_delay_policy=FixedDelayPolicy(delay=30.0),
                )
            )
        )
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.RECONNECTING)
        started = time.monotonic()
        connection.close()
        elapsed = time.monotonic() - started
        assert connection.state is ConnectionState.CLOSED
        assert elapsed < 3.0, f"close() waited {elapsed:.1f}s for the recovery loop"
        assert broker_farm.dials == 1, "a cancelled loop must not redial"

    def test_close_while_recovering_still_forgets_publishers_and_consumers(self, broker_farm):
        connection = Connection(
            ConnectionParameters(
                recovery_configuration=RecoveryConfiguration(
                    back_off_delay_policy=FixedDelayPolicy(delay=30.0),
                )
            )
        )
        publisher = connection.publisher_builder().queue("orders").build()
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.RECONNECTING)
        connection.close()
        assert connection._publishers == {}
        assert not publisher.is_open

    def test_close_is_still_idempotent_after_cancelling_recovery(self, broker_farm):
        connection = Connection(
            ConnectionParameters(
                recovery_configuration=RecoveryConfiguration(
                    back_off_delay_policy=FixedDelayPolicy(delay=30.0),
                )
            )
        )
        broker_farm.latest.drop_connection()
        assert wait_for_state(connection, ConnectionState.RECONNECTING)
        connection.close()
        connection.close()
        assert connection.state is ConnectionState.CLOSED
