"""The performance test example's own logic, exercised without a broker.

Everything the example does apart from the four network calls — connect, declare,
attach, publish — is a pure function or a small object over :class:`Counters`/
:class:`LatencyAccumulator`, so the whole option-parsing/counting/timing/
formatting/exit-code surface is unit-testable here (§11's unit-test list).
``docs/examples`` is on ``pythonpath`` (see ``pyproject.toml``), which is why
``performance_test`` imports as a plain module.
"""

from __future__ import annotations

import io
import threading
from types import SimpleNamespace
from typing import cast

import performance_test as app
import pytest

from rabbitmq_amqp_python_client import (
    AMQPError,
    AMQPTimeoutError,
    Management,
    Outcome,
    OutcomeState,
    ProtocolError,
    PublisherError,
    PublishResult,
    QueueSpecification,
    QueueType,
)
from rabbitmq_amqp_python_client.management import ARG_QUEUE_TYPE
from rabbitmq_amqp_python_client.wire import ApplicationProperties, Long, Message

MILLISECOND_NS = 1_000_000


@pytest.fixture
def counters() -> app.Counters:
    """A fresh, all-zero set of counters."""
    return app.Counters()


@pytest.fixture
def warning() -> app.WarnOnce:
    """A one-shot warning for the missing-timestamp skip path."""
    return app.WarnOnce("no %s on this delivery")


def specification(name: str = "a-queue") -> QueueSpecification:
    """A real specification with no management endpoint behind it.

    Only :meth:`~rabbitmq_amqp_python_client.QueueSpecification.declare` touches
    the endpoint, and nothing here declares.
    """
    return QueueSpecification(cast(Management, None), name)


def snapshot(**counts: int) -> app.CounterSnapshot:
    """A snapshot with the named counters set and the rest at zero."""
    return app.CounterSnapshot(**counts)


class FakeClock:
    """A monotonic clock the tests advance by hand.

    Substituted for the module's ``time`` so a tick's elapsed interval — and
    therefore the rate it computes — is exact instead of whatever the test machine
    happened to take.
    """

    def __init__(self, now: float = 0.0) -> None:
        self.now = now
        self.now_ns = int(now * 1e9)

    def monotonic(self) -> float:
        """Return the current reading, in seconds."""
        return self.now

    def monotonic_ns(self) -> int:
        """Return the current reading, in nanoseconds."""
        return self.now_ns

    def advance(self, seconds: float) -> None:
        """Move the clock forward."""
        self.now += seconds
        self.now_ns += int(seconds * 1e9)


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    """Replace the module's clock with one the test drives."""
    fake = FakeClock()
    monkeypatch.setattr(app, "time", SimpleNamespace(monotonic=fake.monotonic, monotonic_ns=fake.monotonic_ns))
    return fake


# --- option parsing (§2, §8) --------------------------------------------


class TestParseArgs:
    """Defaults, every flag, and the combinations §8 rejects."""

    def test_defaults_match_the_spec_table(self):
        options = app.parse_args([])

        assert options.messages == 1_000_000
        assert options.queue_type == "classic"
        assert options.keep_queue is False
        assert options.message_size == 16
        assert options.initial_credits == 1000
        assert options.consume_timeout == 30.0
        assert options.publish_timeout == 5.0
        assert options.stats_interval == 1.0
        assert options.latency_window_size == 10_000
        assert options.host == "localhost"
        assert options.port is None
        assert options.user == "guest"
        assert options.password == "guest"
        assert options.virtual_host == "/"
        assert options.tls is False

    def test_the_initial_credits_default_overrides_the_clients_own(self):
        from rabbitmq_amqp_python_client.consumer import DEFAULT_INITIAL_CREDITS as CLIENT_DEFAULT

        assert app.DEFAULT_INITIAL_CREDITS == 1000
        assert app.DEFAULT_INITIAL_CREDITS != CLIENT_DEFAULT

    def test_default_queue_name_is_generated_and_unique(self):
        first = app.parse_args([]).queue
        second = app.parse_args([]).queue

        assert first.startswith(app.GENERATED_QUEUE_PREFIX)
        assert first != second

    def test_messages_short_flag(self):
        assert app.parse_args(["-n", "7"]).messages == 7

    @pytest.mark.parametrize("queue_type", ["classic", "quorum", "stream"])
    def test_every_queue_type_is_accepted(self, queue_type):
        assert app.parse_args(["--queue-type", queue_type]).queue_type == queue_type

    def test_unknown_queue_type_is_refused_by_argparse(self):
        with pytest.raises(SystemExit):
            app.parse_args(["--queue-type", "lazy"])

    def test_every_remaining_flag(self):
        options = app.parse_args(
            [
                "--messages",
                "12",
                "--queue-type",
                "stream",
                "--queue",
                "my-queue",
                "--keep-queue",
                "--message-size",
                "1024",
                "--initial-credits",
                "50",
                "--consume-timeout",
                "2.5",
                "--publish-timeout",
                "0.5",
                "--stats-interval",
                "0.25",
                "--latency-window-size",
                "100",
                "--host",
                "broker.example",
                "--port",
                "5671",
                "--user",
                "alice",
                "--password",
                "s3cret",
                "--vhost",
                "/staging",
                "--tls",
            ]
        )

        assert options == app.Options(
            messages=12,
            queue_type="stream",
            queue="my-queue",
            keep_queue=True,
            message_size=1024,
            initial_credits=50,
            consume_timeout=2.5,
            publish_timeout=0.5,
            stats_interval=0.25,
            latency_window_size=100,
            host="broker.example",
            port=5671,
            user="alice",
            password="s3cret",
            virtual_host="/staging",
            tls=True,
        )

    def test_zero_messages_is_rejected(self):
        with pytest.raises(app.OptionsError, match="--messages must be > 0"):
            app.parse_args(["--messages", "0"])

    def test_zero_latency_window_size_is_rejected(self):
        with pytest.raises(app.OptionsError, match="--latency-window-size must be > 0"):
            app.parse_args(["--latency-window-size", "0"])

    @pytest.mark.parametrize(
        ("argv", "expected"),
        [
            (["-n", "-1"], "--messages must be > 0"),
            (["--latency-window-size", "-5"], "--latency-window-size must be > 0"),
            (["--message-size", "-1"], "--message-size must be >= 0"),
            (["--initial-credits", "0"], "--initial-credits must be > 0"),
            (["--consume-timeout", "-1"], "--consume-timeout must be >= 0"),
            (["--publish-timeout", "0"], "--publish-timeout must be > 0"),
            (["--stats-interval", "0"], "--stats-interval must be > 0"),
        ],
    )
    def test_invalid_values_are_rejected(self, argv, expected):
        with pytest.raises(app.OptionsError, match=expected):
            app.parse_args(argv)

    def test_a_zero_message_size_is_allowed(self):
        assert app.parse_args(["--message-size", "0"]).message_size == 0

    def test_a_zero_consume_timeout_is_allowed(self):
        assert app.parse_args(["--consume-timeout", "0"]).consume_timeout == 0.0

    @pytest.mark.parametrize("argv", [["--messages", "0"], ["--latency-window-size", "0"]])
    def test_the_rejected_combinations_never_reach_the_broker(self, monkeypatch, capsys, argv):
        """§8 rows 1-2: rejected at startup, before connecting, with no summary."""

        def refuse_to_connect(parameters):
            raise AssertionError("main() connected despite invalid options")

        monkeypatch.setattr(app, "Connection", refuse_to_connect)

        assert app.main(argv) == app.EXIT_INVALID_OPTIONS
        assert "Messages sent:" not in capsys.readouterr().out


# --- counters (§5) ------------------------------------------------------


class TestCounters:
    """Every increment, and thread safety."""

    def test_a_fresh_set_is_all_zero(self, counters):
        assert counters.snapshot() == app.CounterSnapshot()

    def test_every_recorder_touches_exactly_its_own_counter(self, counters):
        counters.record_sent()
        counters.record_confirmed()
        counters.record_not_confirmed()
        counters.record_consumed()

        assert counters.snapshot() == app.CounterSnapshot(
            messages_sent=1,
            messages_confirmed=1,
            messages_not_confirmed=1,
            messages_consumed=1,
        )

    def test_concurrent_increments_are_not_lost(self, counters):
        def bump() -> None:
            for _ in range(500):
                counters.record_sent()
                counters.record_consumed()

        threads = [threading.Thread(target=bump) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert counters.snapshot() == app.CounterSnapshot(messages_sent=2000, messages_consumed=2000)


# --- throughput formulas (§5.1, §5.2) -----------------------------------


class TestThroughputFormulas:
    """§5's two shared functions, fed the synthetic inputs §11 asks for."""

    @pytest.mark.parametrize(
        ("count_now", "count_previous", "elapsed", "expected"),
        [
            (1000, 0, 1.0, 1000.0),
            (1500, 1000, 0.5, 1000.0),
            (1000, 1000, 2.0, 0.0),
            (300, 100, 4.0, 50.0),
        ],
    )
    def test_the_live_rate_is_a_diff_over_the_interval(self, count_now, count_previous, elapsed, expected):
        assert app.instantaneous_rate(count_now, count_previous, elapsed) == expected

    @pytest.mark.parametrize("elapsed", [0.0, -1.0])
    def test_the_live_rate_refuses_to_divide_by_a_non_positive_interval(self, elapsed):
        assert app.instantaneous_rate(500, 0, elapsed) == 0.0

    @pytest.mark.parametrize(
        ("count", "elapsed", "expected"),
        [(1000, 2.0, 500.0), (0, 5.0, 0.0), (7, 0.5, 14.0)],
    )
    def test_the_overall_rate_is_a_count_over_the_whole_elapsed_time(self, count, elapsed, expected):
        assert app.overall_rate(count, elapsed) == expected

    @pytest.mark.parametrize("elapsed", [0.0, -1.0])
    def test_the_overall_rate_refuses_to_divide_by_a_non_positive_elapsed_time(self, elapsed):
        assert app.overall_rate(1000, elapsed) == 0.0

    @pytest.mark.parametrize(
        ("figure", "count", "elapsed", "expected"),
        [
            # §5.1: the sent figure stops at the publish loop's own end...
            ("sent", 1000, 2.0, 500.0),
            # ...while §5.2's consumed figure runs on to the end of the drain
            # wait, so the same 1000 messages divide by a longer elapsed time.
            ("consumed", 1000, 4.0, 250.0),
        ],
    )
    def test_both_families_share_the_math_and_differ_only_in_their_end_instant(self, figure, count, elapsed, expected):
        start_time = 100.0
        publish_end = start_time + 2.0
        drain_end = start_time + 4.0
        end = publish_end if figure == "sent" else drain_end

        assert app.overall_rate(count, end - start_time) == expected
        assert elapsed == end - start_time


# --- rolling latency window (§6.1, §6.2) --------------------------------


class TestLatencyAccumulator:
    """§6.1's reset boundary, its fallback, and §6.2's never-reset totals."""

    def test_a_zero_sized_window_is_refused(self):
        with pytest.raises(ValueError, match="window size must be > 0"):
            app.LatencyAccumulator(0)

    def test_a_fresh_accumulator_has_no_average_at_all(self):
        accumulator = app.LatencyAccumulator(10)

        assert accumulator.current_average_ns() is None
        assert accumulator.overall_average_ns() is None
        assert accumulator.snapshot() == app.LatencySnapshot()

    def test_the_window_resets_at_exactly_the_window_size_and_not_one_sample_before(self):
        accumulator = app.LatencyAccumulator(4)

        for _ in range(3):
            accumulator.record(2 * MILLISECOND_NS)
        before = accumulator.snapshot()

        accumulator.record(6 * MILLISECOND_NS)
        at_the_boundary = accumulator.snapshot()

        # One sample short: nothing has reset and no window average exists yet.
        assert (before.window_count, before.completed_windows) == (3, 0)
        assert before.last_window_average_ns is None
        assert before.window_sum_ns == 6 * MILLISECOND_NS
        # Exactly at the boundary: sum and count are both back to zero, together.
        assert (at_the_boundary.window_count, at_the_boundary.window_sum_ns) == (0, 0)
        assert at_the_boundary.completed_windows == 1
        assert at_the_boundary.last_window_average_ns == 3 * MILLISECOND_NS

    def test_the_sample_after_the_boundary_starts_a_fresh_window(self):
        accumulator = app.LatencyAccumulator(2)
        accumulator.record(1 * MILLISECOND_NS)
        accumulator.record(3 * MILLISECOND_NS)

        accumulator.record(10 * MILLISECOND_NS)
        after = accumulator.snapshot()

        assert (after.window_count, after.window_sum_ns) == (1, 10 * MILLISECOND_NS)
        assert after.completed_windows == 1
        assert after.last_window_average_ns == 2 * MILLISECOND_NS
        assert after.current_average_ns == 10 * MILLISECOND_NS

    def test_the_reported_average_falls_back_to_the_last_window_right_after_a_reset(self):
        accumulator = app.LatencyAccumulator(2)

        accumulator.record(4 * MILLISECOND_NS)
        accumulator.record(6 * MILLISECOND_NS)

        # The live window is empty, so the fallback stands in for it rather than
        # a misleading 0ms (§6.1's closing formula).
        assert accumulator.snapshot().window_count == 0
        assert accumulator.current_average_ns() == 5 * MILLISECOND_NS

    def test_a_run_shorter_than_one_window_reads_the_live_sum_and_count(self):
        accumulator = app.LatencyAccumulator(10_000)

        for sample in (1 * MILLISECOND_NS, 2 * MILLISECOND_NS, 3 * MILLISECOND_NS):
            accumulator.record(sample)
        current = accumulator.snapshot()

        assert current.completed_windows == 0
        assert current.last_window_average_ns is None
        assert current.window_count == 3
        assert accumulator.current_average_ns() == 2 * MILLISECOND_NS
        assert accumulator.overall_average_ns() == 2 * MILLISECOND_NS

    def test_the_overall_accumulators_survive_every_reset(self):
        accumulator = app.LatencyAccumulator(10)

        for sample in range(1, 26):
            accumulator.record(sample * MILLISECOND_NS)
        current = accumulator.snapshot()

        assert current.completed_windows == 2
        assert current.overall_count == 25
        assert current.overall_sum_ns == sum(range(1, 26)) * MILLISECOND_NS
        assert accumulator.overall_average_ns() == 13 * MILLISECOND_NS

    def test_the_window_size_is_readable(self):
        assert app.LatencyAccumulator(123).window_size == 123

    def test_concurrent_samples_are_all_accounted_for(self):
        accumulator = app.LatencyAccumulator(7)

        def record() -> None:
            for _ in range(100):
                accumulator.record(MILLISECOND_NS)

        threads = [threading.Thread(target=record) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        current = accumulator.snapshot()

        assert current.overall_count == 400
        assert current.completed_windows == 400 // 7
        assert current.window_count == 400 % 7


# --- message payload (§3) -----------------------------------------------


class TestMessagePayload:
    """The stamped timestamp, the filler body, and reading the stamp back."""

    @pytest.mark.parametrize("size", [0, 1, 16, 4096])
    def test_the_body_is_padded_to_the_requested_size(self, size):
        assert len(app.build_payload(size)) == size

    def test_the_timestamp_travels_as_a_typed_application_property(self):
        message = app.build_message(app.build_payload(16), 1_234_567_890_123)

        assert message.application_properties is not None
        stamped = message.application_properties.value[app.SEND_TIMESTAMP_PROPERTY]
        assert stamped == 1_234_567_890_123
        assert isinstance(stamped, Long)
        assert message.body_as_bytes() == app.build_payload(16)

    def test_the_stamp_survives_an_encode_decode_round_trip(self):
        original = app.build_message(app.build_payload(8), 987_654_321)

        decoded = Message.decode(original.encode())

        assert app.read_send_timestamp(decoded) == 987_654_321

    def test_a_small_stamp_is_still_pinned_to_a_signed_64_bit_long(self):
        """A machine only just booted would otherwise stamp an ``int``."""
        decoded = Message.decode(app.build_message(b"", 5).encode())

        assert app.read_send_timestamp(decoded) == 5

    @pytest.mark.parametrize(
        "message",
        [
            Message("no properties at all"),
            Message("empty properties", application_properties=ApplicationProperties({})),
            Message("another key", application_properties=ApplicationProperties({"x-other": 1})),
            Message("not a number", application_properties=ApplicationProperties({"x-send-timestamp": "soon"})),
            Message("a null", application_properties=ApplicationProperties({"x-send-timestamp": None})),
            Message("a boolean", application_properties=ApplicationProperties({"x-send-timestamp": True})),
        ],
    )
    def test_a_missing_or_undecodable_stamp_reads_as_none(self, message):
        assert app.read_send_timestamp(message) is None


# --- delivery accounting (§3 point 4, §4.3) -----------------------------


class TestRecordDelivery:
    """Latency when the stamp is usable, the count always, and one warning."""

    def test_a_stamped_delivery_feeds_both_accumulators(self, counters, warning, clock):
        latency = app.LatencyAccumulator(10)
        message = app.build_message(b"", clock.monotonic_ns())
        clock.advance(0.005)

        app.record_delivery(message, counters, latency, warning)

        assert counters.snapshot() == snapshot(messages_consumed=1)
        assert latency.snapshot().overall_count == 1
        assert latency.overall_average_ns() == pytest.approx(5 * MILLISECOND_NS)
        assert warning.has_logged is False

    def test_an_unstamped_delivery_still_counts_but_skips_latency(self, counters, warning, caplog):
        latency = app.LatencyAccumulator(10)

        with caplog.at_level("WARNING"):
            app.record_delivery(Message("no stamp"), counters, latency, warning)

        assert counters.snapshot() == snapshot(messages_consumed=1)
        assert latency.snapshot() == app.LatencySnapshot()
        assert latency.overall_average_ns() is None
        assert len(caplog.records) == 1

    def test_the_warning_is_logged_once_for_the_whole_run(self, counters, warning, caplog):
        latency = app.LatencyAccumulator(10)

        with caplog.at_level("WARNING"):
            for _ in range(5):
                app.record_delivery(Message("no stamp"), counters, latency, warning)

        assert counters.snapshot() == snapshot(messages_consumed=5)
        assert latency.snapshot().overall_count == 0
        assert len(caplog.records) == 1

    def test_stamped_and_unstamped_deliveries_mix_without_losing_either_number(self, counters, warning, clock):
        latency = app.LatencyAccumulator(10)
        stamped = app.build_message(b"", clock.monotonic_ns())
        clock.advance(0.002)

        app.record_delivery(Message("no stamp"), counters, latency, warning)
        app.record_delivery(stamped, counters, latency, warning)

        assert counters.snapshot() == snapshot(messages_consumed=2)
        assert latency.snapshot().overall_count == 1


class TestWarnOnce:
    """One line per run, whichever thread asks for it."""

    def test_only_the_first_call_logs(self, caplog):
        once = app.WarnOnce("something happened to %s")

        with caplog.at_level("WARNING"):
            first = once.warn("this")
            second = once.warn("that")

        assert (first, second) == (True, False)
        assert once.has_logged is True
        assert [record.getMessage() for record in caplog.records] == ["something happened to this"]

    def test_concurrent_callers_still_only_log_once(self, caplog):
        once = app.WarnOnce("concurrently %s")
        logged: list[bool] = []
        lock = threading.Lock()

        def call() -> None:
            result = once.warn("now")
            with lock:
                logged.append(result)

        threads = [threading.Thread(target=call) for _ in range(8)]
        with caplog.at_level("WARNING"):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        assert logged.count(True) == 1
        assert len(caplog.records) == 1


# --- outcome classification (§4.5 points 3-4) ---------------------------


class TestClassifyOutcome:
    """Two buckets, not three: ``rejected`` and ``released`` fold together."""

    def test_accepted_is_confirmed(self, counters):
        app.classify_outcome(Outcome(state=OutcomeState.ACCEPTED), counters)

        assert counters.snapshot() == snapshot(messages_confirmed=1)

    @pytest.mark.parametrize("state", [OutcomeState.REJECTED, OutcomeState.RELEASED])
    def test_rejected_and_released_land_in_the_same_bucket(self, counters, state, caplog):
        with caplog.at_level("WARNING"):
            app.classify_outcome(Outcome(state=state), counters)

        assert counters.snapshot() == snapshot(messages_not_confirmed=1)
        assert state.value in caplog.text

    def test_an_unmodelled_state_is_not_confirmed_either(self, counters):
        app.classify_outcome(Outcome(state=cast(OutcomeState, "modified")), counters)

        assert counters.snapshot() == snapshot(messages_not_confirmed=1)


class FakePublisher:
    """A publisher whose ``publish`` replays a scripted list of results.

    Each entry is either an :class:`Outcome` to return or an exception to raise.
    """

    def __init__(self, script: list[object]) -> None:
        self.script = list(script)
        self.timeouts: list[float | None] = []
        self.messages: list[Message] = []

    def publish(self, message: Message, timeout: float | None = None) -> PublishResult:
        """Return or raise the next scripted entry."""
        self.timeouts.append(timeout)
        self.messages.append(message)
        entry = self.script.pop(0)
        if isinstance(entry, BaseException):
            raise entry
        return PublishResult(message=message, outcome=cast(Outcome, entry))


class TestPublishAll:
    """The publish loop's counting and stamping contract (§4.5)."""

    def test_every_attempt_is_sent_stamped_classified_and_sized(self, counters):
        publisher = FakePublisher([Outcome(state=OutcomeState.ACCEPTED)] * 3)
        options = app.Options(messages=3, message_size=32, publish_timeout=0.75)

        app.publish_all(cast(app.Publisher, publisher), options, counters)

        assert counters.snapshot() == snapshot(messages_sent=3, messages_confirmed=3)
        assert publisher.timeouts == [0.75, 0.75, 0.75]
        assert [len(message.body_as_bytes()) for message in publisher.messages] == [32, 32, 32]
        stamps = [app.read_send_timestamp(message) for message in publisher.messages]
        assert all(stamp is not None for stamp in stamps)
        assert stamps == sorted(cast(list[int], stamps))

    @pytest.mark.parametrize(
        "error",
        [
            AMQPTimeoutError("no disposition"),
            PublisherError("closed"),
            # A connection that died mid-run raises neither of the two named
            # types, and must not abort the loop either (§4.5 point 4).
            ProtocolError("the connection is gone"),
            AMQPError("something else entirely"),
        ],
    )
    def test_a_raised_publish_is_not_confirmed_and_the_loop_continues(self, counters, error, caplog):
        publisher = FakePublisher([error, Outcome(state=OutcomeState.ACCEPTED)])

        with caplog.at_level("WARNING"):
            app.publish_all(cast(app.Publisher, publisher), app.Options(messages=2), counters)

        assert counters.snapshot() == snapshot(messages_sent=2, messages_confirmed=1, messages_not_confirmed=1)
        assert "publishing message 0 failed" in caplog.text

    def test_a_zero_sized_body_is_published_as_an_empty_one(self, counters):
        publisher = FakePublisher([Outcome(state=OutcomeState.ACCEPTED)])

        app.publish_all(cast(app.Publisher, publisher), app.Options(messages=1, message_size=0), counters)

        assert publisher.messages[0].body_as_bytes() == b""
        assert app.read_send_timestamp(publisher.messages[0]) is not None


# --- output (§7) --------------------------------------------------------


EXPECTED_SUMMARY = """\
Messages sent:                 1,000
Messages confirmed:            995
Messages not confirmed:        5
Messages consumed:             990
Messages sent/sec:             12,345.7
Messages consumed/sec:         11,000.5
Avg latency (ms):              2.500"""


class TestFormatSummary:
    """§7's block: exact labels, exact order, values at a fixed column."""

    def test_the_block_matches_the_spec_line_for_line(self):
        rendered = app.format_summary(
            snapshot(
                messages_sent=1000,
                messages_confirmed=995,
                messages_not_confirmed=5,
                messages_consumed=990,
            ),
            sent_per_second=12_345.67,
            consumed_per_second=11_000.5,
            average_latency_ns=2.5 * MILLISECOND_NS,
        )

        assert rendered == EXPECTED_SUMMARY

    def test_an_all_zero_block_still_carries_every_line_in_order(self):
        lines = app.format_summary(snapshot(), 0.0, 0.0, None).splitlines()

        assert [line.split(":")[0] + ":" for line in lines] == [
            "Messages sent:",
            "Messages confirmed:",
            "Messages not confirmed:",
            "Messages consumed:",
            "Messages sent/sec:",
            "Messages consumed/sec:",
            "Avg latency (ms):",
        ]

    def test_a_run_that_sampled_no_latency_prints_n_a(self):
        rendered = app.format_summary(snapshot(messages_sent=1), 1.0, 1.0, None)

        assert rendered.splitlines()[-1] == f"{'Avg latency (ms):':<{app.LABEL_WIDTH}}n/a"

    def test_every_value_starts_at_the_same_column(self):
        for line in app.format_summary(snapshot(messages_sent=1), 2.0, 3.0, 1.0).splitlines():
            assert line[app.LABEL_WIDTH - 1] == " "
            assert line[app.LABEL_WIDTH] != " "

    @pytest.mark.parametrize(
        ("average_ns", "expected"),
        [(None, "n/a"), (0, "0.000"), (1_500_000, "1.500"), (12_345_678, "12.346")],
    )
    def test_latency_is_rendered_in_milliseconds(self, average_ns, expected):
        assert app.format_latency_ms(average_ns) == expected


# --- periodic printer (§4.6, §5) ----------------------------------------


class TestStatsPrinter:
    """The first-tick skip, the live rates, and the rolling latency it reads."""

    def printer(self, counters: app.Counters, latency: app.LatencyAccumulator) -> tuple[app.StatsPrinter, io.StringIO]:
        """Return a printer writing to a buffer the test can read."""
        stream = io.StringIO()
        return app.StatsPrinter(counters, latency, interval=0.01, stream=stream), stream

    def test_the_very_first_tick_is_skipped_for_both_figures_together(self, counters, clock):
        latency = app.LatencyAccumulator(10)
        printer, stream = self.printer(counters, latency)
        for _ in range(10):
            counters.record_sent()
            counters.record_consumed()

        printed = printer.print_tick()

        assert printed is False
        assert stream.getvalue() == ""

    def test_the_second_tick_reports_both_live_rates_off_the_first_ones_baseline(self, counters, clock):
        latency = app.LatencyAccumulator(10)
        printer, stream = self.printer(counters, latency)
        for _ in range(100):
            counters.record_sent()
        for _ in range(40):
            counters.record_consumed()
        printer.print_tick()

        clock.advance(2.0)
        for _ in range(300):
            counters.record_sent()
        for _ in range(160):
            counters.record_consumed()
        printed = printer.print_tick()

        assert printed is True
        lines = stream.getvalue().splitlines()
        # (400 - 100) / 2s and (200 - 40) / 2s: instantaneous, not cumulative.
        assert lines[5] == f"{'Messages sent/sec:':<{app.LABEL_WIDTH}}150.0"
        assert lines[6] == f"{'Messages consumed/sec:':<{app.LABEL_WIDTH}}80.0"
        assert lines[1] == f"{'Messages sent:':<{app.LABEL_WIDTH}}400"

    def test_a_tick_carries_an_elapsed_time_prefix(self, counters, clock):
        printer, stream = self.printer(counters, app.LatencyAccumulator(10))
        printer.start()
        printer.stop()
        printer.print_tick()
        clock.advance(3.0)
        printer.print_tick()

        assert stream.getvalue().splitlines()[0] == "--- 3.0s elapsed ---"

    def test_it_reads_the_live_window_while_no_window_has_completed_yet(self, counters, clock):
        latency = app.LatencyAccumulator(10_000)
        printer, stream = self.printer(counters, latency)
        printer.print_tick()
        latency.record(2 * MILLISECOND_NS)
        latency.record(4 * MILLISECOND_NS)
        clock.advance(1.0)

        printer.print_tick()

        assert stream.getvalue().splitlines()[7] == f"{'Avg latency (ms):':<{app.LABEL_WIDTH}}3.000"

    def test_it_falls_back_to_the_last_window_right_after_a_reset(self, counters, clock):
        latency = app.LatencyAccumulator(2)
        printer, stream = self.printer(counters, latency)
        printer.print_tick()
        latency.record(2 * MILLISECOND_NS)
        latency.record(8 * MILLISECOND_NS)
        clock.advance(1.0)

        printer.print_tick()

        assert latency.snapshot().window_count == 0
        assert stream.getvalue().splitlines()[7] == f"{'Avg latency (ms):':<{app.LABEL_WIDTH}}5.000"

    def test_it_prints_n_a_before_the_first_delivery(self, counters, clock):
        printer, stream = self.printer(counters, app.LatencyAccumulator(10))
        printer.print_tick()
        clock.advance(1.0)

        printer.print_tick()

        assert stream.getvalue().splitlines()[7] == f"{'Avg latency (ms):':<{app.LABEL_WIDTH}}n/a"

    def test_it_never_mutates_what_it_reads(self, counters, clock):
        latency = app.LatencyAccumulator(10)
        latency.record(MILLISECOND_NS)
        counters.record_sent()
        printer, _ = self.printer(counters, latency)

        printer.print_tick()
        clock.advance(1.0)
        printer.print_tick()

        assert counters.snapshot() == snapshot(messages_sent=1)
        assert latency.snapshot() == app.LatencySnapshot(
            window_sum_ns=MILLISECOND_NS, window_count=1, overall_sum_ns=MILLISECOND_NS, overall_count=1
        )

    def test_a_broken_stream_does_not_kill_the_thread(self, counters, clock, caplog):
        class BrokenStream(io.StringIO):
            def write(self, _data: str) -> int:
                raise OSError("broken pipe")

        printer = app.StatsPrinter(counters, app.LatencyAccumulator(10), interval=0.01, stream=BrokenStream())
        printer.print_tick()
        clock.advance(1.0)

        with caplog.at_level("WARNING"):
            assert printer.print_tick() is True

        assert "could not print the periodic stats" in caplog.text

    def test_start_and_stop_are_idempotent(self, counters):
        printer, _ = self.printer(counters, app.LatencyAccumulator(10))

        printer.start()
        printer.start()
        assert printer.is_running is True
        printer.stop()
        printer.stop()

        assert printer.is_running is False

    def test_a_running_printer_ticks_on_its_own_thread(self, counters):
        printer, stream = self.printer(counters, app.LatencyAccumulator(10))
        counters.record_sent()

        printer.start()
        deadline = threading.Event()
        while not stream.getvalue() and not deadline.wait(0.01):
            counters.record_sent()
            if stream.getvalue():
                break
        printer.stop()

        assert "Messages sent/sec:" in stream.getvalue()


# --- drain wait (§4.7) --------------------------------------------------


class TestWaitForConsumption:
    """The two ways §4.7's wait can return."""

    def test_it_returns_at_once_when_consumption_already_caught_up(self, counters):
        counters.record_sent()
        counters.record_consumed()

        assert app.wait_for_consumption(counters, timeout=5.0) is True

    def test_it_returns_once_the_delivery_loop_catches_up(self, counters):
        counters.record_sent()
        threading.Timer(0.05, counters.record_consumed).start()

        assert app.wait_for_consumption(counters, timeout=5.0) is True

    def test_it_gives_up_when_the_timeout_elapses(self, counters, caplog):
        counters.record_sent()

        with caplog.at_level("WARNING"):
            caught_up = app.wait_for_consumption(counters, timeout=0.05)

        assert caught_up is False
        assert "gave up" in caplog.text


# --- exit codes (§8) ----------------------------------------------------


class TestDecideExitCode:
    """One case per row of §8's table."""

    def test_a_fully_confirmed_and_consumed_run_exits_zero(self, counters):
        for _ in range(3):
            counters.record_sent()
            counters.record_confirmed()
            counters.record_consumed()

        assert app.decide_exit_code(counters.snapshot(), caught_up=True) == app.EXIT_OK

    def test_any_unconfirmed_message_is_non_zero(self, counters):
        counters.record_sent()
        counters.record_not_confirmed()
        counters.record_consumed()

        assert app.decide_exit_code(counters.snapshot(), caught_up=True) == app.EXIT_MESSAGES_NOT_CONFIRMED

    def test_a_drain_wait_that_timed_out_is_non_zero(self, counters):
        for _ in range(2):
            counters.record_sent()
            counters.record_confirmed()
        counters.record_consumed()

        assert app.decide_exit_code(counters.snapshot(), caught_up=False) == app.EXIT_MESSAGES_NOT_CONSUMED

    def test_consumption_short_of_the_attempts_is_non_zero_even_if_the_wait_said_otherwise(self, counters):
        counters.record_sent()
        counters.record_confirmed()

        assert app.decide_exit_code(counters.snapshot(), caught_up=True) == app.EXIT_MESSAGES_NOT_CONSUMED

    def test_an_unconfirmed_message_outranks_a_late_consumer(self, counters):
        counters.record_sent()
        counters.record_not_confirmed()

        assert app.decide_exit_code(counters.snapshot(), caught_up=False) == app.EXIT_MESSAGES_NOT_CONFIRMED

    def test_a_run_that_did_nothing_at_all_exits_zero(self, counters):
        assert app.decide_exit_code(counters.snapshot(), caught_up=True) == app.EXIT_OK

    def test_every_failure_code_is_non_zero_and_distinct(self):
        codes = {
            app.EXIT_MESSAGES_NOT_CONFIRMED,
            app.EXIT_INVALID_OPTIONS,
            app.EXIT_SETUP_FAILED,
            app.EXIT_MESSAGES_NOT_CONSUMED,
            app.EXIT_INTERRUPTED,
        }

        assert len(codes) == 5
        assert app.EXIT_OK not in codes


# --- queue-type mapping (§2.1) ------------------------------------------


class TestMapQueueType:
    """Each ``--queue-type`` value drives its own sub-builder, and nothing else."""

    @pytest.mark.parametrize(
        ("queue_type", "expected"),
        [
            ("classic", QueueType.CLASSIC),
            ("quorum", QueueType.QUORUM),
            ("stream", QueueType.STREAM),
        ],
    )
    def test_the_mapping_sets_only_x_queue_type(self, queue_type, expected):
        original = specification("orders")

        mapped = app.map_queue_type(app.Options(queue_type=queue_type), original)

        assert mapped is original
        assert mapped.queue_arguments == {ARG_QUEUE_TYPE: expected.value}
        assert mapped.queue_name == "orders"

    def test_an_unknown_queue_type_raises(self):
        with pytest.raises(app.OptionsError, match="unknown queue type"):
            app.map_queue_type(app.Options(queue_type="lazy"), specification())


# --- connection parameters (§4.1, §10) ----------------------------------


class TestBuildConnectionParameters:
    """The options are forwarded verbatim, and no recovery is wired in."""

    def test_every_connection_option_is_forwarded(self):
        options = app.Options(
            host="broker.example",
            port=5673,
            virtual_host="/staging",
            user="alice",
            password="s3cret",
        )

        parameters = app.build_connection_parameters(options)

        assert (parameters.host, parameters.port, parameters.virtual_host) == ("broker.example", 5673, "/staging")
        assert (parameters.user, parameters.password) == ("alice", "s3cret")
        assert parameters.tls is None

    def test_no_reconnection_is_wired_in(self):
        """§4.1/§10: no ``on_unexpected_close``, and recovery switched off."""
        parameters = app.build_connection_parameters(app.Options())

        assert parameters.on_unexpected_close is None
        assert parameters.recovery_configuration.activated is False

    def test_no_port_lets_the_client_default_it(self):
        assert app.build_connection_parameters(app.Options()).resolved_port == 5672

    def test_tls_brings_its_own_context_and_port(self):
        parameters = app.build_connection_parameters(app.Options(tls=True))

        assert parameters.tls is not None
        assert parameters.resolved_port == 5671


# --- teardown (§4.9) ----------------------------------------------------


class TestTearDown:
    """Every step runs, in order, and a failing one is only logged."""

    def test_the_whole_sequence_runs_in_order(self):
        calls: list[str] = []
        consumer = SimpleNamespace(close=lambda: calls.append("consumer"))
        publisher = SimpleNamespace(close=lambda: calls.append("publisher"))
        connection = FakeConnection(calls)

        app.tear_down(
            cast(app.Connection, connection),
            cast(app.Consumer, consumer),
            cast(app.Publisher, publisher),
            app.Options(queue="q"),
        )

        assert calls == ["consumer", "publisher", "delete:q", "connection"]

    def test_keep_queue_skips_the_delete(self):
        calls: list[str] = []

        app.tear_down(cast(app.Connection, FakeConnection(calls)), None, None, app.Options(queue="q", keep_queue=True))

        assert calls == ["connection"]

    def test_a_failing_step_is_logged_and_the_rest_still_run(self, caplog):
        calls: list[str] = []

        def explode() -> None:
            raise AMQPError("the link is gone")

        with caplog.at_level("WARNING"):
            app.tear_down(
                cast(app.Connection, FakeConnection(calls)),
                cast(app.Consumer, SimpleNamespace(close=explode)),
                None,
                app.Options(queue="q"),
            )

        assert calls == ["delete:q", "connection"]
        assert "closing the consumer" in caplog.text


class FakeConnection:
    """Just enough of a connection for :func:`tear_down` to drive it."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def management(self) -> FakeConnection:
        """Stand in for the management endpoint."""
        return self

    def queue(self, name: str) -> SimpleNamespace:
        """Stand in for a queue specification with only ``delete``."""
        return SimpleNamespace(delete=lambda: self._calls.append(f"delete:{name}"))

    def close(self) -> None:
        """Record the closure."""
        self._calls.append("connection")


# --- run result ---------------------------------------------------------


class TestRunResult:
    """A setup failure carries the code and nothing measured."""

    def test_a_bare_result_is_all_zero(self):
        result = app.RunResult(exit_code=app.EXIT_SETUP_FAILED)

        assert result.counters == app.CounterSnapshot()
        assert result.latency == app.LatencySnapshot()
        assert (result.sent_per_second, result.consumed_per_second) == (0.0, 0.0)

    def test_a_connection_failure_never_prints_a_summary(self, monkeypatch, capsys):
        def refuse_to_connect(parameters):
            raise AMQPError("connection refused")

        monkeypatch.setattr(app, "Connection", refuse_to_connect)

        assert app.run(app.Options(messages=1)).exit_code == app.EXIT_SETUP_FAILED
        assert "Messages sent:" not in capsys.readouterr().out
