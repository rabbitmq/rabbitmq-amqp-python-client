"""The console application example's own logic, exercised without a broker.

Everything the example does apart from the four network calls — connect, declare,
attach, publish — is a pure function or a small object over ``Counters``, so the
whole option-parsing/counting/formatting/exit-code surface is unit-testable here.
``docs/examples`` is on ``pythonpath`` (see ``pyproject.toml``), which is why
``console_application`` imports as a plain module.
"""

from __future__ import annotations

import threading
from typing import cast

import console_application as app
import pytest

from rabbitmq_amqp_python_client import (
    AMQPError,
    AMQPTimeoutError,
    Management,
    Outcome,
    OutcomeState,
    PublisherError,
    PublishResult,
    QueueSpecification,
    QueueType,
)
from rabbitmq_amqp_python_client.management import ARG_QUEUE_TYPE
from rabbitmq_amqp_python_client.wire import Message


@pytest.fixture
def counters() -> app.Counters:
    """A fresh, all-zero set of counters."""
    return app.Counters()


def specification(name: str = "a-queue") -> QueueSpecification:
    """A real specification with no management endpoint behind it.

    Only :meth:`~rabbitmq_amqp_python_client.QueueSpecification.declare` touches
    the endpoint, and nothing here declares.
    """
    return QueueSpecification(cast(Management, None), name)


def snapshot(**counts: int) -> app.CounterSnapshot:
    """A snapshot with the named counters set and the rest at zero."""
    return app.CounterSnapshot(**counts)


# --- option parsing (§2, §7) -------------------------------------------


class TestParseArgs:
    """Defaults, every flag, and the combinations §7 rejects."""

    def test_defaults_match_the_spec_table(self):
        options = app.parse_args([])

        assert options.messages == 1_000_000
        assert options.queue_type == "classic"
        assert options.keep_queue is False
        assert options.consume_timeout == 30.0
        assert options.publish_timeout == 5.0
        assert options.stats_interval == 1.0
        assert options.host == "localhost"
        assert options.port is None
        assert options.user == "guest"
        assert options.password == "guest"
        assert options.virtual_host == "/"
        assert options.tls is False
        assert options.recovery is True
        assert options.recovery_topology is False

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
                "--queue",
                "my-queue",
                "--keep-queue",
                "--consume-timeout",
                "2.5",
                "--publish-timeout",
                "0.5",
                "--stats-interval",
                "0.25",
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
                "--no-recovery",
                "--recovery-topology",
            ]
        )

        assert options == app.Options(
            messages=12,
            queue_type="classic",
            queue="my-queue",
            keep_queue=True,
            consume_timeout=2.5,
            publish_timeout=0.5,
            stats_interval=0.25,
            host="broker.example",
            port=5671,
            user="alice",
            password="s3cret",
            virtual_host="/staging",
            tls=True,
            recovery=False,
            recovery_topology=True,
        )

    def test_recovery_can_be_turned_back_on_explicitly(self):
        assert app.parse_args(["--recovery"]).recovery is True
        assert app.parse_args(["--no-recovery-topology"]).recovery_topology is False

    def test_zero_messages_is_rejected(self):
        with pytest.raises(app.OptionsError, match="--messages must be > 0"):
            app.parse_args(["--messages", "0"])

    def test_negative_messages_is_rejected(self):
        with pytest.raises(app.OptionsError, match="--messages must be > 0"):
            app.parse_args(["-n", "-1"])

    @pytest.mark.parametrize(
        ("argv", "expected"),
        [
            (["--consume-timeout", "-1"], "--consume-timeout must be >= 0"),
            (["--publish-timeout", "0"], "--publish-timeout must be > 0"),
            (["--stats-interval", "0"], "--stats-interval must be > 0"),
        ],
    )
    def test_invalid_timings_are_rejected(self, argv, expected):
        with pytest.raises(app.OptionsError, match=expected):
            app.parse_args(argv)

    def test_a_zero_consume_timeout_is_allowed(self):
        assert app.parse_args(["--consume-timeout", "0"]).consume_timeout == 0.0


# --- counters (§5) ------------------------------------------------------


class TestCounters:
    """Every increment, the derived totals, and thread safety."""

    def test_a_fresh_set_is_all_zero(self, counters):
        assert counters.snapshot() == app.CounterSnapshot()

    def test_every_recorder_touches_exactly_its_own_counter(self, counters):
        counters.record_sent()
        counters.record_confirmed()
        counters.record_rejected()
        counters.record_released()
        counters.record_failed()
        counters.record_consumed()
        counters.record_unexpected_close()

        assert counters.snapshot() == app.CounterSnapshot(
            messages_sent=1,
            messages_confirmed=1,
            messages_rejected=1,
            messages_released=1,
            messages_failed=1,
            messages_consumed=1,
            unexpected_close_count=1,
        )

    def test_not_confirmed_sums_the_three_non_accepted_buckets(self):
        assert snapshot(messages_rejected=2, messages_released=3, messages_failed=4).messages_not_confirmed == 9

    def test_classified_counts_every_settled_attempt(self):
        assert snapshot(messages_confirmed=5, messages_released=1).messages_classified == 6

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

        assert counters.snapshot().messages_sent == 2000
        assert counters.snapshot().messages_consumed == 2000


# --- outcome classification (§3.5) --------------------------------------


class TestClassifyOutcome:
    """One bucket per outcome, plus the raised-publish path."""

    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            (OutcomeState.ACCEPTED, "messages_confirmed"),
            (OutcomeState.REJECTED, "messages_rejected"),
            (OutcomeState.RELEASED, "messages_released"),
        ],
    )
    def test_each_state_lands_in_its_own_bucket(self, counters, state, expected):
        app.classify_outcome(Outcome(state=state), counters)

        assert getattr(counters.snapshot(), expected) == 1
        assert counters.snapshot().messages_classified == 1

    def test_an_unmodelled_state_is_counted_as_a_failure(self, counters):
        app.classify_outcome(cast(Outcome, Outcome(state=cast(OutcomeState, "modified"))), counters)

        assert counters.snapshot().messages_failed == 1

    @pytest.mark.parametrize(
        "error",
        [AMQPTimeoutError("no disposition"), PublisherError("closed"), AMQPError("the connection is closed")],
    )
    def test_a_raised_publish_counts_as_failed_and_the_loop_continues(self, counters, error):
        publisher = FakePublisher([error, Outcome(state=OutcomeState.ACCEPTED)])
        options = app.Options(messages=2, publish_timeout=0.1)

        app.publish_all(cast(app.Publisher, publisher), options, counters, threading.Event())

        assert counters.snapshot() == app.CounterSnapshot(messages_sent=2, messages_confirmed=1, messages_failed=1)


class FakePublisher:
    """A publisher whose ``publish`` replays a scripted list of results.

    Each entry is either an :class:`Outcome` to return or an exception to raise.
    """

    def __init__(self, script: list[object]) -> None:
        self.script = list(script)
        self.timeouts: list[float | None] = []
        self.bodies: list[str] = []

    def publish(self, message: Message, timeout: float | None = None) -> PublishResult:
        """Return or raise the next scripted entry."""
        self.timeouts.append(timeout)
        self.bodies.append(cast(str, message.body.value.decode()))  # type: ignore[union-attr]
        entry = self.script.pop(0)
        if isinstance(entry, BaseException):
            raise entry
        return PublishResult(message=message, outcome=cast(Outcome, entry))


class TestPublishAll:
    """The publish loop's counting contract (§3.5)."""

    def test_every_attempt_is_sent_classified_and_carries_its_index(self, counters):
        publisher = FakePublisher([Outcome(state=OutcomeState.ACCEPTED)] * 3)
        options = app.Options(messages=3, publish_timeout=0.75)

        app.publish_all(cast(app.Publisher, publisher), options, counters, threading.Event())

        assert counters.snapshot() == app.CounterSnapshot(messages_sent=3, messages_confirmed=3)
        assert publisher.timeouts == [0.75, 0.75, 0.75]
        assert publisher.bodies == [
            "console-app-message-0",
            "console-app-message-1",
            "console-app-message-2",
        ]

    def test_an_already_aborted_run_never_sends_anything(self, counters):
        publisher = FakePublisher([])
        aborted = threading.Event()
        aborted.set()

        app.publish_all(cast(app.Publisher, publisher), app.Options(messages=5), counters, aborted)

        assert counters.snapshot() == app.CounterSnapshot()

    def test_an_unexpected_closure_mid_loop_ends_it_early(self, counters):
        aborted = threading.Event()

        def abort_after_first(message: Message, timeout: float | None = None) -> PublishResult:
            aborted.set()
            return PublishResult(message=message, outcome=Outcome(state=OutcomeState.ACCEPTED))

        publisher = FakePublisher([])
        publisher.publish = abort_after_first  # type: ignore[method-assign]

        app.publish_all(cast(app.Publisher, publisher), app.Options(messages=100), counters, aborted)

        assert counters.snapshot() == app.CounterSnapshot(messages_sent=1, messages_confirmed=1)


# --- output (§6) --------------------------------------------------------


EXPECTED_SUMMARY = """\
Messages sent:                 10
Messages confirmed:            4
Messages not confirmed:        6
  rejected:                    1
  released:                    2
  failed:                      3
Messages consumed:             4
Unexpected closures:           1"""


class TestFormatSummary:
    """§6's block: exact labels, exact order, values at a fixed column."""

    def test_the_block_matches_the_spec_line_for_line(self):
        rendered = app.format_snapshot(
            snapshot(
                messages_sent=10,
                messages_confirmed=4,
                messages_rejected=1,
                messages_released=2,
                messages_failed=3,
                messages_consumed=4,
                unexpected_close_count=1,
            )
        )

        assert rendered == EXPECTED_SUMMARY

    def test_an_all_zero_block_still_carries_every_line(self):
        lines = app.format_snapshot(snapshot()).splitlines()

        assert [line.split(":")[0] + ":" for line in lines] == [
            "Messages sent:",
            "Messages confirmed:",
            "Messages not confirmed:",
            "  rejected:",
            "  released:",
            "  failed:",
            "Messages consumed:",
            "Unexpected closures:",
        ]
        assert all(line.endswith("0") for line in lines)

    def test_every_value_starts_at_the_same_column(self):
        for line in app.format_snapshot(snapshot(messages_sent=1)).splitlines():
            assert line[: app.LABEL_WIDTH].rstrip() == line[: app.LABEL_WIDTH].rstrip()
            assert line[app.LABEL_WIDTH - 1] == " "
            assert line[app.LABEL_WIDTH] != " "

    def test_format_summary_reads_the_live_counters(self, counters):
        counters.record_sent()
        counters.record_confirmed()

        assert app.format_summary(counters) == app.format_snapshot(snapshot(messages_sent=1, messages_confirmed=1))


# --- exit codes (§7) ----------------------------------------------------


class TestDecideExitCode:
    """One case per row of §7's table."""

    def test_a_fully_confirmed_and_consumed_run_exits_zero(self, counters):
        for _ in range(3):
            counters.record_sent()
            counters.record_confirmed()
            counters.record_consumed()

        assert app.decide_exit_code(app.Options(messages=3), counters) == app.EXIT_OK

    def test_consumption_falling_short_is_non_zero(self, counters):
        for _ in range(2):
            counters.record_sent()
            counters.record_confirmed()
        counters.record_consumed()

        assert app.decide_exit_code(app.Options(messages=2), counters) == app.EXIT_MESSAGES_NOT_CONSUMED

    @pytest.mark.parametrize("recorder", ["record_rejected", "record_released", "record_failed"])
    def test_any_non_accepted_outcome_is_non_zero(self, counters, recorder):
        counters.record_sent()
        getattr(counters, recorder)()
        counters.record_consumed()

        assert app.decide_exit_code(app.Options(messages=1), counters) == app.EXIT_MESSAGES_NOT_CONFIRMED

    def test_an_unexpected_closure_wins_over_a_perfect_run(self, counters):
        counters.record_sent()
        counters.record_confirmed()
        counters.record_consumed()
        counters.record_unexpected_close()

        assert app.decide_exit_code(app.Options(messages=1), counters) == app.EXIT_UNEXPECTED_CLOSE

    def test_a_publish_loop_cut_short_is_non_zero(self, counters):
        counters.record_sent()
        counters.record_confirmed()
        counters.record_consumed()

        assert app.decide_exit_code(app.Options(messages=5), counters) == app.EXIT_MESSAGES_NOT_CONFIRMED

    def test_every_failure_code_is_non_zero_and_distinct(self):
        codes = {
            app.EXIT_MESSAGES_NOT_CONFIRMED,
            app.EXIT_INVALID_OPTIONS,
            app.EXIT_SETUP_FAILED,
            app.EXIT_MESSAGES_NOT_CONSUMED,
            app.EXIT_UNEXPECTED_CLOSE,
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

    def test_the_declare_body_carries_nothing_else(self):
        mapped = app.map_queue_type(app.Options(queue_type="quorum"), specification("orders"))

        assert mapped.declare_body() == {
            "durable": True,
            "exclusive": False,
            "auto_delete": False,
            "arguments": {ARG_QUEUE_TYPE: "quorum"},
        }

    def test_an_unknown_queue_type_raises(self):
        with pytest.raises(app.OptionsError, match="unknown queue type"):
            app.map_queue_type(app.Options(queue_type="lazy"), specification())


# --- connection parameters (§3.1) ---------------------------------------


class TestBuildConnectionParameters:
    """The options are forwarded verbatim, with no local default of their own."""

    def test_every_connection_option_is_forwarded(self):
        options = app.Options(
            host="broker.example",
            port=5673,
            virtual_host="/staging",
            user="alice",
            password="s3cret",
            recovery=False,
            recovery_topology=True,
        )
        callback = lambda error: None  # noqa: E731 - a one-line stub

        parameters = app.build_connection_parameters(options, callback)

        assert (parameters.host, parameters.port, parameters.virtual_host) == ("broker.example", 5673, "/staging")
        assert (parameters.user, parameters.password) == ("alice", "s3cret")
        assert parameters.tls is None
        assert parameters.on_unexpected_close is callback
        assert parameters.recovery_configuration.activated is False
        assert parameters.recovery_configuration.topology is True

    def test_no_port_lets_the_client_default_it(self):
        assert app.build_connection_parameters(app.Options(), lambda error: None).resolved_port == 5672

    def test_tls_brings_its_own_context_and_port(self):
        parameters = app.build_connection_parameters(app.Options(tls=True), lambda error: None)

        assert parameters.tls is not None
        assert parameters.resolved_port == 5671


# --- unexpected close (§4) ----------------------------------------------


class TestUnexpectedCloseReporter:
    """The callback logs once at error level, counts once, and aborts the run."""

    def test_it_logs_the_counters_and_the_error(self, counters, caplog):
        counters.record_sent()
        counters.record_confirmed()
        counters.record_consumed()
        reporter = app.UnexpectedCloseReporter(counters)

        with caplog.at_level("ERROR"):
            reporter(AMQPError("the broker vanished"))

        assert counters.snapshot().unexpected_close_count == 1
        assert reporter.aborted.is_set()
        record = next(item for item in caplog.records if item.levelname == "ERROR")
        assert "sent=1" in record.getMessage()
        assert "confirmed=1" in record.getMessage()
        assert "consumed=1" in record.getMessage()
        assert "the broker vanished" in record.getMessage()

    def test_a_clean_peer_close_carries_no_error(self, counters, caplog):
        with caplog.at_level("ERROR"):
            app.UnexpectedCloseReporter(counters)(None)

        assert counters.snapshot().unexpected_close_count == 1
        assert "None" in caplog.records[0].getMessage()

    def test_the_connection_state_is_reported_when_one_is_known(self, counters, caplog):
        reporter = app.UnexpectedCloseReporter(counters)
        reporter.connection = cast(app.Connection, FakeConnection())

        with caplog.at_level("ERROR"):
            reporter(None)

        assert "state=closed" in caplog.records[0].getMessage()

    def test_an_unknown_connection_state_is_not_fatal(self, counters, caplog):
        with caplog.at_level("ERROR"):
            app.UnexpectedCloseReporter(counters)(None)

        assert "state=unknown" in caplog.records[0].getMessage()


class FakeConnection:
    """Just enough of a connection for the reporter to read its state."""

    def __init__(self) -> None:
        from rabbitmq_amqp_python_client import ConnectionState

        self.state = ConnectionState.CLOSED


# --- drain wait (§3.6) --------------------------------------------------


class TestWaitForConsumption:
    """The three ways §3.6's wait can return."""

    def test_it_returns_at_once_when_consumption_already_caught_up(self, counters):
        counters.record_sent()
        counters.record_consumed()

        assert app.wait_for_consumption(counters, timeout=5.0, aborted=threading.Event()) is True

    def test_it_returns_once_the_delivery_loop_catches_up(self, counters):
        counters.record_sent()
        threading.Timer(0.05, counters.record_consumed).start()

        assert app.wait_for_consumption(counters, timeout=5.0, aborted=threading.Event()) is True

    def test_it_gives_up_when_the_timeout_elapses(self, counters, caplog):
        counters.record_sent()

        with caplog.at_level("WARNING"):
            caught_up = app.wait_for_consumption(counters, timeout=0.05, aborted=threading.Event())

        assert caught_up is False
        assert "gave up" in caplog.text

    def test_it_stops_early_once_the_connection_died(self, counters, caplog):
        counters.record_sent()
        aborted = threading.Event()
        aborted.set()

        with caplog.at_level("WARNING"):
            caught_up = app.wait_for_consumption(counters, timeout=60.0, aborted=aborted)

        assert caught_up is False
        assert "the connection died" in caplog.text


# --- periodic stats (§3.9) ----------------------------------------------


class TestStatsPrinter:
    """The printer reads the counters, prints the block, and joins on stop."""

    def test_a_tick_prints_the_block_behind_an_elapsed_marker(self, counters, capsys):
        counters.record_sent()
        printer = app.StatsPrinter(counters, interval=60.0)
        printer.start()
        printer.print_tick()
        printer.stop()

        out = capsys.readouterr().out
        assert "elapsed" in out.splitlines()[0]
        assert app.format_summary(counters) in out

    def test_it_ticks_repeatedly_while_running(self, counters, capsys):
        printer = app.StatsPrinter(counters, interval=0.01)
        printer.start()
        try:
            deadline = threading.Event()
            deadline.wait(0.2)
        finally:
            printer.stop()

        assert capsys.readouterr().out.count("Messages sent:") >= 2

    def test_stop_joins_the_thread_and_is_idempotent(self, counters):
        printer = app.StatsPrinter(counters, interval=0.01)
        printer.start()
        assert printer.is_running is True

        printer.stop()
        printer.stop()

        assert printer.is_running is False

    def test_starting_twice_keeps_one_thread(self, counters):
        printer = app.StatsPrinter(counters, interval=60.0)
        printer.start()
        printer.start()
        printer.stop()

        assert printer.is_running is False

    def test_it_never_mutates_the_counters(self, counters):
        printer = app.StatsPrinter(counters, interval=0.01)
        printer.start()
        threading.Event().wait(0.1)
        printer.stop()

        assert counters.snapshot() == app.CounterSnapshot()


# --- teardown (§3.8) ----------------------------------------------------


class RecordingConnection:
    """Records the teardown calls made on it, and can be told to fail one."""

    def __init__(self, *, failing: str = "") -> None:
        self.calls: list[str] = []
        self.failing = failing

    def management(self) -> RecordingConnection:
        """Stand in for the management endpoint as well as the connection."""
        return self

    def queue(self, name: str) -> RecordingConnection:
        """Stand in for the queue specification too."""
        self.calls.append(f"queue({name})")
        return self

    def delete(self) -> None:
        """Record, or fail, the queue delete."""
        self._record("delete")

    def close(self) -> None:
        """Record, or fail, the connection close."""
        self._record("close")

    def _record(self, name: str) -> None:
        self.calls.append(name)
        if self.failing == name:
            raise AMQPError(f"{name} failed")


class RecordingEndpoint:
    """A consumer/publisher stand-in that records its ``close``."""

    def __init__(self, name: str, calls: list[str], *, failing: bool = False) -> None:
        self.name = name
        self.calls = calls
        self.failing = failing

    def close(self) -> None:
        """Record, or fail, the close."""
        self.calls.append(self.name)
        if self.failing:
            raise AMQPError(f"{self.name} failed")


class TestTearDown:
    """§3.8's order, and its refusal to let one failure stop the rest."""

    def test_it_closes_everything_in_order(self):
        connection = RecordingConnection()
        consumer = RecordingEndpoint("consumer", connection.calls)
        publisher = RecordingEndpoint("publisher", connection.calls)

        app.tear_down(
            cast(app.Connection, connection),
            cast(app.Consumer, consumer),
            cast(app.Publisher, publisher),
            app.Options(queue="orders"),
        )

        assert connection.calls == ["consumer", "publisher", "queue(orders)", "delete", "close"]

    def test_keep_queue_skips_the_delete_only(self):
        connection = RecordingConnection()

        app.tear_down(cast(app.Connection, connection), None, None, app.Options(queue="orders", keep_queue=True))

        assert connection.calls == ["close"]

    def test_a_failing_step_does_not_stop_the_rest(self, caplog):
        connection = RecordingConnection(failing="delete")
        consumer = RecordingEndpoint("consumer", connection.calls, failing=True)

        with caplog.at_level("WARNING"):
            app.tear_down(
                cast(app.Connection, connection),
                cast(app.Consumer, consumer),
                None,
                app.Options(queue="orders"),
            )

        assert connection.calls == ["consumer", "queue(orders)", "delete", "close"]
        assert "closing the consumer" in caplog.text
        assert "deleting the queue" in caplog.text

    def test_endpoints_that_were_never_built_are_skipped(self):
        connection = RecordingConnection()

        app.tear_down(cast(app.Connection, connection), None, None, app.Options(queue="orders"))

        assert connection.calls == ["queue(orders)", "delete", "close"]


# --- entry point (§7) ---------------------------------------------------


class TestMain:
    """``main`` never connects on a bad option, and forwards ``run``'s code."""

    def test_zero_messages_exits_non_zero_without_running_anything(self, monkeypatch, capsys, caplog):
        def refuse_to_run(options: app.Options) -> int:
            raise AssertionError("run() must not be reached for invalid options")

        monkeypatch.setattr(app, "run", refuse_to_run)

        with caplog.at_level("ERROR"):
            code = app.main(["--messages", "0"])

        assert code == app.EXIT_INVALID_OPTIONS
        assert "--messages must be > 0" in caplog.text
        assert "Messages sent:" not in capsys.readouterr().out

    def test_the_run_result_becomes_the_exit_code(self, monkeypatch):
        monkeypatch.setattr(app, "run", lambda options: app.EXIT_MESSAGES_NOT_CONSUMED)

        assert app.main([]) == app.EXIT_MESSAGES_NOT_CONSUMED

    def test_the_parsed_options_reach_run(self, monkeypatch):
        seen: list[app.Options] = []

        def capture(options: app.Options) -> int:
            seen.append(options)
            return app.EXIT_OK

        monkeypatch.setattr(app, "run", capture)

        assert app.main(["-n", "4", "--queue", "q", "--queue-type", "stream"]) == app.EXIT_OK
        assert seen == [app.Options(messages=4, queue="q", queue_type="stream")]

    def test_ctrl_c_is_reported_rather_than_traced(self, monkeypatch, caplog):
        def interrupt(options: app.Options) -> int:
            raise KeyboardInterrupt

        monkeypatch.setattr(app, "run", interrupt)

        with caplog.at_level("WARNING"):
            code = app.main(["-n", "1"])

        assert code == app.EXIT_INTERRUPTED
        assert "interrupted" in caplog.text
