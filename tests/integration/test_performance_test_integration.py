"""The performance test example, run end to end against a live broker.

step_050's verification bar is "a runnable demonstration", so this file is a
smoke test rather than an exhaustive suite: it runs the real program the way a
caller would and checks what §8/§11 make contractual — the exit code, the summary
block, and that the rolling latency window (§6.1) really does reset once per
``--latency-window-size`` consumed messages while §6.2's overall accumulator
keeps every sample. Everything decidable without a broker is covered in
``tests/unit/test_performance_test.py``.
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
import urllib.error
import uuid
from pathlib import Path

import performance_test as app
import pytest

from src import Connection, ConnectionParameters
from src.wire import Message

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "docs" / "examples" / "performance_test.py"

SMOKE_MESSAGE_COUNT = 50
SMOKE_LATENCY_WINDOW_SIZE = 10
SUBPROCESS_TIMEOUT_SECONDS = 120


def summary_lines(output: str) -> list[str]:
    """Return the last summary block found in ``output``.

    Args:
        output: Everything the program printed on stdout.

    Returns:
        The seven lines of the final block, in order.
    """
    return [line for line in output.splitlines() if line.startswith(("Messages ", "Avg latency"))][-7:]


def value_of(lines: list[str], label: str) -> str:
    """Return the value printed on the line starting with ``label``."""
    return next(line for line in lines if line.startswith(label)).split(":", 1)[1].strip()


@pytest.fixture
def queue_name(management_api):
    """A unique queue name, deleted afterwards however the run ended."""
    name = f"perf-test-it-{uuid.uuid4().hex[:12]}"
    yield name
    with contextlib.suppress(urllib.error.HTTPError):
        management_api("DELETE", f"/api/queues/%2F/{name}")


def test_a_small_run_resets_the_window_once_per_window_size(queue_name, capsys):
    """§11 integration case 1: every window resets, and §6.2 keeps every sample.

    Run in-process so the accumulators themselves can be asserted on, rather than
    inferred from what the periodic printer happened to catch.
    """
    options = app.parse_args(
        [
            "--messages",
            str(SMOKE_MESSAGE_COUNT),
            "--latency-window-size",
            str(SMOKE_LATENCY_WINDOW_SIZE),
            "--queue",
            queue_name,
            "--stats-interval",
            "0.05",
            "--consume-timeout",
            "20",
        ]
    )

    result = app.run(options)

    assert result.exit_code == app.EXIT_OK
    assert result.counters == app.CounterSnapshot(
        messages_sent=SMOKE_MESSAGE_COUNT,
        messages_confirmed=SMOKE_MESSAGE_COUNT,
        messages_consumed=SMOKE_MESSAGE_COUNT,
    )
    # 50 messages through a 10-message window: five completed windows, none left
    # over, and the last one's average retained across the resets (§6.1 point 3).
    assert result.latency.completed_windows >= SMOKE_MESSAGE_COUNT // SMOKE_LATENCY_WINDOW_SIZE
    assert result.latency.last_window_average_ns is not None
    # §6.2 is computed from every one of the 50, not just the last window.
    assert result.latency.overall_count == SMOKE_MESSAGE_COUNT
    assert result.latency.overall_average_ns is not None
    assert result.latency.overall_average_ns > 0
    assert result.sent_per_second > 0
    assert result.consumed_per_second > 0
    assert (
        summary_lines(capsys.readouterr().out)
        == app.format_summary(
            result.counters,
            result.sent_per_second,
            result.consumed_per_second,
            result.latency.overall_average_ns,
        ).splitlines()
    )


@pytest.mark.parametrize("queue_type", ["classic", "quorum", "stream"])
def test_a_full_run_of_every_queue_type_exits_zero(queue_type, queue_name):
    """§11 integration case 2: every queue type reports non-zero throughput and exits 0."""
    completed = subprocess.run(  # noqa: S603 - a fixed, local script
        [
            sys.executable,
            str(SCRIPT),
            "--messages",
            str(SMOKE_MESSAGE_COUNT),
            "--latency-window-size",
            str(SMOKE_LATENCY_WINDOW_SIZE),
            "--queue-type",
            queue_type,
            "--queue",
            queue_name,
            "--consume-timeout",
            "30",
        ],
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
        check=False,
    )

    assert completed.returncode == app.EXIT_OK, completed.stderr
    lines = summary_lines(completed.stdout)
    assert value_of(lines, "Messages sent:") == str(SMOKE_MESSAGE_COUNT)
    assert value_of(lines, "Messages confirmed:") == str(SMOKE_MESSAGE_COUNT)
    assert value_of(lines, "Messages not confirmed:") == "0"
    assert value_of(lines, "Messages consumed:") == str(SMOKE_MESSAGE_COUNT)
    assert float(value_of(lines, "Messages sent/sec:").replace(",", "")) > 0
    assert float(value_of(lines, "Messages consumed/sec:").replace(",", "")) > 0
    assert float(value_of(lines, "Avg latency (ms):")) > 0


def test_a_delivery_without_a_timestamp_is_counted_but_not_timed(queue_name, management_api, caplog):
    """§11 integration case 3: §3 point 4's skip path, end to end.

    The queue is seeded out of band with one message carrying no
    ``x-send-timestamp``, so exactly one of the deliveries this run consumes has
    no usable stamp: it still counts toward ``messages_consumed`` and is still
    accepted, but it is excluded from both latency accumulators and warned about
    once. How many of the eleven deliveries the drain wait has seen by the time it
    returns is timing-dependent (it stops as soon as ten have arrived), so the
    assertions below are on the invariant — exactly one consumed delivery went
    untimed — rather than on a fixed total.
    """
    management_api("PUT", f"/api/queues/%2F/{queue_name}", {"durable": True, "arguments": {}})
    seeding = Connection(ConnectionParameters())
    try:
        publisher = seeding.publisher_builder().queue(queue_name).build()
        publisher.publish(Message("seeded without a send timestamp"))
        publisher.close()
    finally:
        seeding.close()

    options = app.parse_args(
        [
            "--messages",
            "10",
            "--latency-window-size",
            "5",
            "--queue",
            queue_name,
            "--keep-queue",
            "--consume-timeout",
            "20",
        ]
    )
    with caplog.at_level("WARNING"):
        result = app.run(options)

    assert result.counters.messages_sent == 10
    assert result.counters.messages_confirmed == 10
    assert result.counters.messages_consumed >= 10
    # The seeded delivery counts, and is the only one latency accounting skipped.
    assert result.latency.overall_count == result.counters.messages_consumed - 1
    assert result.latency.overall_average_ns is not None
    assert len([record for record in caplog.records if app.SEND_TIMESTAMP_PROPERTY in record.getMessage()]) == 1


@pytest.mark.parametrize(
    ("argv", "expected_message"),
    [
        (["--messages", "0"], "--messages must be > 0"),
        (["--latency-window-size", "0"], "--latency-window-size must be > 0"),
    ],
)
def test_the_rejected_options_never_reach_the_broker(argv, expected_message):
    """§8 rows 1-2: rejected at startup, non-zero, with no summary printed."""
    completed = subprocess.run(  # noqa: S603 - a fixed, local script
        [sys.executable, str(SCRIPT), *argv],
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
        check=False,
    )

    assert completed.returncode == app.EXIT_INVALID_OPTIONS
    assert "Messages sent:" not in completed.stdout
    assert expected_message in completed.stderr
