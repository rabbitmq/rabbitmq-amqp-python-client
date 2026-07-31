"""The console application example, run end to end against a live broker.

step_045's verification bar is "a runnable demonstration", so this file is a
smoke test rather than an exhaustive suite: it runs the real script the way a
caller would and checks the two things the spec makes contractual — the summary
block and the exit code. Everything decidable without a broker is covered in
``tests/unit/test_console_application.py``.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
import sys
import threading
import urllib.error
import uuid
from pathlib import Path

import console_application as app
import pytest

pytestmark = pytest.mark.integration

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "docs" / "examples" / "console_application.py"

SMOKE_MESSAGE_COUNT = 200
SUBPROCESS_TIMEOUT_SECONDS = 120


def summary_lines(output: str) -> list[str]:
    """Return the last summary block found in ``output``.

    Args:
        output: Everything the program printed on stdout.

    Returns:
        The eight lines of the final block, in order.
    """
    lines = [line for line in output.splitlines() if line.startswith(("Messages ", "  ", "Unexpected "))]
    return lines[-8:]


def expected_summary(count: int) -> list[str]:
    """Return the summary block a fully successful run of ``count`` messages prints."""
    return app.format_snapshot(
        app.CounterSnapshot(messages_sent=count, messages_confirmed=count, messages_consumed=count)
    ).splitlines()


@pytest.fixture
def queue_name(management_api):
    """A unique queue name, deleted afterwards however the run ended."""
    name = f"console-app-it-{uuid.uuid4().hex[:12]}"
    yield name
    with contextlib.suppress(urllib.error.HTTPError):
        management_api("DELETE", f"/api/queues/%2F/{name}")


@pytest.mark.parametrize("queue_type", ["classic", "quorum", "stream"])
def test_a_full_run_of_every_queue_type_exits_zero(queue_type, queue_name):
    """Every queue type publishes, consumes and reports a clean run."""
    completed = subprocess.run(  # noqa: S603 - a fixed, local script
        [
            sys.executable,
            str(SCRIPT),
            "--messages",
            str(SMOKE_MESSAGE_COUNT),
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
    assert summary_lines(completed.stdout) == expected_summary(SMOKE_MESSAGE_COUNT)


def test_zero_messages_never_reaches_the_broker():
    """``--messages 0`` exits non-zero with no summary and no connection."""
    completed = subprocess.run(  # noqa: S603 - a fixed, local script
        [sys.executable, str(SCRIPT), "--messages", "0"],
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
        check=False,
    )

    assert completed.returncode == app.EXIT_INVALID_OPTIONS
    assert "Messages sent:" not in completed.stdout
    assert "--messages must be > 0" in completed.stderr


def test_keep_queue_leaves_the_queue_behind(queue_name, management_api):
    """``--keep-queue`` skips the delete, so the queue is still there afterwards."""
    completed = subprocess.run(  # noqa: S603 - a fixed, local script
        [
            sys.executable,
            str(SCRIPT),
            "--messages",
            "20",
            "--queue",
            queue_name,
            "--keep-queue",
            "--consume-timeout",
            "30",
        ],
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
        check=False,
    )

    assert completed.returncode == app.EXIT_OK, completed.stderr
    assert management_api("GET", f"/api/queues/%2F/{queue_name}")["name"] == queue_name


def test_a_forced_disconnect_is_logged_and_counted(monkeypatch, capsys, queue_name):
    """A socket torn down mid-publish surfaces as ``Unexpected closures: 1``.

    Runs in-process rather than as a subprocess, because the transport has to be
    reachable to be destroyed. ``--no-recovery`` makes §4's callback fire the
    moment the drop is noticed instead of after a back-off give-up, which against
    a broker that is still up would never happen.
    """
    connections: list[app.Connection] = []
    real_connection = app.Connection

    class WatchedConnection(real_connection):  # type: ignore[misc,valid-type]
        """Records itself, so the test can reach the live socket."""

        def __init__(self, parameters):
            super().__init__(parameters)
            connections.append(self)

    monkeypatch.setattr(app, "Connection", WatchedConnection)

    def kill_the_transport() -> None:
        """Tear the socket down once the run has had time to get going."""
        threading.Event().wait(1.0)
        if connections:
            with contextlib.suppress(OSError):
                connections[0]._socket.shutdown(socket.SHUT_RDWR)

    killer = threading.Thread(target=kill_the_transport, daemon=True)
    killer.start()
    try:
        code = app.main(["--messages", "200000", "--queue", queue_name, "--no-recovery", "--consume-timeout", "30"])
    finally:
        killer.join(timeout=10.0)

    assert code == app.EXIT_UNEXPECTED_CLOSE
    printed = summary_lines(capsys.readouterr().out)
    assert printed[-1].startswith("Unexpected closures:")
    assert printed[-1].split()[-1] == "1"
