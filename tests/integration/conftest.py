"""Shared fixtures for the tests that need a live RabbitMQ broker."""

from __future__ import annotations

import base64
import contextlib
import json
import socket
import urllib.error
import urllib.parse
import urllib.request

import pytest

BROKER_HOST = "localhost"
BROKER_PORT = 5672
MANAGEMENT_URL = "http://localhost:15672"
BROKER_USER = "guest"
BROKER_PASSWORD = "guest"
PROBE_TIMEOUT_SECONDS = 2.0


def _probe(host: str, port: int) -> bool:
    """Whether a TCP connection to ``host``/``port`` succeeds."""
    try:
        with socket.create_connection((host, port), timeout=PROBE_TIMEOUT_SECONDS):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_broker():
    """Skip the whole integration suite unless the broker answers on 5672."""
    if not _probe(BROKER_HOST, BROKER_PORT):
        pytest.skip(f"no RabbitMQ broker reachable at {BROKER_HOST}:{BROKER_PORT}")


@pytest.fixture(scope="session")
def management_api():
    """Skip tests needing the HTTP API unless it answers on 15672."""
    if not _probe(BROKER_HOST, 15672):
        pytest.skip(f"no RabbitMQ management API reachable at {MANAGEMENT_URL}")
    return _http_request


def _http_request(method: str, path: str, body: dict | None = None):
    """Call the broker's HTTP management API.

    Args:
        method: HTTP verb.
        path: Path below ``/api``, already percent-encoded.
        body: Optional JSON body.

    Returns:
        The decoded JSON response, or ``None`` for an empty body.
    """
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(f"{MANAGEMENT_URL}{path}", data=data, method=method)
    credentials = base64.b64encode(f"{BROKER_USER}:{BROKER_PASSWORD}".encode()).decode()
    request.add_header("Authorization", f"Basic {credentials}")
    request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 - fixed localhost URL
        payload = response.read()
    return json.loads(payload) if payload else None


@pytest.fixture
def queue_factory(management_api):
    """Return ``make(name) -> address``, creating queues and deleting them after."""
    created: list[str] = []

    def make(name: str) -> str:
        quoted = urllib.parse.quote(name, safe="")
        management_api("PUT", f"/api/queues/%2F/{quoted}", {"durable": True, "arguments": {}})
        created.append(quoted)
        return f"/queues/{quoted}"

    yield make

    for quoted in created:
        with contextlib.suppress(urllib.error.HTTPError):
            management_api("DELETE", f"/api/queues/%2F/{quoted}")
