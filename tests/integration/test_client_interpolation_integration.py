"""Cross-client interpolation: this client <-> pika, against a live broker.

Implements 005_clients_interpolation/step_100_client-interpolation.md's Python
specialization (§7.1) of its generic spec (§1-§6) and test matrix (§3):

  - A -> B: publish with this client, consume with pika (AMQP 0-9-1).
  - B -> A: publish with pika, consume with this client.

Each direction runs against both a classic and a quorum queue, round-tripping body,
content-type/content-encoding, and string/bool/int application properties/headers
(§2 points 1, 2, 4, 5). B -> A additionally round-trips string message-id/
correlation-id (§2 point 3).

Verified against RabbitMQ 4.3.5: this client's ``ApplicationProperties`` do map to
pika's ``BasicProperties.headers`` on A -> B, but the reverse is not symmetric -
RabbitMQ converts AMQP 0-9-1 ``headers`` into AMQP 1.0 ``MessageAnnotations``, not
``ApplicationProperties``, so B -> A asserts against ``Message.message_annotations``
instead (§2 point 4's note on this).
"""

from __future__ import annotations

import contextlib
import threading
import time
import uuid

import pika
import pytest

from rabbitmq_amqp_python_client import Connection, ConnectionParameters, OutcomeState
from rabbitmq_amqp_python_client.management import QueueInfo
from rabbitmq_amqp_python_client.wire import ApplicationProperties, Message, Properties

pytestmark = pytest.mark.integration

PIKA_HOST = "localhost"
PIKA_USER = "guest"
PIKA_PASSWORD = "guest"

WAIT_TIMEOUT_SECONDS = 15.0
PUBLISH_TIMEOUT_SECONDS = 10.0


def _name(prefix: str) -> str:
    """A unique name for one test's queue."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _wait_until(predicate, description, timeout=WAIT_TIMEOUT_SECONDS) -> None:
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


def _declare_classic(management, name: str) -> QueueInfo:
    return management.queue(name).declare()


def _declare_quorum(management, name: str) -> QueueInfo:
    return management.queue(name).quorum().queue().declare()


def _pika_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(PIKA_USER, PIKA_PASSWORD)
    parameters = pika.ConnectionParameters(PIKA_HOST, credentials=credentials)
    return pika.BlockingConnection(parameters)


def _consume_one_with_pika(queue_name: str) -> dict:
    """Consume exactly one message from ``queue_name`` with pika."""
    pika_connection = _pika_connection()
    received: dict = {}
    try:
        channel = pika_connection.channel()

        def on_message(chan, method_frame, header_frame, body, userdata=None):
            received["body"] = body
            received["header_frame"] = header_frame
            chan.basic_ack(delivery_tag=method_frame.delivery_tag)
            channel.stop_consuming()

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue_name, on_message)
        channel.start_consuming()
    finally:
        pika_connection.close()
    return received


def _publish_one_with_pika(queue_name: str, body: bytes, properties: pika.BasicProperties) -> None:
    pika_connection = _pika_connection()
    try:
        channel = pika_connection.channel()
        channel.basic_publish(exchange="", routing_key=queue_name, body=body, properties=properties)
    finally:
        pika_connection.close()


class _CapturingHandler:
    """Records the first delivery, accepts it, and lets callers wait for it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._messages: list[Message] = []

    def __call__(self, context, message: Message) -> None:
        with self._lock:
            self._messages.append(message)
        context.accept()

    @property
    def message(self) -> Message | None:
        with self._lock:
            return self._messages[0] if self._messages else None

    def wait(self, timeout: float = WAIT_TIMEOUT_SECONDS) -> None:
        _wait_until(lambda: self.message is not None, "one delivery", timeout)


def _run_a_to_b(connection, management, declare) -> None:
    """Direction A -> B (§1): publish with this client, consume with pika."""
    queue_name = declare(management, _name("interp-a-to-b")).name

    try:
        publisher = connection.publisher_builder().queue(queue_name).build()
        try:
            message = Message(
                body="interpolation-a-to-b",
                properties=Properties(content_type="text/plain", content_encoding="utf-8"),
                application_properties=ApplicationProperties({"x-string": "a-value", "x-bool": True, "x-int": 42}),
            )
            result = publisher.publish(message, timeout=PUBLISH_TIMEOUT_SECONDS)
            assert result.outcome.state is OutcomeState.ACCEPTED
        finally:
            publisher.close()

        received = _consume_one_with_pika(queue_name)

        assert received["body"] == b"interpolation-a-to-b"
        header_frame = received["header_frame"]
        assert header_frame.content_type == "text/plain"
        assert header_frame.content_encoding == "utf-8"
        assert header_frame.headers["x-string"] == "a-value"
        assert header_frame.headers["x-bool"] is True
        assert header_frame.headers["x-int"] == 42
    finally:
        with contextlib.suppress(Exception):  # cleanup must not mask a failure
            management.queue(queue_name).delete()


def _run_b_to_a(connection, management, declare) -> None:
    """Direction B -> A (§1): publish with pika, consume with this client."""
    queue_name = declare(management, _name("interp-b-to-a")).name

    try:
        _publish_one_with_pika(
            queue_name,
            b"interpolation-b-to-a",
            pika.BasicProperties(
                content_type="text/plain",
                content_encoding="utf-8",
                message_id="b-to-a-message-id",
                correlation_id="b-to-a-correlation-id",
                headers={"x-string": "b-value", "x-bool": True, "x-int": 7},
            ),
        )

        handler = _CapturingHandler()
        consumer = connection.consumer_builder().queue(queue_name).message_handler(handler).build()
        try:
            handler.wait()
        finally:
            consumer.close()

        message = handler.message
        assert message is not None
        assert message.body_as_string() == "interpolation-b-to-a"
        assert message.properties is not None
        assert message.properties.content_type == "text/plain"
        assert message.properties.content_encoding == "utf-8"
        assert message.properties.message_id == "b-to-a-message-id"
        assert message.properties.correlation_id == "b-to-a-correlation-id"
        # RabbitMQ converts AMQP 0-9-1 headers into AMQP 1.0 MessageAnnotations, not
        # ApplicationProperties (verified against RabbitMQ 4.3.5) - see module docstring.
        assert message.application_properties is None
        assert message.message_annotations is not None
        annotations = message.message_annotations.value
        assert annotations["x-string"] == "b-value"
        assert annotations["x-bool"] is True
        assert annotations["x-int"] == 7
    finally:
        with contextlib.suppress(Exception):  # cleanup must not mask a failure
            management.queue(queue_name).delete()


class TestClientInterpolation:
    """step_100_client-interpolation.md §3's full direction x queue-type matrix."""

    def test_a_to_b_classic_queue(self, connection, management) -> None:
        _run_a_to_b(connection, management, _declare_classic)

    def test_a_to_b_quorum_queue(self, connection, management) -> None:
        _run_a_to_b(connection, management, _declare_quorum)

    def test_b_to_a_classic_queue(self, connection, management) -> None:
        _run_b_to_a(connection, management, _declare_classic)

    def test_b_to_a_quorum_queue(self, connection, management) -> None:
        _run_b_to_a(connection, management, _declare_quorum)
