"""Cross-client interpolation, for stream queues: this client <-> rstream, against a live broker.

Applies 005_clients_interpolation/step_100_client-interpolation.md's cross-client round-trip pattern to a
**stream** queue (003_stream_features/step_080_stream-filtering.md), which step_100 §1/§6 explicitly excludes
from its own classic/quorum matrix. The "other side" here is
`rstream <https://github.com/rabbitmq-community/rstream>`_, RabbitMQ's own Python client for the stream protocol
(not AMQP 0-9-1 like ``pika`` in ``test_client_interpolation_integration.py``) — a good match for a stream queue
since streams are the protocol rstream is built around:

  - A -> B: publish with this client (AMQP 1.0), consume with rstream (the stream protocol).
  - B -> A: publish with rstream, consume with this client.

Both directions round-trip body, content-type/content-encoding, string message-id/correlation-id, and
string/bool/int application properties (step_100 §2 points 1-4).

Verified against RabbitMQ 4.3.5: unlike the ``pika``/AMQP 0-9-1 case, both sides here speak native AMQP 1.0
message encoding (a RabbitMQ stream stores the published message's encoded AMQP 1.0 sections directly), so
application properties round-trip symmetrically in *both* directions through this client's own
``ApplicationProperties`` — there is no ``pika``-style "B -> A lands in message annotations instead" asymmetry.
The one asymmetry that does exist is on rstream's decode side: rstream's AMQP 1.0 parser (``amqp_decoder``)
decodes ``symbol``-typed fields (``content-type``, ``content-encoding``) and the polymorphic ``message-id``/
``correlation-id`` fields as raw ``bytes``, not ``str`` — even though this client published them as ``str`` and
would itself decode them back as ``str`` (as ``test_client_interpolation_integration.py``'s B -> A case confirms
for the wire format both clients share). A -> B assertions below compare against ``bytes`` for exactly those
fields; B -> A assertions compare against ``str``, since this client's own decoder is what's under test there.

rstream additionally requires the ``rabbitmq_stream``/``rabbitmq_stream_management`` plugins and a broker
listening on the stream protocol's default port 5552 — not just AMQP 1.0's 5672 that the rest of this suite's
``require_broker`` fixture (``tests/integration/conftest.py``) checks — so this module skips itself separately
when that port isn't reachable, rather than widening that shared fixture for every other integration test.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
import time
import uuid

import pytest
from rstream import AMQPMessage, ConsumerOffsetSpecification, OffsetType, amqp_decoder
from rstream import Consumer as RstreamConsumer
from rstream import Producer as RstreamProducer
from rstream import Properties as RstreamProperties

from rabbitmq_amqp_python_client import Connection, ConnectionParameters, OutcomeState
from rabbitmq_amqp_python_client.consumer import StreamOffsetSpecification
from rabbitmq_amqp_python_client.management import QueueInfo
from rabbitmq_amqp_python_client.wire import ApplicationProperties, Message, Properties

pytestmark = pytest.mark.integration

RSTREAM_HOST = "localhost"
RSTREAM_PORT = 5552
RSTREAM_VHOST = "/"
RSTREAM_USER = "guest"
RSTREAM_PASSWORD = "guest"
PROBE_TIMEOUT_SECONDS = 2.0

WAIT_TIMEOUT_SECONDS = 15.0
PUBLISH_TIMEOUT_SECONDS = 10.0


def _probe(host: str, port: int) -> bool:
    """Whether a TCP connection to ``host``/``port`` succeeds."""
    try:
        with socket.create_connection((host, port), timeout=PROBE_TIMEOUT_SECONDS):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module", autouse=True)
def require_stream_port():
    """Skip this module unless the broker answers on the stream protocol's port."""
    if not _probe(RSTREAM_HOST, RSTREAM_PORT):
        pytest.skip(f"no RabbitMQ stream listener reachable at {RSTREAM_HOST}:{RSTREAM_PORT}")


def _name(prefix: str) -> str:
    """A unique name for one test's stream."""
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


def _declare_stream(management, name: str) -> QueueInfo:
    return management.queue(name).stream().queue().declare()


async def _consume_one_with_rstream(stream_name: str, timeout: float) -> AMQPMessage:
    """Consume exactly one message from ``stream_name`` with rstream, from its first offset."""
    consumer = RstreamConsumer(
        host=RSTREAM_HOST, port=RSTREAM_PORT, vhost=RSTREAM_VHOST, username=RSTREAM_USER, password=RSTREAM_PASSWORD
    )
    received: list[AMQPMessage] = []

    async def on_message(message: AMQPMessage, message_context) -> None:
        received.append(message)
        consumer.stop()

    try:
        await consumer.subscribe(
            stream=stream_name,
            callback=on_message,
            decoder=amqp_decoder,
            offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST),
        )
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(consumer.run(), timeout=timeout)
    finally:
        await consumer.close()

    if not received:
        raise AssertionError(f"timed out after {timeout:g}s waiting for one delivery from {stream_name!r}")
    return received[0]


def _consume_one_with_rstream_sync(stream_name: str, timeout: float = WAIT_TIMEOUT_SECONDS) -> AMQPMessage:
    return asyncio.run(_consume_one_with_rstream(stream_name, timeout))


async def _publish_one_with_rstream(stream_name: str, message: AMQPMessage) -> None:
    async with RstreamProducer(RSTREAM_HOST, username=RSTREAM_USER, password=RSTREAM_PASSWORD) as producer:
        await producer.create_stream(stream_name, exists_ok=True)
        await producer.send_wait(stream_name, message)


def _publish_one_with_rstream_sync(stream_name: str, message: AMQPMessage) -> None:
    asyncio.run(_publish_one_with_rstream(stream_name, message))


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


class TestClientStreamInterpolation:
    """step_080_stream-filtering.md's Consumer/Publisher against rstream, both directions."""

    def test_a_to_b(self, connection, management) -> None:
        """Direction A -> B: publish with this client, consume with rstream."""
        stream_name = _declare_stream(management, _name("stream-interp-a-to-b")).name

        try:
            publisher = connection.publisher_builder().queue(stream_name).build()
            try:
                message = Message(
                    body="stream-interpolation-a-to-b",
                    properties=Properties(
                        content_type="text/plain",
                        content_encoding="utf-8",
                        message_id="a-to-b-message-id",
                        correlation_id="a-to-b-correlation-id",
                    ),
                    application_properties=ApplicationProperties({"x-string": "a-value", "x-bool": True, "x-int": 42}),
                )
                result = publisher.publish(message, timeout=PUBLISH_TIMEOUT_SECONDS)
                assert result.outcome.state is OutcomeState.ACCEPTED
            finally:
                publisher.close()

            received = _consume_one_with_rstream_sync(stream_name)

            assert received.body == b"stream-interpolation-a-to-b"
            assert received.properties is not None
            # rstream's decoder returns `bytes` for symbol-/message-id-typed fields - see module docstring.
            assert received.properties.content_type == b"text/plain"
            assert received.properties.content_encoding == b"utf-8"
            assert received.properties.message_id == b"a-to-b-message-id"
            assert received.properties.correlation_id == b"a-to-b-correlation-id"
            assert received.application_properties is not None
            assert received.application_properties[b"x-string"] == b"a-value"
            assert received.application_properties[b"x-bool"] is True
            assert received.application_properties[b"x-int"] == 42
        finally:
            with contextlib.suppress(Exception):  # cleanup must not mask a failure
                management.queue(stream_name).delete()

    def test_b_to_a(self, connection, management) -> None:
        """Direction B -> A: publish with rstream, consume with this client."""
        stream_name = _declare_stream(management, _name("stream-interp-b-to-a")).name

        try:
            amqp_message = AMQPMessage(
                body=b"stream-interpolation-b-to-a",
                properties=RstreamProperties(
                    content_type="text/plain",
                    content_encoding="utf-8",
                    message_id="b-to-a-message-id",
                    correlation_id="b-to-a-correlation-id",
                ),
                application_properties={"x-string": "b-value", "x-bool": True, "x-int": 7},
            )
            _publish_one_with_rstream_sync(stream_name, amqp_message)

            handler = _CapturingHandler()
            consumer = (
                connection.consumer_builder()
                .queue(stream_name)
                .stream()
                .offset(StreamOffsetSpecification.FIRST)
                .builder()
                .message_handler(handler)
                .build()
            )
            try:
                handler.wait()
            finally:
                consumer.close()

            message = handler.message
            assert message is not None
            assert message.body_as_string() == "stream-interpolation-b-to-a"
            assert message.properties is not None
            assert message.properties.content_type == "text/plain"
            assert message.properties.content_encoding == "utf-8"
            assert message.properties.message_id == "b-to-a-message-id"
            assert message.properties.correlation_id == "b-to-a-correlation-id"
            assert message.application_properties is not None
            properties = message.application_properties.value
            assert properties["x-string"] == "b-value"
            assert properties["x-bool"] is True
            assert properties["x-int"] == 7
        finally:
            with contextlib.suppress(Exception):  # cleanup must not mask a failure
                management.queue(stream_name).delete()
