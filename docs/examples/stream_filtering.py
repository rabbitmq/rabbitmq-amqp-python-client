"""Stream consumption and filtering (step_080 §6).

Run against a local RabbitMQ 4.2+ broker::

    PYTHONPATH=. .venv/bin/python docs/examples/stream_filtering.py

A stream queue is declared and filled with messages that alternate
``properties.subject`` between two values and the application property
``region`` between two others, each one also tagged with the
``x-stream-filter-value`` message annotation the bloom filter reads. Three
consumers then read the same stream from ``offset(FIRST)``, each narrowing it a
different way:

1. ``filter().subject(...).property("region", ...)`` — AMQP filter expressions,
   which the broker evaluates exactly against the message's own sections, so the
   consumer sees only the messages matching both;
2. ``filter().sql(...)`` — the same condition as one broker-evaluated SQL
   expression (RabbitMQ 4.2+). The script reports whether this broker really
   enforces it: a broker that does not recognise the filter ignores it and
   delivers everything instead of refusing the attach;
3. ``filter_values(...)`` — the bloom filter, which is cheap and approximate:
   every tagged message is delivered, but a non-matching one may be too.

Nothing here is publisher-side API: tagging a message is a plain
``MessageAnnotations`` write. The annotation key must be a :class:`Symbol` —
RabbitMQ refuses a message-annotations map keyed by strings.
"""

from __future__ import annotations

import logging
import queue
import uuid

from src import (
    ApplicationProperties,
    Connection,
    ConnectionParameters,
    Consumer,
    Context,
    Message,
    MessageAnnotations,
    Properties,
    StreamOffsetSpecification,
    Symbol,
)
from src.constants import STREAM_FILTER_VALUE_ANNOTATION

#: How long the example waits for the deliveries a filter should let through.
TIMEOUT_SECONDS = 15.0

#: Messages published to the stream, alternating both fields.
BATCH = 12

#: The subject and region combination every filter below selects.
WANTED_SUBJECT = "orders"
WANTED_REGION = "emea"

#: The same condition as an AMQP SQL filter expression (RabbitMQ 4.2+).
WANTED_SQL = f"properties.subject = '{WANTED_SUBJECT}' AND region = '{WANTED_REGION}'"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class Collector:
    """Accepts every delivery and records its body."""

    def __init__(self, label: str) -> None:
        """Start collecting for the consumer named ``label``."""
        self.label = label
        self.bodies: queue.Queue[str] = queue.Queue()

    def on_message(self, context: Context, message: Message) -> None:
        """Log, accept and record one delivery."""
        body = message.body_as_string()
        logger.info("%s: received %r", self.label, body)
        self.bodies.put(body)
        context.accept()

    def drain(self, count: int) -> list[str]:
        """Return the next ``count`` bodies received, or raise while waiting."""
        return [self.bodies.get(timeout=TIMEOUT_SECONDS) for _ in range(count)]

    def drained(self) -> list[str]:
        """Return everything received so far, without waiting for more."""
        received = []
        while not self.bodies.empty():
            received.append(self.bodies.get_nowait())
        return received


def stream_messages() -> list[Message]:
    """Build the batch, alternating subject and region and tagging every message."""
    messages = []
    for index in range(BATCH):
        subject = WANTED_SUBJECT if index % 2 == 0 else "invoices"
        region = WANTED_REGION if index % 3 == 0 else "apac"
        messages.append(
            Message(
                f"m-{index}-{subject}-{region}",
                properties=Properties(subject=subject),
                application_properties=ApplicationProperties({"region": region}),
                # The bloom filter's publisher side (§2.1): one annotation, no
                # publisher API of its own.
                message_annotations=MessageAnnotations({Symbol(STREAM_FILTER_VALUE_ANNOTATION): subject}),
            )
        )
    return messages


def wanted_bodies(messages: list[Message]) -> list[str]:
    """The bodies of ``messages`` matching both the subject and the region."""
    return [
        message.body_as_string()
        for message in messages
        if message.properties is not None
        and message.properties.subject == WANTED_SUBJECT
        and message.application_properties is not None
        and message.application_properties.value["region"] == WANTED_REGION
    ]


def tagged_bodies(messages: list[Message]) -> list[str]:
    """The bodies of ``messages`` tagged with the wanted filter value."""
    return [
        message.body_as_string()
        for message in messages
        if message.properties is not None and message.properties.subject == WANTED_SUBJECT
    ]


def stream_filtering() -> None:
    """Publish a mixed batch to a stream, then read it back three filtered ways."""
    connection = Connection(ConnectionParameters(container_id=f"example-stream-{uuid.uuid4().hex[:8]}"))
    name = f"example-stream-{uuid.uuid4().hex[:8]}"
    try:
        connection.management().queue(name).stream().queue().declare()
        logger.info("declared the stream queue %r", name)

        messages = stream_messages()
        publisher = connection.publisher_builder().queue(name).build()
        try:
            for message in messages:
                publisher.publish(message, timeout=TIMEOUT_SECONDS)
        finally:
            publisher.close()
        expected = wanted_bodies(messages)
        logger.info("published %d messages, of which %s match both fields", len(messages), expected)

        properties_filter = Collector("properties-filter")
        sql_filter = Collector("sql-filter")
        bloom_filter = Collector("bloom-filter")
        consumers = [
            _consumer(connection, name, properties_filter, "properties"),
            _consumer(connection, name, sql_filter, "sql"),
            _consumer(connection, name, bloom_filter, "bloom"),
        ]
        try:
            got = properties_filter.drain(len(expected))
            logger.info("the properties/application-properties filter selected %s", got)
            assert got == expected, f"expected {expected}, got {got}"

            sql_got = sql_filter.drain(len(expected))
            if len(sql_got) == len(expected) and sql_filter.bodies.empty():
                logger.info("this broker enforces amqp:sql-filter: it selected %s", sql_got)
            else:
                # step_080 §6 point 4: an unrecognised filter descriptor is
                # ignored rather than refused, so the consumer sees everything.
                logger.warning("this broker does not enforce amqp:sql-filter: it delivered %s", sql_got)

            tagged = tagged_bodies(messages)
            bloom_got = bloom_filter.drain(len(tagged))
            logger.info("the bloom filter delivered %s of the %d tagged messages", bloom_got, len(tagged))
            # False positives are allowed, false negatives are not (§2).
            assert set(tagged) <= set(bloom_got + bloom_filter.drained())

            logger.info("the last offset the properties-filter consumer saw is %s", consumers[0].last_stream_offset)
        finally:
            for consumer in consumers:
                consumer.close()
        connection.management().queue(name).delete()
        logger.info("deleted the stream queue %r", name)
    finally:
        connection.close()


def _consumer(connection: Connection, name: str, collector: Collector, kind: str) -> Consumer:
    """Build one consumer on ``name``, filtered the way ``kind`` names."""
    stream = (
        connection.consumer_builder()
        .queue(name)
        .message_handler(collector.on_message)
        .stream()
        .offset(StreamOffsetSpecification.FIRST)
    )
    if kind == "properties":
        stream.filter().subject(WANTED_SUBJECT).property("region", WANTED_REGION)
    elif kind == "sql":
        stream.filter().sql(WANTED_SQL)
    else:
        stream.filter_values(WANTED_SUBJECT)
    return stream.builder().build()


if __name__ == "__main__":
    stream_filtering()
