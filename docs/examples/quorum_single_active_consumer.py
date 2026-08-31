"""Single active consumer notifications on a quorum queue (step_090 §6).

Run against a local RabbitMQ 4.3+ broker::

    PYTHONPATH=. .venv/bin/python docs/examples/quorum_single_active_consumer.py

The queue is declared with ``single_active_consumer(True)``, so the broker feeds
exactly one of its consumers at a time and tells every one of them where it
stands by setting ``rabbitmq:active`` on a ``flow`` sent to its link. Two
consumers are then built on it, each registering a
``single_active_consumer_state_changed`` handler through the quorum sub-builder:

1. the first one is told ``is_active=True``, the second one ``is_active=False``;
2. a few messages are published, and only the active consumer's message handler
   sees them;
3. the active consumer is closed, at which point the standby one is told
   ``is_active=True`` and starts receiving what is published next.

Both consumers live in this one process, which the protocol does not care about —
the broker picks the active one per link, not per connection.
"""

from __future__ import annotations

import logging
import queue
import uuid

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    Consumer,
    Context,
    Message,
)

#: How long the example waits for a notification or a delivery.
TIMEOUT_SECONDS = 15.0

#: Messages published to each of the two consumers in turn.
BATCH = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class Role:
    """One consumer's view of itself: its statuses, and what it received."""

    def __init__(self, label: str) -> None:
        """Start recording for the consumer named ``label``."""
        self.label = label
        self.statuses: queue.Queue[bool] = queue.Queue()
        self.bodies: queue.Queue[str] = queue.Queue()

    def on_state_changed(self, consumer: Consumer, is_active: bool) -> None:
        """Log and record one active/standby status the broker reported."""
        logger.info("%s: is_active=%s", self.label, is_active)
        self.statuses.put(is_active)

    def on_message(self, context: Context, message: Message) -> None:
        """Log, accept and record one delivery."""
        body = message.body_as_string()
        logger.info("%s: received %r", self.label, body)
        self.bodies.put(body)
        context.accept()

    def next_status(self) -> bool:
        """Return the next status reported, or raise once the wait times out."""
        return self.statuses.get(timeout=TIMEOUT_SECONDS)

    def drain(self, count: int) -> list[str]:
        """Return the next ``count`` bodies received, or raise while waiting."""
        return [self.bodies.get(timeout=TIMEOUT_SECONDS) for _ in range(count)]


def single_active_consumer() -> None:
    """Watch a promotion happen on a single-active-consumer quorum queue."""
    connection = Connection(ConnectionParameters(container_id=f"example-sac-{uuid.uuid4().hex[:8]}"))
    name = f"example-sac-{uuid.uuid4().hex[:8]}"
    first, second = Role("consumer-1"), Role("consumer-2")
    try:
        connection.management().queue(name).single_active_consumer(True).quorum().queue().declare()
        logger.info("declared the single-active-consumer quorum queue %r", name)

        consumers = [
            connection.consumer_builder()
            .queue(name)
            .quorum()
            .single_active_consumer_state_changed(role.on_state_changed)
            .builder()
            .message_handler(role.on_message)
            .initial_credits(1)
            .build()
            for role in (first, second)
        ]
        logger.info("consumer-1 active=%s, consumer-2 active=%s", first.next_status(), second.next_status())

        publisher = connection.publisher_builder().queue(name).build()
        try:
            for index in range(BATCH):
                publisher.publish(Message(f"before-{index}"), timeout=TIMEOUT_SECONDS)
            logger.info("consumer-1 got %s while consumer-2 stood by", first.drain(BATCH))

            logger.info("closing consumer-1, which must promote consumer-2")
            consumers[0].close()
            logger.info("consumer-2 is now active=%s", second.next_status())

            for index in range(BATCH):
                publisher.publish(Message(f"after-{index}"), timeout=TIMEOUT_SECONDS)
            logger.info("consumer-2 got %s", second.drain(BATCH))
        finally:
            publisher.close()
            for consumer in consumers:
                consumer.close()
        connection.management().queue(name).delete()
    finally:
        connection.close()


if __name__ == "__main__":
    single_active_consumer()
