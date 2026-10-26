"""Consumer priority: ``rabbitmq:priority`` (step_140).

Run against a local RabbitMQ 4.3+ broker::

    PYTHONPATH=. .venv/bin/python docs/examples/consumer_priority_example.py

When several consumers share a queue, RabbitMQ delivers to the highest-priority
one that has credit, and falls back to lower-priority ones only when the higher
ones run out of it. This client surfaces that as ``consumer_builder().priority(n)``
(the ``rabbitmq:priority`` attach property); the default is ``0``.

This script attaches two consumers to one queue — one with a high priority, one
with the default — publishes a batch, and prints how many messages each got. The
high-priority consumer receives them all, because it has credit for the whole
batch. Priority is a preference among consumers *with credit*, not a
reservation: give it a small ``initial_credits`` and never settle, and the rest
flow on to the other consumer.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    Context,
    Message,
    QueueType,
)

#: Messages published to the queue.
BATCH = 20

#: How long the example waits for something it expects to happen.
TIMEOUT_SECONDS = 20.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class Counter:
    """Thread-safe count of the deliveries one consumer received."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0

    def __call__(self, context: Context, message: Message) -> None:
        context.accept()
        with self._lock:
            self._count += 1

    @property
    def count(self) -> int:
        with self._lock:
            return self._count


def consumer_priority() -> None:
    """Show a high-priority consumer served ahead of a default-priority one."""
    connection = Connection(ConnectionParameters(container_id=f"example-consumer-priority-{uuid.uuid4().hex[:8]}"))
    name = f"example-consumer-priority-{uuid.uuid4().hex[:8]}"
    try:
        connection.management().queue(name).type(QueueType.QUORUM).declare()
        logger.info("declared quorum queue %r", name)

        low, high = Counter(), Counter()
        low_consumer = connection.consumer_builder().queue(name).message_handler(low).initial_credits(BATCH * 2).build()
        high_consumer = (
            connection.consumer_builder()
            .queue(name)
            .message_handler(high)
            .initial_credits(BATCH * 2)
            .priority(10)
            .build()
        )
        logger.info("attached a default-priority consumer and one with rabbitmq:priority=10")

        try:
            publisher = connection.publisher_builder().queue(name).build()
            try:
                for index in range(BATCH):
                    publisher.publish(Message(f"message-{index}"), timeout=TIMEOUT_SECONDS)
            finally:
                publisher.close()

            deadline = time.monotonic() + TIMEOUT_SECONDS
            while low.count + high.count < BATCH and time.monotonic() < deadline:
                time.sleep(0.05)
            logger.info(
                "high-priority consumer received %d, default-priority consumer received %d", high.count, low.count
            )
            if high.count != BATCH:
                raise RuntimeError(f"expected the high-priority consumer to receive all {BATCH} messages")
        finally:
            high_consumer.close()
            low_consumer.close()

        connection.management().queue(name).delete()
        logger.info("deleted the queue %r", name)
    finally:
        connection.close()


if __name__ == "__main__":
    consumer_priority()
