"""Consuming: a message handler, credit, and pausing the flow (step_030 §6).

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/consumer_example.py

A consumer is a receiver link plus a ``message_handler``. The link is granted
``initial_credits`` when it attaches, and the client re-grants one credit every
time a delivery is settled, so at most that many messages are ever outstanding.
The handler runs on the client's delivery thread and is handed a
:class:`~rabbitmq_amqp_python_client.Context` alongside the message; calling
``context.accept()`` settles the delivery as ``accepted``, which is what tells
the broker it may drop it. ``discard()`` and ``requeue()`` are the other two
choices, and doing nothing leaves the delivery unsettled — visible as
``consumer.unsettled_message_count``.

The script then shows ``pause()``/``unpause()``, which is flow control and not
cancellation: ``pause()`` sends a ``flow`` holding the link at zero credit, so
the broker stops pushing and the messages published next simply stay queued.
``unpause()`` restores the credit in one go and they arrive immediately. The
link stays attached throughout — only ``close()`` detaches it, and it is called
before the connection so the detach is clean.
"""

from __future__ import annotations

import logging
import queue
import uuid

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    Context,
    Message,
)

#: How long the example waits for a delivery it expects.
TIMEOUT_SECONDS = 15.0

#: How long it waits to confirm a delivery it expects *not* to get.
QUIET_SECONDS = 2.0

#: Messages published before pausing, and again while paused.
BATCH = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class Counter:
    """Accepts every delivery, counts it, and records its body."""

    def __init__(self) -> None:
        """Start with an empty count."""
        self.count = 0
        self.bodies: queue.Queue[str] = queue.Queue()

    def on_message(self, context: Context, message: Message) -> None:
        """Count, log and accept one delivery."""
        self.count += 1
        body = message.body_as_string()
        logger.info("received #%d %r (delivery-id %d)", self.count, body, context.delivery_id)
        self.bodies.put(body)
        context.accept()

    def drain(self, count: int) -> list[str]:
        """Return the next ``count`` bodies received, or raise once the wait times out."""
        return [self.bodies.get(timeout=TIMEOUT_SECONDS) for _ in range(count)]

    def nothing_arrives(self) -> bool:
        """Whether no delivery shows up within :data:`QUIET_SECONDS`."""
        try:
            self.bodies.get(timeout=QUIET_SECONDS)
        except queue.Empty:
            return True
        return False


def consumer_example() -> None:
    """Consume a batch, pause over a second batch, then unpause and consume it."""
    connection = Connection(ConnectionParameters(container_id=f"example-consumer-{uuid.uuid4().hex[:8]}"))
    name = f"example-consumer-{uuid.uuid4().hex[:8]}"
    counter = Counter()
    try:
        connection.management().queue(name).declare()
        logger.info("declared the queue %r", name)

        consumer = connection.consumer_builder().queue(name).message_handler(counter.on_message).build()
        logger.info("consuming from %r with %d initial credits", consumer.queue, consumer.initial_credits)

        publisher = connection.publisher_builder().queue(name).build()
        try:
            for index in range(BATCH):
                publisher.publish(Message(f"before-pause-{index}"), timeout=TIMEOUT_SECONDS)
            logger.info("the handler got %s", counter.drain(BATCH))
            logger.info("%d message(s) unsettled, %d accepted so far", consumer.unsettled_message_count, counter.count)

            consumer.pause()
            logger.info("paused: is_paused=%s, so the broker holds at zero credit", consumer.is_paused)
            for index in range(BATCH):
                publisher.publish(Message(f"while-paused-{index}"), timeout=TIMEOUT_SECONDS)
            if counter.nothing_arrives():
                logger.info("nothing was delivered while paused, as expected")
            else:
                logger.warning("a delivery arrived while paused — it was already on the wire")

            consumer.unpause()
            logger.info("unpaused: is_paused=%s", consumer.is_paused)
            logger.info("the handler got %s", counter.drain(BATCH))
        finally:
            publisher.close()
            consumer.close()
            logger.info("closed the publisher and the consumer after %d accepted message(s)", counter.count)

        connection.management().queue(name).delete()
        logger.info("deleted the queue %r", name)
    finally:
        connection.close()


if __name__ == "__main__":
    consumer_example()
