"""Delayed messages on a quorum queue: publish with a delivery time, then consume.

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/quorum_publisher_delay_example.py

The queue is declared as a quorum queue with ``queue(name).quorum().queue().declare()``:
``quorum()`` sets ``x-queue-type`` to ``quorum`` and ``queue()`` returns to the
plain queue builder for the terminal ``declare()``.

``Message.DeliveryAfter(seconds)`` stamps each message with an
``x-opt-delivery-time`` annotation (milliseconds since the epoch) ``seconds``
ahead, so the quorum queue holds it back and only
delivers it once that time is reached. See
https://www.rabbitmq.com/docs/quorum-queues#delayed-messages

A queue publisher (``publisher_builder().queue(name)``) targets ``/queues/{name}``.
Every ``publish`` blocks until the broker settles the message. For a quorum queue
the ``ACCEPTED`` outcome means the message has been replicated to a majority of
the queue's members and written to disk, so it is safe to treat as durable.

A consumer is attached before publishing. Its handler logs the current datetime
for every delivery, accepts it, and sets an event once ``MESSAGE_COUNT`` messages
have arrived; the main thread waits on that event and then closes the consumer.

The publisher and the consumer are closed before the connection so their links
detach cleanly, and the queue is deleted at the end so the example leaves
nothing behind.
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime

from rabbitmq_amqp_python_client import Connection, ConnectionParameters, Context, Message

#: How many messages to publish.
MESSAGE_COUNT = 10

#: How long each message is held back by the broker before delivery.
DELAY_SECONDS = 60

#: How long to wait for all the messages to be received. The published messages
#: carry a delivery time :data:`DELAY_SECONDS` ahead, so this must be longer than that.
RECEIVE_TIMEOUT_SECONDS = DELAY_SECONDS + 30.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class Receiver:
    """Accepts every delivery, logs when it arrived, and signals once all have."""

    def __init__(self, expected: int) -> None:
        """Wait for ``expected`` deliveries."""
        self.expected = expected
        self.count = 0
        self.done = threading.Event()

    def on_message(self, context: Context, message: Message) -> None:
        """Log the receive time of one delivery and accept it."""
        self.count += 1
        logger.info(
            "received at %s: %r (%d/%d)",
            datetime.now().isoformat(),
            message.body_as_string(),
            self.count,
            self.expected,
        )
        context.accept()
        if self.count >= self.expected:
            self.done.set()


def quorum_publisher_delay_example() -> None:
    """Declare a quorum queue, publish to it, consume everything, then delete it."""
    queue_name = f"example-quorum-{uuid.uuid4().hex[:8]}"
    connection = Connection(ConnectionParameters(container_id=f"example-quorum-publisher-{uuid.uuid4().hex[:8]}"))
    try:
        management = connection.management()
        management.queue(queue_name).quorum().queue().declare()
        logger.info("declared the quorum queue %r", queue_name)

        receiver = Receiver(MESSAGE_COUNT)
        consumer = connection.consumer_builder().queue(queue_name).message_handler(receiver.on_message).build()
        publisher = connection.publisher_builder().queue(queue_name).build()
        try:
            for i in range(MESSAGE_COUNT):
                msgq = Message(f"message {i}").delivery_after(DELAY_SECONDS)
                result = publisher.publish(msgq)
                logger.info("%r -> %s", result.message.body_as_string(), result.outcome.state.value)

            if not receiver.done.wait(timeout=RECEIVE_TIMEOUT_SECONDS):
                logger.warning("only %d/%d message(s) received before the timeout", receiver.count, MESSAGE_COUNT)
        finally:
            publisher.close()
            consumer.close()
            logger.info("closed the publisher and the consumer after %d message(s)", receiver.count)

        info = management.queue_info(queue_name)
        logger.info("queue %r (%s) holds %d message(s)", queue_name, info.queue_type.value, info.message_count)

        management.queue(queue_name).delete()
        logger.info("deleted the queue %r", queue_name)
    finally:
        connection.close()


if __name__ == "__main__":
    quorum_publisher_delay_example()
