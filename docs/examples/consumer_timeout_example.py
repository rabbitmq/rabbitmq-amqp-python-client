"""Consumer timeout: ``rabbitmq:consumer-timeout`` and ``on_delivery_release`` (step_130).

Run against a local RabbitMQ 4.3+ broker::

    PYTHONPATH=. .venv/bin/python docs/examples/consumer_timeout_example.py

RabbitMQ limits how long a consumer may hold a delivery unsettled before the
broker takes it back and blocks further delivery to that link, until the
client acknowledges the release. This client surfaces that as
``quorum().consumer_timeout(...)`` (the ``rabbitmq:consumer-timeout`` attach
property) plus ``quorum().on_delivery_release(handler)``, which is called once
per delivery the broker force-releases this way — ``handler(context, message)``
is given a :class:`TimeoutContext`, whose only valid settlement is
``accept()``, and calling it is what lifts the broker's block.

This script attaches a consumer with a short ``consumer_timeout`` and an
``on_delivery_release`` handler that logs the release and accepts it,
publishes one message whose handler deliberately sleeps past that timeout on
its first delivery (settling normally afterward), observes the release fire
and unlock the consumer, then publishes a second message and confirms it
still arrives before closing everything down.
"""

from __future__ import annotations

import logging
import time
import uuid

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    ConsumerError,
    Context,
    Message,
    QueueType,
    TimeoutContext,
)

#: The consumer's own rabbitmq:consumer-timeout, in milliseconds and seconds.
CONSUMER_TIMEOUT_MS = 3_000
CONSUMER_TIMEOUT_SECONDS = CONSUMER_TIMEOUT_MS / 1_000

#: How long the example waits for something it expects to happen.
TIMEOUT_SECONDS = 20.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def consumer_timeout() -> None:
    """Show a timed-out delivery released by the broker, and the consumer unlocked afterward."""
    connection = Connection(ConnectionParameters(container_id=f"example-consumer-timeout-{uuid.uuid4().hex[:8]}"))
    name = f"example-consumer-timeout-{uuid.uuid4().hex[:8]}"
    try:
        connection.management().queue(name).type(QueueType.QUORUM).declare()
        logger.info("declared quorum queue %r", name)

        released: list[tuple[TimeoutContext, Message]] = []
        second_message: list[Message] = []
        timeout_triggered = False

        def on_delivery_release(context: TimeoutContext, message: Message) -> None:
            logger.info("broker released %r past the %.1fs consumer-timeout; accepting", message.body_as_string(), CONSUMER_TIMEOUT_SECONDS)
            context.accept()
            released.append((context, message))

        def on_message(context: Context, message: Message) -> None:
            nonlocal timeout_triggered
            if not timeout_triggered:
                timeout_triggered = True
                logger.info(
                    "holding %r unsettled for %.1fs to trigger the consumer-timeout",
                    message.body_as_string(),
                    CONSUMER_TIMEOUT_SECONDS + 2,
                )
                time.sleep(CONSUMER_TIMEOUT_SECONDS + 2)
                try:
                    context.accept()
                except ConsumerError:
                    logger.info("as expected: the broker had already released this delivery")
                return
            context.accept()
            second_message.append(message)
            logger.info("second message %r arrived and was accepted normally", message.body_as_string())

        consumer = (
            connection.consumer_builder()
            .queue(name)
            .quorum()
            .consumer_timeout(CONSUMER_TIMEOUT_MS)
            .on_delivery_release(on_delivery_release)
            .builder()
            .message_handler(on_message)
            .initial_credits(1)
            .build()
        )
        logger.info("consumer attached with rabbitmq:consumer-timeout=%dms", CONSUMER_TIMEOUT_MS)

        try:
            publisher = connection.publisher_builder().queue(name).build()
            try:
                publisher.publish(Message("held-too-long"), timeout=TIMEOUT_SECONDS)

                deadline = time.monotonic() + TIMEOUT_SECONDS
                while not released and time.monotonic() < deadline:
                    time.sleep(0.05)
                if not released:
                    raise TimeoutError(f"the broker never released the timed-out delivery within {TIMEOUT_SECONDS:g}s")
                logger.info("on_delivery_release fired; the broker's block on this link should now be lifted")

                publisher.publish(Message("second"), timeout=TIMEOUT_SECONDS)
                deadline = time.monotonic() + TIMEOUT_SECONDS
                while not second_message and time.monotonic() < deadline:
                    time.sleep(0.05)
                if not second_message:
                    raise TimeoutError("the consumer was never unlocked: the second message never arrived")
                logger.info("confirmed: the consumer is unlocked and receiving normally again")
            finally:
                publisher.close()
        finally:
            consumer.close()

        connection.management().queue(name).delete()
        logger.info("deleted the queue %r", name)
    finally:
        connection.close()


if __name__ == "__main__":
    consumer_timeout()
