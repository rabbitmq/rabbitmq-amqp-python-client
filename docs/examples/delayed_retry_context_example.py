"""Per-message delayed retry: ``context.delayed_retry(...)`` (step_120 §1-§2).

Run against a local RabbitMQ 4.3+ broker::

    PYTHONPATH=. .venv/bin/python docs/examples/delayed_retry_context_example.py

``context.delayed_retry(delay, delivery_failed=False)`` is sugar over
``context.requeue({"x-opt-delivery-time": now_ms + delay}, delivery_failed)``:
it stamps a per-message redelivery hint directly on the message being
requeued, so the queue holds that one redelivery back for at least ``delay``
before trying again. Unlike ``delayed_retry_example.py``'s queue-level
``x-delayed-retry-type``/``delayed_retry_min``/``delayed_retry_max`` (which
apply to every redelivery on that queue, and require the queue to be declared
with them up front), this mechanism needs **no** queue-level configuration at
all — it works on a plain quorum queue, and the delay is chosen fresh on each
call, message by message.

This script declares a plain quorum queue (no ``x-delayed-retry-*``
arguments), publishes one message, and has the handler call
``delayed_retry(...)`` on the first delivery and ``accept()`` on the second —
then measures and reports the elapsed time between the two, which must be at
least the requested delay.
"""

from __future__ import annotations

import logging
import time
import uuid

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    Context,
    Message,
    QueueType,
)

#: How long the second delivery is held back, in milliseconds and in seconds.
DELAY_MS = 3_000
DELAY_SECONDS = DELAY_MS / 1_000

#: How long the example waits for a delivery it expects.
TIMEOUT_SECONDS = 15.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def delayed_retry_context() -> None:
    """Show one message's redelivery held back by its own ``delayed_retry`` call."""
    connection = Connection(ConnectionParameters(container_id=f"example-delayed-retry-context-{uuid.uuid4().hex[:8]}"))
    name = f"example-delayed-retry-context-{uuid.uuid4().hex[:8]}"
    try:
        # A plain quorum queue: no x-delayed-retry-* arguments at all — the
        # delay this example asks for comes entirely from the per-message
        # x-opt-delivery-time annotation delayed_retry() stamps on.
        connection.management().queue(name).type(QueueType.QUORUM).declare()
        logger.info("declared plain quorum queue %r (no queue-level delayed-retry configuration)", name)

        arrived_at: list[float] = []

        def on_message(context: Context, message: Message) -> None:
            arrived_at.append(time.monotonic())
            if len(arrived_at) == 1:
                logger.info("first delivery %r; delaying its retry by %dms", message.body_as_string(), DELAY_MS)
                context.delayed_retry(DELAY_MS)
            else:
                logger.info("redelivery %r arrived; accepting", message.body_as_string())
                context.accept()

        consumer = connection.consumer_builder().queue(name).message_handler(on_message).initial_credits(1).build()
        try:
            publisher = connection.publisher_builder().queue(name).build()
            try:
                publisher.publish(Message("retry-me"), timeout=TIMEOUT_SECONDS)
            finally:
                publisher.close()

            deadline = time.monotonic() + TIMEOUT_SECONDS
            while len(arrived_at) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            if len(arrived_at) < 2:
                raise TimeoutError(f"the redelivery never arrived within {TIMEOUT_SECONDS:g}s")

            elapsed = arrived_at[1] - arrived_at[0]
            logger.info("the redelivery arrived after %.2fs (requested delay was %.2fs)", elapsed, DELAY_SECONDS)
            assert elapsed >= DELAY_SECONDS * 0.5, (
                f"delayed_retry should have held the redelivery back, took {elapsed:.2f}s"
            )
        finally:
            consumer.close()

        connection.management().queue(name).delete()
        logger.info("deleted the queue %r", name)
    finally:
        connection.close()


if __name__ == "__main__":
    delayed_retry_context()
