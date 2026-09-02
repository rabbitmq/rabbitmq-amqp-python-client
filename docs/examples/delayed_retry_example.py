"""Delayed retry on a quorum queue: a redelivery held back instead of instant
(step_001_management.md §5.4).

Run against a local RabbitMQ 4.3+ broker::

    PYTHONPATH=. .venv/bin/python docs/examples/delayed_retry_example.py

``QuorumQueueSpecification.delayed_retry_type(QuorumQueueDelayedRetryType.FAILED)``,
paired with ``delayed_retry_min``/``delayed_retry_max``, sets ``x-delayed-retry-type``,
``x-delayed-retry-min`` and ``x-delayed-retry-max`` on the declare. Once set, the
queue holds a message aside for that window before redelivering it, rather than
redelivering it the instant it comes back — but only for the *kind* of return
the configured type names. Of the outcomes ``Context`` can send
(step_030_consumers.md §4), only the two that leave a message eligible for
redelivery at all take part in this: ``context.requeue(delivery_failed=True)``,
which sends ``modified{delivery-failed=true, undeliverable-here=false}``, is a
*failed* return; ``context.requeue()`` (``delivery_failed=False``), which sends
``released``, is a *returned* one. (A terminal ``context.discard()`` — the
``rejected`` outcome — is always dropped outright and plays no part in this;
delayed retry only ever holds back a message that was going to be redelivered
anyway.)

* ``DISABLED`` (the default) never delays either kind;
* ``ALL`` delays both kinds;
* ``FAILED`` delays only a *failed* return;
* ``RETURNED`` delays only a *returned* one.

This script declares a queue with ``FAILED``, and shows both halves of that
distinction on the same queue: a failed return comes back only after
``delayed_retry_min`` has passed, while a returned one — not covered by
``FAILED`` — comes back right away, exactly as it would with delayed retry
disabled altogether.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    Context,
    ManagementError,
    Message,
    QuorumQueueDelayedRetryType,
)
from rabbitmq_amqp_python_client.management import STATUS_BAD_REQUEST

#: How long a redelivery is held back, in milliseconds and in seconds.
DELAYED_RETRY_MS = 3_000
DELAYED_RETRY_SECONDS = DELAYED_RETRY_MS / 1_000

#: How long the example waits for a delivery it expects.
TIMEOUT_SECONDS = 15.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def _consume_one_and_settle(
    connection: Connection, name: str, settle: Callable[[Context], None]
) -> tuple[Context, float]:
    """Attach a fresh consumer, receive the queue's one message, settle it, and
    return the context that was used plus when the delivery arrived.
    """
    delivered: list[tuple[Context, Message, float]] = []

    def on_message(context: Context, message: Message) -> None:
        delivered.append((context, message, time.monotonic()))

    consumer = connection.consumer_builder().queue(name).message_handler(on_message).initial_credits(1).build()
    try:
        deadline = time.monotonic() + TIMEOUT_SECONDS
        while not delivered and time.monotonic() < deadline:
            time.sleep(0.05)
        if not delivered:
            raise TimeoutError(f"no delivery arrived within {TIMEOUT_SECONDS:g}s")
        context, message, arrived_at = delivered[0]
        settle(context)
        return context, arrived_at
    finally:
        consumer.close()


def delayed_retry() -> None:
    """Show a failed return delayed by FAILED, and a returned one that is not."""
    connection = Connection(ConnectionParameters(container_id=f"example-delayed-retry-{uuid.uuid4().hex[:8]}"))
    name = f"example-delayed-retry-{uuid.uuid4().hex[:8]}"
    try:
        try:
            (
                connection.management()
                .queue(name)
                .quorum()
                .delayed_retry_type(QuorumQueueDelayedRetryType.FAILED)
                .delayed_retry_min(DELAYED_RETRY_MS)
                .delayed_retry_max(DELAYED_RETRY_MS)
                .queue()
                .declare()
            )
        except ManagementError as error:
            if error.status_code != STATUS_BAD_REQUEST:
                raise
            logger.warning("this broker does not support x-delayed-retry-* (needs RabbitMQ 4.3+); stopping here")
            return
        logger.info("declared %r with delayed_retry_type=FAILED, min=max=%dms", name, DELAYED_RETRY_MS)

        publisher = connection.publisher_builder().queue(name).build()
        try:
            publisher.publish(Message("failed-return"), timeout=TIMEOUT_SECONDS)
            _, failed_at = _consume_one_and_settle(
                connection, name, lambda context: context.requeue(delivery_failed=True)
            )
            logger.info("requeued %r as a failed return; waiting for the delayed redelivery", "failed-return")
            _, redelivered_at = _consume_one_and_settle(connection, name, lambda context: context.accept())
            elapsed = redelivered_at - failed_at
            logger.info(
                "the failed return came back after %.2fs (delayed_retry_min=%.2fs)", elapsed, DELAYED_RETRY_SECONDS
            )
            assert elapsed >= DELAYED_RETRY_SECONDS * 0.5, (
                f"FAILED should have delayed the failed return, took {elapsed:.2f}s"
            )

            publisher.publish(Message("returned"), timeout=TIMEOUT_SECONDS)
            _, returned_at = _consume_one_and_settle(connection, name, lambda context: context.requeue())
            logger.info("requeued %r as a returned delivery; not covered by FAILED", "returned")
            _, redelivered_at = _consume_one_and_settle(connection, name, lambda context: context.accept())
            elapsed = redelivered_at - returned_at
            logger.info("the returned delivery came back after %.2fs — not delayed", elapsed)
            assert elapsed < DELAYED_RETRY_SECONDS * 0.5, (
                f"FAILED should not have delayed the returned delivery, took {elapsed:.2f}s"
            )
        finally:
            publisher.close()

        connection.management().queue(name).delete()
        logger.info("deleted the queue %r", name)
    finally:
        connection.close()


if __name__ == "__main__":
    delayed_retry()
