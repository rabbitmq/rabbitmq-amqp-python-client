"""Why a message was rejected: reading RejectionDetails off the outcome (step_070 §6).

Run against a local broker; RabbitMQ 4.3+ to see the details themselves::

    PYTHONPATH=. .venv/bin/python docs/examples/rejection_reason_example.py

A quorum queue declared with ``max_length(5)`` and the ``reject-publish``
overflow strategy accepts its first five messages and rejects once it is over
the limit and nothing is being consumed. That makes it the cheapest way to see a
``REJECTED`` outcome on demand. Where exactly the line falls is the broker's
business, not the client's: a quorum queue checks the limit *before* enqueuing,
so the publish that takes it to ``max_length`` still succeeds and the rejections
start one message later. The script therefore publishes ``max_length`` messages,
which must all be accepted, and then keeps going until the first rejection
instead of asserting a fixed message number.

A rejection is **not** an error here: ``publish`` returns normally with
``outcome.state == OutcomeState.REJECTED`` rather than raising, because the
publish attempt itself succeeded — it is the routing that did not. Anything the
broker said about why is parsed out of the ``rejected`` disposition's
``error.info`` map into
:class:`~src.RejectionDetails`:

* ``reason`` — the broker's explanation, e.g. ``maximum queue length exceeded``;
* ``rejected_by_queue`` — which queue rejected it, which matters when the
  message was routed through an exchange to several queues.

Both fields are read independently and either may be ``None``: a pre-4.3 broker
populates neither, and the outcome then carries ``rejection_details=None``
altogether. The script says so explicitly rather than failing, which is what
client code should do too.
"""

from __future__ import annotations

import logging
import uuid

from src import (
    Connection,
    ConnectionParameters,
    Message,
    OutcomeState,
    OverflowStrategy,
    Publisher,
)

#: The queue's ``x-max-length``: the messages after this one have nowhere to go.
MAX_LENGTH = 5

#: How many overflowing publishes to try before giving up on seeing a rejection.
OVERFLOW_ATTEMPTS = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def publish_one(publisher: Publisher, body: str) -> OutcomeState:
    """Publish one message, log its outcome and any rejection details, return the state."""
    outcome = publisher.publish(Message(body)).outcome
    logger.info("%r -> %s", body, outcome.state.value)
    if outcome.state is not OutcomeState.REJECTED:
        return outcome.state

    details = outcome.rejection_details
    if details is None:
        # Pre-4.3 brokers reject without saying anything structured (§1).
        logger.warning("  the broker supplied no rejection details; raw error=%r", outcome.error)
        return outcome.state
    logger.info("  reason=%r", details.reason)
    logger.info("  rejected_by_queue=%r", details.rejected_by_queue)
    if details.reason is None or details.rejected_by_queue is None:
        logger.warning("  the broker supplied only part of the details")
    return outcome.state


def rejection_reason() -> None:
    """Fill a bounded quorum queue, then read why the overflowing publish was rejected."""
    connection = Connection(ConnectionParameters(container_id=f"example-rejection-{uuid.uuid4().hex[:8]}"))
    name = f"example-rejection-{uuid.uuid4().hex[:8]}"
    try:
        (
            connection.management()
            .queue(name)
            .max_length(MAX_LENGTH)
            .overflow_strategy(OverflowStrategy.REJECT_PUBLISH)
            .quorum()
            .queue()
            .declare()
        )
        logger.info("declared the quorum queue %r with max_length=%d and reject-publish", name, MAX_LENGTH)

        publisher = connection.publisher_builder().queue(name).build()
        try:
            for index in range(MAX_LENGTH):
                state = publish_one(publisher, f"fits-{index}")
                assert state is OutcomeState.ACCEPTED, f"message {index} should have been accepted, got {state}"
            logger.info("the queue is full at %d message(s); publishing until the broker pushes back", MAX_LENGTH)

            for attempt in range(1, OVERFLOW_ATTEMPTS + 1):
                if publish_one(publisher, f"one-too-many-{attempt}") is OutcomeState.REJECTED:
                    logger.info("the broker rejected overflowing publish %d of at most %d", attempt, OVERFLOW_ATTEMPTS)
                    break
            else:
                logger.warning("no publish was rejected in %d attempts over the limit", OVERFLOW_ATTEMPTS)
        finally:
            publisher.close()

        management = connection.management()
        logger.info("the queue settled at %d message(s)", management.queue_info(name).message_count)
        management.queue(name).delete()
        logger.info("deleted the queue %r", name)
    finally:
        connection.close()


if __name__ == "__main__":
    rejection_reason()
