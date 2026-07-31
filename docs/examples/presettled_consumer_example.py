"""Presettled consumption: at-most-once delivery with no dispositions (step_060 §6).

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/presettled_consumer_example.py

``consumer_builder().presettled()`` attaches the receiver link with
``snd-settle-mode = settled``. The broker then considers every delivery settled
the moment it puts it on the wire: it forgets the message immediately, and the
client never sends a ``disposition`` back. That is the whole trade — one frame
per message instead of two, and no redelivery if the consumer dies holding one.

Two consequences show up directly in the API and are asserted below:

* the handler must not touch its :class:`~src.Context`.
  There is nothing left to settle, so ``accept()``/``discard()``/``requeue()``
  each raise :class:`~src.ConsumerError`. A presettled
  handler only reads the message. ``context.is_presettled`` says which mode it
  is running in, for a handler shared between both;
* ``consumer.unsettled_message_count`` stays at ``0`` for the consumer's whole
  life, because nothing is ever outstanding.

The second half of the script is the reconnection requirement of step_060 §6:
the socket is torn down underneath the connection, the default
``RecoveryConfiguration`` redials and re-attaches both links, and the *same*
publisher and consumer objects keep working — still presettled, still settling
nothing. Tearing down the live socket (``connection._socket.shutdown``) reaches
into the client on purpose; application code never does this.
"""

from __future__ import annotations

import logging
import queue
import socket
import time
import uuid

from src import (
    Connection,
    ConnectionParameters,
    ConnectionState,
    Consumer,
    Context,
    Message,
    Publisher,
    RecoveryConfiguration,
)

#: How long the example waits for a delivery it expects.
TIMEOUT_SECONDS = 15.0

#: How long it waits for auto-reconnection to finish.
RECOVERY_TIMEOUT_SECONDS = 60.0

POLL_INTERVAL_SECONDS = 0.05

#: Messages published before the disconnect, and again after it.
BATCH = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class Tally:
    """Counts and logs deliveries, and never settles one."""

    def __init__(self) -> None:
        """Start with an empty count."""
        self.count = 0
        self.bodies: queue.Queue[str] = queue.Queue()

    def on_message(self, context: Context, message: Message) -> None:
        """Count and log one delivery, without touching ``context``."""
        self.count += 1
        body = message.body_as_string()
        logger.info("received #%d %r (presettled=%s)", self.count, body, context.is_presettled)
        self.bodies.put(body)

    def drain(self, count: int) -> list[str]:
        """Return the next ``count`` bodies received, or raise once the wait times out."""
        return [self.bodies.get(timeout=TIMEOUT_SECONDS) for _ in range(count)]


def wait_for(connection: Connection, state: ConnectionState, timeout: float) -> bool:
    """Whether ``connection`` is seen in ``state`` within ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if connection.state is state:
            return True
        time.sleep(POLL_INTERVAL_SECONDS)
    return False


def publish_and_drain(publisher: Publisher, consumer: Consumer, tally: Tally, prefix: str) -> None:
    """Publish a batch, wait for it, and check nothing was left unsettled."""
    for index in range(BATCH):
        publisher.publish(Message(f"{prefix}-{index}"), timeout=TIMEOUT_SECONDS)
    logger.info("the handler got %s", tally.drain(BATCH))
    unsettled = consumer.unsettled_message_count
    logger.info("unsettled_message_count is %d after %d message(s)", unsettled, tally.count)
    assert unsettled == 0, f"a presettled consumer must never hold anything unsettled, got {unsettled}"


def presettled_consumer() -> None:
    """Consume a quorum queue presettled, across a forced disconnect."""
    connection = Connection(
        ConnectionParameters(
            container_id=f"example-presettled-{uuid.uuid4().hex[:8]}",
            on_unexpected_close=lambda error: logger.error("the connection died for good: %s", error),
            recovery_configuration=RecoveryConfiguration(),  # activated=True, topology=False
        )
    )
    name = f"example-presettled-{uuid.uuid4().hex[:8]}"
    tally = Tally()
    try:
        connection.management().queue(name).quorum().queue().declare()
        logger.info("declared the quorum queue %r", name)

        consumer = connection.consumer_builder().queue(name).message_handler(tally.on_message).presettled().build()
        logger.info("consuming from %r presettled=%s", consumer.queue, consumer.is_presettled)

        publisher = connection.publisher_builder().queue(name).build()
        try:
            publish_and_drain(publisher, consumer, tally, "before-drop")

            logger.info("simulating a network failure")
            connection._socket.shutdown(socket.SHUT_RDWR)
            if not wait_for(connection, ConnectionState.RECONNECTING, timeout=10.0):
                logger.warning("never observed RECONNECTING — detection and recovery both landed inside one poll")
            if not wait_for(connection, ConnectionState.OPEN, RECOVERY_TIMEOUT_SECONDS):
                logger.error("the connection never recovered")
                return
            logger.info("state is back to %s, links re-attached underneath both objects", connection.state.value)

            publish_and_drain(publisher, consumer, tally, "after-drop")
        finally:
            publisher.close()
            consumer.close()
            logger.info("closed the publisher and the consumer after %d message(s)", tally.count)

        connection.management().queue(name).delete()
        logger.info("deleted the quorum queue %r", name)
    finally:
        connection.close()


if __name__ == "__main__":
    presettled_consumer()
