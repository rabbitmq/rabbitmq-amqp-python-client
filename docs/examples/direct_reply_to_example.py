"""Direct reply-to: RabbitMQ's request/reply pseudo-queue (step_060_consumer_strategy.md §7).

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/direct_reply_to_example.py

``consumer_builder().settle_strategy(ConsumerSettleStrategy.DIRECT_REPLY_TO)``
attaches a receiver link to no caller-supplied queue at all: the broker
dynamically generates a private, per-attach pseudo-queue address
(``/queues/amq.rabbitmq.reply-to.<opaque-suffix>``) and returns it in the
``attach`` reply, read back here as ``consumer.queue``. A caller who wants a
reply puts that address in an outgoing request's ``properties.reply_to``;
whoever answers just needs to publish to it — no dedicated, exclusive reply
queue to declare and clean up per requester. Like a presettled consumer, its
``Context`` methods all raise :class:`~rabbitmq_amqp_python_client.ConsumerError`: the broker considers
every delivery on this link already settled the instant it sends it.

This script plays both roles, on two separate connections:

* the **requester** (``requester_connection``) builds one ``DIRECT_REPLY_TO``
  consumer and never declares a queue of its own;
* the **responder** (``responder_connection``) declares an ordinary request
  queue, consumes it with a normal ``ExplicitSettle`` consumer, and replies to
  whatever address arrived in the request's ``reply_to`` — an ordinary
  anonymous :class:`~rabbitmq_amqp_python_client.Publisher` (step_020_publishers.md §3.3) also sends
  the request itself, targeting the request queue.

Because the pseudo-queue is scoped to the exact connection and session that
attached it (§3.3 point 5), it does not survive a reconnect: the second half of
this script forces the requester's socket down, lets auto-reconnection redial
and re-attach, and shows ``consumer.queue`` reading back a *different*
broker-generated address afterward — a caller must always re-read it after
recovery, never cache it. Tearing down the live socket
(``connection._socket.shutdown``) reaches into the client on purpose;
application code never does this.
"""

from __future__ import annotations

import logging
import queue
import socket
import time
import uuid

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    ConnectionState,
    Consumer,
    ConsumerSettleStrategy,
    Context,
    Message,
    Publisher,
    RecoveryConfiguration,
    queue_address,
)
from rabbitmq_amqp_python_client.wire import Properties

#: How long the example waits for a reply it expects.
TIMEOUT_SECONDS = 15.0

#: How long it waits for auto-reconnection to finish.
RECOVERY_TIMEOUT_SECONDS = 60.0

POLL_INTERVAL_SECONDS = 0.05

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class ReplyBox:
    """Records every reply the requester's direct-reply-to consumer receives."""

    def __init__(self) -> None:
        """Start with nothing received."""
        self.replies: queue.Queue[str] = queue.Queue()

    def on_message(self, context: Context, message: Message) -> None:
        """Log and record one reply, without touching ``context`` — it is presettled."""
        body = message.body_as_string()
        logger.info("requester received reply %r (presettled=%s)", body, context.is_presettled)
        self.replies.put(body)

    def drain_one(self) -> str:
        """Return the next reply body, or raise once the wait times out."""
        return self.replies.get(timeout=TIMEOUT_SECONDS)


def wait_for(connection: Connection, state: ConnectionState, timeout: float) -> bool:
    """Whether ``connection`` is seen in ``state`` within ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if connection.state is state:
            return True
        time.sleep(POLL_INTERVAL_SECONDS)
    return False


def build_responder(connection: Connection, request_queue: str) -> Consumer:
    """Attach the queue-bound consumer that answers every request with a reply."""

    def on_request(context: Context, message: Message) -> None:
        reply_to = message.properties.reply_to if message.properties else None
        if not reply_to:
            logger.warning("a request with no reply_to arrived; discarding it")
            context.discard()
            return
        logger.info("responder received request %r, replying to %r", message.body_as_string(), reply_to)
        reply_publisher = connection.publisher_builder().build()  # anonymous: reply_to is per-request
        try:
            reply_publisher.publish(
                Message(
                    f"pong-for-{message.body_as_string()}",
                    properties=Properties(
                        to=reply_to,
                        correlation_id=message.properties.message_id if message.properties else None,
                    ),
                ),
                timeout=TIMEOUT_SECONDS,
            )
        finally:
            reply_publisher.close()
        context.accept()

    return connection.consumer_builder().queue(request_queue).message_handler(on_request).build()


def send_request(connection: Connection, request_queue: str, reply_to: str, body: str) -> None:
    """Publish one request naming ``reply_to``, from a fresh anonymous publisher.

    Anonymous (step_020_publishers.md §3.3) so each request can carry its own
    ``to`` alongside its ``reply_to`` — a queue-bound publisher could not set
    the former, since its target is fixed at ``build()`` time.
    """
    publisher: Publisher = connection.publisher_builder().build()
    try:
        publisher.publish(
            Message(
                body,
                properties=Properties(
                    message_id=str(uuid.uuid4()),
                    to=queue_address(request_queue),
                    reply_to=reply_to,
                ),
            ),
            timeout=TIMEOUT_SECONDS,
        )
    finally:
        publisher.close()
    logger.info("requester sent %r to %r with reply_to=%r", body, request_queue, reply_to)


def direct_reply_to() -> None:
    """Round-trip a request/reply exchange over direct-reply-to, across a forced disconnect."""
    requester_connection = Connection(
        ConnectionParameters(
            container_id=f"example-direct-reply-to-requester-{uuid.uuid4().hex[:8]}",
            on_unexpected_close=lambda error: logger.error("the requester connection died for good: %s", error),
            recovery_configuration=RecoveryConfiguration(),  # activated=True, topology=False
        )
    )
    responder_connection = Connection(
        ConnectionParameters(container_id=f"example-direct-reply-to-responder-{uuid.uuid4().hex[:8]}")
    )
    request_queue = f"example-direct-reply-to-{uuid.uuid4().hex[:8]}"
    reply_box = ReplyBox()
    try:
        responder_connection.management().queue(request_queue).declare()
        logger.info("declared the request queue %r", request_queue)

        responder = build_responder(responder_connection, request_queue)
        requester = (
            requester_connection.consumer_builder()
            .message_handler(reply_box.on_message)
            .settle_strategy(ConsumerSettleStrategy.DIRECT_REPLY_TO)
            .build()
        )
        try:
            first_address = requester.queue
            assert first_address is not None, "build() only returns once DIRECT_REPLY_TO's address is resolved"
            logger.info("requester attached to the broker-generated pseudo-queue %r", first_address)
            send_request(responder_connection, request_queue, first_address, "hello")
            logger.info("requester received %r", reply_box.drain_one())

            logger.info("simulating a network failure on the requester connection")
            requester_connection._socket.shutdown(socket.SHUT_RDWR)
            if not wait_for(requester_connection, ConnectionState.RECONNECTING, timeout=10.0):
                logger.warning("never observed RECONNECTING — detection and recovery both landed inside one poll")
            if not wait_for(requester_connection, ConnectionState.OPEN, RECOVERY_TIMEOUT_SECONDS):
                logger.error("the requester connection never recovered")
                return
            logger.info(
                "state is back to %s, the receiver link re-attached underneath the same Consumer object",
                requester_connection.state.value,
            )

            second_address = requester.queue
            assert second_address is not None, "the re-attach must have resolved a fresh address"
            logger.info("requester re-attached to a fresh pseudo-queue %r", second_address)
            assert second_address != first_address, (
                "a direct-reply-to pseudo-queue is session-scoped and must not survive a reconnect"
            )
            send_request(responder_connection, request_queue, second_address, "hello-again")
            logger.info("requester received %r", reply_box.drain_one())
        finally:
            requester.close()
            responder.close()
            logger.info("closed the requester and the responder")

        responder_connection.management().queue(request_queue).delete()
        logger.info("deleted the request queue %r", request_queue)
    finally:
        requester_connection.close()
        responder_connection.close()


if __name__ == "__main__":
    direct_reply_to()
