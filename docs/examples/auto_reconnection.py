"""Auto-reconnection: surviving an unexpected disconnect (step_040 §7).

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/auto_reconnection.py

Two connections are opened in turn:

1. One with the **default** ``RecoveryConfiguration`` — recovery on, topology
   off. Its socket is torn down underneath it, and its ``state`` is watched
   moving ``OPEN → RECONNECTING → OPEN``. A publisher and a consumer built
   *before* the disconnect keep working afterwards without being rebuilt: that is
   connection recovery, which is not gated behind ``topology``.
2. One with ``topology=True``, which declares an **exclusive** queue — the broker
   deletes it the moment its owning connection dies. After the same forced
   disconnect the queue exists again, which only topology recovery can explain.

Tearing down the live socket (``connection._socket.shutdown``) reaches into the
client on purpose: it is the shortest way to make the frame reader see what a
network partition looks like. Application code never does this.
"""

from __future__ import annotations

import logging
import socket
import time
import uuid

from rabbitmq_amqp_python_client import (
    AMQPError,
    Connection,
    ConnectionParameters,
    ConnectionState,
    Context,
    ManagementError,
    Message,
    RecoveryConfiguration,
)

RECOVERY_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 0.05

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def kill_the_transport(connection: Connection) -> None:
    """Tear down the live socket, so the frame reader sees an unexpected drop."""
    logger.info("simulating a network failure")
    connection._socket.shutdown(socket.SHUT_RDWR)


def wait_for(connection: Connection, state: ConnectionState, timeout: float) -> bool:
    """Whether ``connection`` is seen in ``state`` within ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if connection.state is state:
            return True
        time.sleep(POLL_INTERVAL_SECONDS)
    return False


def wait_for_recovery(connection: Connection) -> bool:
    """Wait for the drop to be noticed, and only then for recovery to finish.

    Polling for ``OPEN`` straight after the disconnect would exit on its first
    check: the state still reads the ``OPEN`` from *before* the drop until the
    frame reader notices it (step_040 §8.2's pitfall).
    """
    if not wait_for(connection, ConnectionState.RECONNECTING, timeout=10.0):
        logger.warning("never observed RECONNECTING — detection and recovery both landed inside one poll")
    return wait_for(connection, ConnectionState.OPEN, RECOVERY_TIMEOUT_SECONDS)


def connection_recovery() -> None:
    """Show a publisher and a consumer surviving a drop with ``topology`` off."""
    logger.info("--- connection recovery, with the default RecoveryConfiguration ---")
    connection = Connection(
        ConnectionParameters(
            container_id=f"example-recovery-{uuid.uuid4().hex[:8]}",
            on_unexpected_close=lambda error: logger.error("the connection died for good: %s", error),
            recovery_configuration=RecoveryConfiguration(),  # activated=True, topology=False
        )
    )
    queue = f"example-recovery-{uuid.uuid4().hex[:8]}"
    try:
        connection.management().queue(queue).declare()
        received: list[str] = []

        def handler(context: Context, message: Message) -> None:
            received.append(message.body_as_string())
            context.accept()

        publisher = connection.publisher_builder().queue(queue).build()
        connection.consumer_builder().queue(queue).message_handler(handler).build()

        kill_the_transport(connection)
        if wait_for(connection, ConnectionState.RECONNECTING, timeout=10.0):
            logger.info("state is now %s", connection.state.value)
            try:
                publisher.publish(Message("during the gap"), timeout=1.0)
            except AMQPError as error:
                logger.info("a call made during the gap failed, as it must: %s", error)
        else:
            logger.warning("never observed RECONNECTING — detection and recovery both landed inside one poll")

        if not wait_for(connection, ConnectionState.OPEN, RECOVERY_TIMEOUT_SECONDS):
            logger.error("the connection never recovered")
            return
        logger.info("state is back to %s", connection.state.value)

        # The publisher and consumer objects are the originals: their links were
        # re-attached underneath them.
        publisher.publish(Message("after the drop"))
        for _ in range(100):
            if received:
                break
            time.sleep(POLL_INTERVAL_SECONDS)
        logger.info("the consumer received %r after the drop", received)
        connection.management().queue(queue).delete()
    finally:
        connection.close()


def topology_recovery() -> None:
    """Show an exclusive queue being recreated with ``topology`` on."""
    logger.info("--- topology recovery, with RecoveryConfiguration(topology=True) ---")
    connection = Connection(
        ConnectionParameters(
            container_id=f"example-topology-{uuid.uuid4().hex[:8]}",
            recovery_configuration=RecoveryConfiguration(topology=True),
        )
    )
    queue = f"example-exclusive-{uuid.uuid4().hex[:8]}"
    try:
        connection.management().queue(queue).exclusive().declare()
        logger.info("declared the exclusive queue %r, which the broker drops with its connection", queue)

        kill_the_transport(connection)
        if not wait_for_recovery(connection):
            logger.error("the connection never recovered")
            return

        try:
            info = connection.management().queue_info(queue)
        except ManagementError as error:
            logger.error("the queue is gone: %s", error)
            return
        logger.info("the queue %r exists again (exclusive=%s)", info.name, info.exclusive)
    finally:
        connection.close()


if __name__ == "__main__":
    connection_recovery()
    topology_recovery()
