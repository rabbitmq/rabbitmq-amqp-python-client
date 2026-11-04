"""One shared ``Connection``, one ``Publisher`` and one ``Consumer`` per thread.

Run against a local RabbitMQ broker::

    PYTHONPATH=. .venv/bin/python docs/examples/multithreaded_example.py

A single :class:`Connection` can be shared by every thread in the process: its
state and its socket writes are guarded by internal locks, and one background
thread reads the inbound frames. A :class:`Publisher` or :class:`Consumer`
serialises its own calls too, so sharing one is *safe*, but it gains no
throughput. The recommended layout is therefore:

* one ``Connection`` for the whole process,
* one ``Publisher`` per publishing thread,
* one ``Consumer`` per consuming thread (its handler runs on the client's own
  delivery thread, so state it shares with your code needs a lock).

This script starts ``WORKERS`` threads. Each builds its own publisher and
consumer on the shared connection, publishes ``MESSAGES_PER_WORKER`` messages
to a shared queue, and waits until the queue's consumers have received
everything.
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

#: Worker threads; each owns one publisher and one consumer.
WORKERS = 4

#: Messages each worker publishes.
MESSAGES_PER_WORKER = 25

#: How long the example waits for something it expects to happen.
TIMEOUT_SECONDS = 30.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


class Received:
    """Thread-safe count of the deliveries every consumer has received."""

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


def worker(
    connection: Connection,
    queue: str,
    index: int,
    received: Received,
    total: int,
    errors: list[BaseException],
) -> None:
    """Publish and consume on ``queue`` using this thread's own publisher and consumer."""
    try:
        consumer = connection.consumer_builder().queue(queue).message_handler(received).build()
        try:
            publisher = connection.publisher_builder().queue(queue).build()
            try:
                for number in range(MESSAGES_PER_WORKER):
                    publisher.publish(Message(f"worker-{index}-message-{number}"), timeout=TIMEOUT_SECONDS)
            finally:
                publisher.close()

            # Keep this thread's consumer attached until everything arrived,
            # so no message is stranded when the last consumer closes.
            deadline = time.monotonic() + TIMEOUT_SECONDS
            while received.count < total and time.monotonic() < deadline:
                time.sleep(0.05)
        finally:
            consumer.close()
    except BaseException as error:  # noqa: BLE001 - reported to the main thread
        logger.exception("worker %d failed", index)
        errors.append(error)


def multithreaded() -> None:
    """Share one connection between several threads, each with its own publisher and consumer."""
    connection = Connection(ConnectionParameters(container_id=f"example-multithreaded-{uuid.uuid4().hex[:8]}"))
    queue = f"example-multithreaded-{uuid.uuid4().hex[:8]}"
    try:
        connection.management().queue(queue).type(QueueType.QUORUM).declare()
        logger.info("declared quorum queue %r", queue)

        total = WORKERS * MESSAGES_PER_WORKER
        received = Received()
        errors: list[BaseException] = []
        threads = [
            threading.Thread(
                target=worker,
                args=(connection, queue, index, received, total, errors),
                name=f"worker-{index}",
            )
            for index in range(WORKERS)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if errors:
            raise RuntimeError(f"{len(errors)} worker(s) failed") from errors[0]
        logger.info("%d workers published and received %d/%d messages", WORKERS, received.count, total)
        if received.count != total:
            raise RuntimeError(f"expected {total} messages, received {received.count}")

        connection.management().queue(queue).delete()
        logger.info("deleted the queue %r", queue)
    finally:
        connection.close()


if __name__ == "__main__":
    multithreaded()
