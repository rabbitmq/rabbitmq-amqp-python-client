# type: ignore
"""
Quorum Queue Single Active Consumer (SAC) Notification Example
=============================================================

This example demonstrates the Single Active Consumer feature for Quorum Queues
in RabbitMQ 4.3+.  When multiple consumers subscribe to a SAC-enabled queue,
only one is "active" (receiving messages) at a time; the others are "standby".

RabbitMQ notifies each consumer of its active/standby status via an AMQP 1.0
FLOW frame carrying the ``rabbitmq:active`` boolean link-state property.
The ``QuorumConsumerOptions(sac_state_handler=...)`` callback lets your code
react to these transitions in real time.

Threading note
--------------
The qpid-proton transport is NOT thread-safe.  Each consumer must own its
connection exclusively.  In this example each consumer thread creates and
closes its own ``Connection`` without sharing it with any other thread.
The consumer threads are daemon threads, so they are killed automatically
when the main thread exits.

Prerequisites
-------------
- RabbitMQ 4.3+ with the Quorum Queue type.
- The queue must be declared with ``single_active_consumer=True``.

How to run
----------
Start RabbitMQ::

    docker run -it --rm -p 5672:5672 rabbitmq:4.3-management

Then run this script.  You should see both consumers attach, one becoming
ACTIVE and the other STANDBY.  Messages are published and only the active
consumer receives them.
"""

import threading
import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    Converter,
    Environment,
    Event,
    Message,
    OutcomeState,
    QuorumConsumerOptions,
    QuorumQueueSpecification,
)

QUEUE_NAME = "example-sac-quorum-queue"
MESSAGES_TO_PUBLISH = 20
BROKER_URI = "amqp://guest:guest@localhost:5672/"


class SACMessageHandler(AMQPMessagingHandler):
    """Message handler that counts and accepts every incoming message."""

    def __init__(self, name: str) -> None:
        super().__init__()
        self._name = name
        self._count = 0

    def on_amqp_message(self, event: Event) -> None:
        self._count += 1
        body = Converter.bytes_to_string(event.message.body)
        print(f"[{self._name}] received message #{self._count}: {body}")
        self.delivery_context.accept(event)

    def on_connection_closed(self, event: Event) -> None:
        print(f"[{self._name}] connection closed")

    def on_link_closed(self, event: Event) -> None:
        print(f"[{self._name}] link closed")


def sac_state_callback(consumer_name: str, is_active: bool) -> None:
    """Called each time RabbitMQ changes this consumer's SAC status."""
    status = "ACTIVE  ← receiving messages" if is_active else "STANDBY ← waiting"
    print(f"\n*** [{consumer_name}] SAC state changed → {status} ***\n")


def publish_messages(connection: Connection) -> None:
    """Publish a batch of test messages to the queue."""
    addr = AddressHelper.queue_address(QUEUE_NAME)
    publisher = connection.publisher(addr)
    print(f"\nPublishing {MESSAGES_TO_PUBLISH} messages …")
    for i in range(MESSAGES_TO_PUBLISH):
        status = publisher.publish(
            Message(body=Converter.string_to_bytes(f"message {i + 1}"))
        )
        if status.remote_state != OutcomeState.ACCEPTED:
            print(f"  unexpected outcome: {status.remote_state}")
    publisher.close()
    print("Done publishing.\n")


def run_consumer(consumer_name: str, addr: str) -> None:
    """
    Open a dedicated connection and run the consumer indefinitely.

    This function is intended to run in a **daemon thread**.  The thread
    owns its connection exclusively – no other thread touches it – so
    there are no cross-thread Proton reactor races.  When the main thread
    finishes, Python kills all daemon threads automatically.
    """
    conn = Connection(uri=BROKER_URI)
    conn.dial()

    consumer = conn.consumer(
        addr,
        message_handler=SACMessageHandler(consumer_name),
        consumer_options=QuorumConsumerOptions(
            sac_state_handler=lambda active: sac_state_callback(consumer_name, active)
        ),
    )

    print(f"[{consumer_name}] consumer attached, waiting for SAC notifications …")

    try:
        consumer.run()
        print("Done consuming.\n")
    except Exception:
        pass


def main() -> None:
    environment = Environment(uri=BROKER_URI)

    setup_conn = environment.connection()
    setup_conn.dial()
    # Keep management open for the lifetime of setup_conn so we can reuse it.
    management = setup_conn.management()

    print(f"Declaring Quorum Queue '{QUEUE_NAME}' with single_active_consumer=True …")
    management.declare_queue(
        QuorumQueueSpecification(name=QUEUE_NAME, single_active_consumer=True)
    )

    addr = AddressHelper.queue_address(QUEUE_NAME)

    # Start consumers as daemon threads.  Each thread owns its connection
    # exclusively; no connection is shared across threads.
    t1 = threading.Thread(target=run_consumer, args=("consumer-1", addr), daemon=True)
    t2 = threading.Thread(target=run_consumer, args=("consumer-2", addr), daemon=True)
    t1.start()
    t2.start()

    # Give consumers time to attach and receive their initial SAC notifications.
    time.sleep(10)

    # Publish messages — only the active consumer will receive them.
    publish_messages(setup_conn)

    # Let consumers drain the queue.
    time.sleep(3)

    # Clean up the queue while consumers are still attached (safe with quorum queues).
    try:
        management.delete_queue(QUEUE_NAME)
        print(f"Queue '{QUEUE_NAME}' deleted.")
    except Exception as e:
        print(f"Could not delete queue: {e}")

    management.close()
    setup_conn.close()

    print("Done.  (Daemon consumer threads will be stopped automatically.)")


if __name__ == "__main__":
    main()
