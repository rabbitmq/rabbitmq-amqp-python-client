# type: ignore
"""
Reconnection example.

A publisher and a consumer run independently, each one in its own thread and
with its own connection (the TCP connection is not thread safe).
Every few seconds the main thread prints a status line.

Try to close the connections from the RabbitMQ management UI (or restart the
broker) while the example is running and watch the clients recover.
Press Ctrl+C to stop.
"""
import threading
import time
from datetime import datetime, timedelta

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    ConnectionClosed,
    Converter,
    Environment,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    Message,
    QuorumQueueSpecification,
    RecoveryConfiguration,
)

EXCHANGE_NAME = "test-exchange"
QUEUE_NAME = "example-queue"
ROUTING_KEY = "routing-key"

# pause between two published messages: 0.01 means ~100 msg/s
PUBLISH_INTERVAL_SECONDS = 0.01
# how often the status line is printed
STATUS_INTERVAL_SECONDS = 5

environment = Environment(
    uri="amqp://guest:guest@localhost:5672/",
    recovery_configuration=RecoveryConfiguration(
        back_off_reconnect_interval=timedelta(seconds=10)
    ),
)

stop_event = threading.Event()


def log(who: str, text: str) -> None:
    print(f"{datetime.now():%H:%M:%S.%f}"[:-3] + f" [{who:<9}] {text}", flush=True)


class Stats:
    """Counters shared between the publisher and the consumer threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.published = 0
        self.consumed = 0
        self.publish_errors = 0

    def inc(self, name: str) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + 1)

    def snapshot(self) -> tuple:
        with self._lock:
            return self.published, self.consumed, self.publish_errors


stats = Stats()


class MyMessageHandler(AMQPMessagingHandler):

    def on_message(self, event: Event):
        # accepting
        self.delivery_context.accept(event)

        # in case of rejection (+eventually deadlettering)
        # self.delivery_context.discard(event)

        # in case of requeuing
        # self.delivery_context.requeue(event)

        # annotations = {}
        # annotations[symbol('x-opt-string')] = 'x-test1'
        # in case of requeuing with annotations added
        # self.delivery_context.requeue_with_annotations(event, annotations)

        # in case of rejection with annotations added
        # self.delivery_context.discard_with_annotations(event)

        stats.inc("consumed")

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        log("consumer", "connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        log("consumer", "link closed")


def create_connection() -> Connection:
    # for multinode specify a list of urls and fill the field uris of Connection instead of url
    # uris = [
    #    "amqp://ha_tls-rabbit_node0-1:5682/",
    #    "amqp://ha_tls-rabbit_node1-1:5692/",
    #    "amqp://ha_tls-rabbit_node2-1:5602/",
    # ]
    # connection = Connection(uris=uris, on_disconnection_handler=on_disconnected)

    connection = environment.connection()
    connection.dial()

    return connection


def publisher_loop(addr: str) -> None:
    """Publish until stop_event is set. Uses its own connection."""
    connection = create_connection()
    publisher = connection.publisher(addr)
    log("publisher", "started")

    reconnecting = False
    while not stop_event.is_set():
        try:
            publisher.publish(Message(body=Converter.string_to_bytes("test")))
        except ConnectionClosed:
            # the client is recovering the connection: wait and retry
            stats.inc("publish_errors")
            if not reconnecting:
                log("publisher", "connection lost, waiting for recovery...")
                reconnecting = True
            time.sleep(1)
            continue

        if reconnecting:
            log("publisher", "connection recovered, publishing again")
            reconnecting = False
        stats.inc("published")
        time.sleep(PUBLISH_INTERVAL_SECONDS)

    log("publisher", "stopping")
    try:
        publisher.close()
    except ConnectionClosed:
        pass


def consumer_loop(addr_queue: str, consumer_ready: threading.Event) -> None:
    """Consume until stop_event is set. Uses its own connection."""
    connection = create_connection()
    consumer = connection.consumer(addr_queue, message_handler=MyMessageHandler())
    log("consumer", "started")

    # keep a reference so the main thread can stop it
    consumer_loop.consumer = consumer
    consumer_ready.set()

    while not stop_event.is_set():
        try:
            consumer.run()
        except KeyboardInterrupt:
            break
        except ConnectionClosed:
            log("consumer", "connection lost, waiting for recovery...")
            time.sleep(1)
            continue
        except Exception as e:
            log("consumer", f"exited for exception {e!r}")
            break

        if not stop_event.is_set():
            time.sleep(1)

    # the consumer container has been stopped: the connection is released
    # by environment.close() in main
    log("consumer", "stopped")


def main() -> None:
    log("main", "connecting to amqp server")
    connection = create_connection()
    management = connection.management()

    log("main", "declaring exchange and queue")
    management.declare_exchange(ExchangeSpecification(name=EXCHANGE_NAME))
    management.declare_queue(
        QuorumQueueSpecification(name=QUEUE_NAME)
        # QuorumQueueSpecification(name=queue_name, dead_letter_exchange="dead-letter")
    )

    log("main", "binding queue to exchange")
    bind_name = management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=EXCHANGE_NAME,
            destination_queue=QUEUE_NAME,
            binding_key=ROUTING_KEY,
        )
    )

    log("main", "purging the queue")
    log("main", f"messages purged: {management.purge_queue(QUEUE_NAME)}")

    addr = AddressHelper.exchange_address(EXCHANGE_NAME, ROUTING_KEY)
    addr_queue = AddressHelper.queue_address(QUEUE_NAME)

    consumer_ready = threading.Event()
    consumer_thread = threading.Thread(
        target=consumer_loop, args=(addr_queue, consumer_ready), name="consumer"
    )
    publisher_thread = threading.Thread(
        target=publisher_loop, args=(addr,), name="publisher"
    )

    # the consumer is started first, then the publisher: they are fully independent
    consumer_thread.start()
    consumer_ready.wait(timeout=30)
    publisher_thread.start()

    log(
        "main",
        "running - close the connections from the RabbitMQ UI to test the "
        "recovery, press Ctrl+C to stop",
    )

    last_published = last_consumed = 0
    try:
        while True:
            time.sleep(STATUS_INTERVAL_SECONDS)
            published, consumed, errors = stats.snapshot()
            log(
                "status",
                f"published={published} (+{published - last_published}) "
                f"consumed={consumed} (+{consumed - last_consumed}) "
                f"in-flight={published - consumed} publish-errors={errors}",
            )
            last_published, last_consumed = published, consumed
    except KeyboardInterrupt:
        log("main", "Ctrl+C received, shutting down")

    stop_event.set()
    publisher_thread.join(timeout=30)

    # give the consumer a moment to drain the remaining messages
    time.sleep(1)
    consumer = getattr(consumer_loop, "consumer", None)
    if consumer is not None:
        try:
            consumer.stop()
        except Exception as e:
            log("main", f"error while stopping the consumer: {e!r}")
    consumer_thread.join(timeout=30)

    published, consumed, errors = stats.snapshot()
    log(
        "main",
        f"final: published={published} consumed={consumed} publish-errors={errors}",
    )

    log("main", "unbind")
    management.unbind(bind_name)

    log("main", "delete queue")
    management.delete_queue(QUEUE_NAME)

    log("main", "delete exchange")
    management.delete_exchange(EXCHANGE_NAME)

    log("main", "closing connections")
    management.close()
    environment.close()
    log("main", "done")


if __name__ == "__main__":
    main()
