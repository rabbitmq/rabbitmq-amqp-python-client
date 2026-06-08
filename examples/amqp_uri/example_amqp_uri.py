# type: ignore
"""
AmqpUri example
===============

Demonstrates how to configure an AMQP connection using the ``AmqpUri``
dataclass instead of a raw URI string.

``AmqpUri`` lets you specify each part of the connection (schema, host, port,
user, password, vhost) as a separate field with sensible defaults, so you
never have to manually compose or parse a URI string.

Exactly one of ``uri``, ``uris``, or ``amqp_uri`` must be passed to
``Environment``.  Mixing them raises a ``ValueError``.

Usage
-----
Start RabbitMQ first, then run:

    python examples/amqp_uri/example_amqp_uri.py
"""

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    AmqpUri,
    Converter,
    Environment,
    Event,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
)

MESSAGES_TO_PUBLISH = 10
QUEUE_NAME = "amqp-uri-example-queue"


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self) -> None:
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event) -> None:
        print(
            "received message: {}".format(
                Converter.bytes_to_string(event.message.body)
            )
        )
        self.delivery_context.accept(event)
        self._count += 1
        if self._count == MESSAGES_TO_PUBLISH:
            print("all messages received – stopping consumer")
            raise SystemExit(0)

    def on_connection_closed(self, event: Event) -> None:
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        print("link closed")


def main() -> None:
    # ------------------------------------------------------------------
    # 1. All defaults – equivalent to "amqp://guest:guest@localhost:5672/"
    # ------------------------------------------------------------------
    default_uri = AmqpUri()
    print(f"default AmqpUri  → {default_uri.to_uri()}")

    # ------------------------------------------------------------------
    # 2. Custom fields – only override what differs from the defaults
    # ------------------------------------------------------------------
    custom_uri = AmqpUri(
        schema="amqp",
        host="localhost",
        port=5672,
        user="guest",
        password="guest",
        vhost="/",
    )
    print(f"custom AmqpUri   → {custom_uri.to_uri()}")

    # ------------------------------------------------------------------
    # 3. Connect using amqp_uri=  (mutually exclusive with uri= / uris=)
    # ------------------------------------------------------------------
    print("\nconnecting to RabbitMQ via AmqpUri …")
    environment = Environment(amqp_uri=AmqpUri())
    connection = environment.connection()
    connection.dial()
    print("connected")

    management = connection.management()

    print(f"declaring queue '{QUEUE_NAME}'")
    management.declare_queue(QuorumQueueSpecification(name=QUEUE_NAME))

    addr_queue = AddressHelper.queue_address(QUEUE_NAME)

    # ------------------------------------------------------------------
    # 4. Publish messages
    # ------------------------------------------------------------------
    print(f"publishing {MESSAGES_TO_PUBLISH} messages")
    publisher = connection.publisher(addr_queue)
    for i in range(MESSAGES_TO_PUBLISH):
        status = publisher.publish(
            Message(body=Converter.string_to_bytes(f"message {i}"))
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            print(f"  message {i} accepted")
        elif status.remote_state == OutcomeState.RELEASED:
            print(f"  message {i} not routed")
        elif status.remote_state == OutcomeState.REJECTED:
            print(f"  message {i} rejected")
    publisher.close()

    # ------------------------------------------------------------------
    # 5. Consume messages
    # ------------------------------------------------------------------
    print("consuming messages (press Ctrl+C to stop early) …")
    consumer = connection.consumer(addr_queue, message_handler=MyMessageHandler())
    try:
        consumer.run()
    except (KeyboardInterrupt, SystemExit):
        pass

    # ------------------------------------------------------------------
    # 6. Clean up
    # ------------------------------------------------------------------
    print("\ncleaning up")
    consumer.close()

    management = connection.management()
    management.delete_queue(QUEUE_NAME)
    management.close()

    environment.close()
    print("done")


if __name__ == "__main__":
    main()
