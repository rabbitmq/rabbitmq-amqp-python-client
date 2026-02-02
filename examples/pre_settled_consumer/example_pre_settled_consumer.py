"""
Example demonstrating FIFO consumer with pre-settled deliveries.

This example shows how to use FIFOConsumerOptions with pre_settled=True
to enable at-most-once delivery semantics for FIFO (Classic and Quorum) queues.

When pre_settled=True:
- Messages are automatically settled when received
- Messages cannot be redelivered if processing fails
- Suitable for use cases where message loss is acceptable
  (e.g., metrics, logs, sensor data)

When pre_settled=False (default):
- Messages require explicit acknowledgment
- Messages can be redelivered if not acknowledged
- Provides at-least-once delivery semantics
"""

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    ConsumerOptions,
    Converter,
    Environment,
    Event,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.entities import (
    ConsumerFeature,
)

MESSAGES_TO_PUBLISH = 50


class MyMessageHandler(AMQPMessagingHandler):
    """Message handler that processes messages and accepts them."""

    def __init__(self) -> None:
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event) -> None:
        """Handle incoming AMQP messages."""
        message_body = Converter.bytes_to_string(event.message.body)
        print(f"Received message: {message_body}")

        # With pre_settled=True, messages are automatically settled
        # calling self.delivery_context.accept(event) will raise an error

        self._count = self._count + 1
        print(f"Processed {self._count} messages")

        if self._count == MESSAGES_TO_PUBLISH:
            print("Received all messages")


def create_connection(environment: Environment) -> Connection:
    """Create and dial a connection to RabbitMQ."""
    connection = environment.connection()
    connection.dial()
    return connection


def main() -> None:
    """Main function demonstrating FIFO consumer with pre-settled deliveries."""
    queue_name = "example-fifo-queue"

    print("Connecting to AMQP server...")
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = create_connection(environment)

    management = connection.management()

    print(f"Declaring queue: {queue_name}")
    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    print("Creating publisher and publishing messages...")
    publisher = connection.publisher(addr_queue)

    # Purge any existing messages
    messages_purged = management.purge_queue(queue_name)
    print(f"Messages purged: {messages_purged}")

    # Publish messages
    for i in range(MESSAGES_TO_PUBLISH):
        status = publisher.publish(
            Message(body=Converter.string_to_bytes(f"test message {i}"))
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            if i % 10 == 0:  # Print every 10th message
                print(f"Published message {i}")
        elif status.remote_state == OutcomeState.RELEASED:
            print(f"Message {i} not routed")
        elif status.remote_state == OutcomeState.REJECTED:
            print(f"Message {i} rejected")

    publisher.close()

    print("\n" + "=" * 60)
    print("Creating FIFO consumer with pre_settled=True")
    print("(at-most-once delivery semantics)")
    print("=" * 60)

    # Create consumer with pre_settled=True
    consumer = connection.consumer(
        destination=addr_queue,
        message_handler=MyMessageHandler(),
        consumer_options=ConsumerOptions(ConsumerFeature.Presettled),
    )

    print("\nStarting consumer - press Ctrl+C to stop...")
    try:
        consumer.run()
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")

    print("\nCleaning up...")
    consumer.close()

    # Cleanup
    management = connection.management()
    management.delete_queue(queue_name)
    management.close()
    environment.close()

    print("Example completed!")


if __name__ == "__main__":
    main()
