# type: ignore

# This example demonstrates the RabbitMQ 4.3+ "rejected-by and rejection reason" feature.
#
# When a queue rejects a published message (e.g. because the queue is full and the
# overflow strategy is set to "reject-publish"), RabbitMQ 4.3+ includes the queue
# name and the specific rejection reason in the AMQP Rejected outcome.
#
# The client surfaces this as an AmqpMessageRejectedException whose message
# contains the broker-provided reason string.
#
# See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#amqp-rejection-reason

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AmqpMessageRejectedException,
    Converter,
    Environment,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
)

QUEUE_NAME = "example-rejected-messages"
MAX_LENGTH = 5


def main() -> None:
    print("Connecting to RabbitMQ...")
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = environment.connection()
    connection.dial()

    management = connection.management()

    print(
        f"Declaring queue '{QUEUE_NAME}' with max-length={MAX_LENGTH} "
        "and overflow strategy 'reject-publish'..."
    )
    management.declare_queue(
        QuorumQueueSpecification(
            name=QUEUE_NAME,
            max_len=MAX_LENGTH,
            overflow_behaviour="reject-publish",
        )
    )

    addr = AddressHelper.queue_address(QUEUE_NAME)
    publisher = connection.publisher(addr)

    print(f"\nPublishing {MAX_LENGTH} messages to fill the queue...")
    for i in range(MAX_LENGTH):
        status = publisher.publish(
            Message(body=Converter.string_to_bytes(f"message-{i}"))
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            print(f"  message-{i}: accepted")

    print("Publishing one more message (queue is full - expecting rejection)...")

    for i in range(MAX_LENGTH):
        try:
            publisher.publish(
                Message(body=Converter.string_to_bytes("overflow-message"))
            )
            print("  Message was accepted (broker version < 4.3 or queue not full yet)")
        except AmqpMessageRejectedException as e:
            print("AmqpMessageRejectedException raised as expected!")
            print(f"  Rejection reason from broker: {e.msg}")

    publisher.close()

    print("\nCleaning up...")
    management.purge_queue(QUEUE_NAME)
    management.delete_queue(QUEUE_NAME)
    management.close()
    environment.close()
    print("Done.")


if __name__ == "__main__":
    main()
