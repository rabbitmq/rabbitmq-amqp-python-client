from rabbitmq_amqp_python_client import (
    Connection,
    Converter,
    DirectReplyToConsumerOptions,
    Environment,
    Message,
    OutcomeState,
)


def test_consumer_create_reply_name(connection: Connection) -> None:
    consumer = connection.consumer("", consumer_options=DirectReplyToConsumerOptions())
    assert "/queues/amq.rabbitmq.reply-to." in consumer.get_queue_address()
    consumer.close()


def test_direct_reply_to_send_and_receive(
    environment: Environment, connection: Connection
) -> None:
    """Test that messages can be published to and consumed from a direct reply-to queue."""
    messages_to_send = 10

    # Create a consumer using DirectReplyToConsumerOptions
    consumer = connection.consumer("", consumer_options=DirectReplyToConsumerOptions())

    # Get the queue address from the consumer
    addr = consumer.get_queue_address()
    assert addr is not None
    assert "/queues/amq.rabbitmq.reply-to." in addr

    # Create a new connection and publisher to publish to the reply-to address
    publisher_connection = environment.connection()
    publisher_connection.dial()
    publisher = publisher_connection.publisher(addr)

    # Publish messages to the direct reply-to queue
    for i in range(messages_to_send):
        msg = Message(body=Converter.string_to_bytes("test message {}".format(i)))
        status = publisher.publish(msg)
        assert status.remote_state == OutcomeState.ACCEPTED

    # Consume messages synchronously
    consumed = 0
    for i in range(messages_to_send):
        message = consumer.consume()
        if Converter.bytes_to_string(message.body) == "test message {}".format(i):
            consumed = consumed + 1

    # Clean up
    publisher.close()
    publisher_connection.close()
    consumer.close()

    # Verify all messages were received
    assert consumed == messages_to_send
