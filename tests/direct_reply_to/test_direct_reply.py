from rabbitmq_amqp_python_client import (
    Connection,
    DirectReplyToConsumerOptions,
)


def test_consumer_create_reply_name(connection: Connection) -> None:
    consumer = connection.consumer(consumer_options=DirectReplyToConsumerOptions())
    assert "/queues/amq.rabbitmq.reply-to." in consumer.address
