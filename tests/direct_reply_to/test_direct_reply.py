from rabbitmq_amqp_python_client import (
    Connection,
    ConsumerOptions,
    ConsumerSettleStrategy,
)


def test_consumer_create_reply_name(connection: Connection) -> None:
    consumer = connection.consumer(
        consumer_options=ConsumerOptions(
            settle_strategy=ConsumerSettleStrategy.DirectReplyTo
        )
    )
    assert "/queues/amq.rabbitmq.reply-to." in consumer.address
