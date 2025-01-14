import time

from rabbitmq_amqp_python_client import (
    Connection,
    Event,
    Message,
    MessagingHandler,
    QuorumQueueSpecification,
    queue_address,
)


class MyMessageHandler(MessagingHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        self.accept(event.delivery)
        self._received = self._received + 1

    def on_link_closed(self, event: Event) -> None:
        assert self._received > 10


def test_consumer_sync_queue(connection: Connection) -> None:

    queue_name = "test-queue"
    messages_to_send = 100
    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)
    consumer = connection.consumer(addr_queue)

    consumed = 0

    # publish 10 messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))

    time.sleep(5)

    # consumer synchronously without handler
    for i in range(messages_to_send):
        message = consumer.consume()
        if message.body == "test" + str(i):
            consumed = consumed + 1

    assert consumed >= 10

    publisher.close()
    consumer.close()

    management.delete_queue(queue_name)
    management.close()


def test_consumer_async_queue(connection: Connection) -> None:

    messages_to_send = 1000

    queue_name = "test-queue"

    management = connection.management()

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = queue_address(queue_name)

    publisher = connection.publisher("/queues/" + queue_name)

    consumer = connection.consumer(addr_queue, handler=MyMessageHandler())

    # publish 10 messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i)))

    time.sleep(5)
    publisher.close()
    consumer.close()

    management.delete_queue(queue_name)
    management.close()
