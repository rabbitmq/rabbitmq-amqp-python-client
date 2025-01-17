# type: ignore
from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    Delivery,
    Event,
    ExchangeSpecification,
    Message,
    MessagingHandler,
    QuorumQueueSpecification,
    exchange_address,
    queue_address,
)


class MyMessageHandler(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=False)
        self._count = 0

    def on_message(self, event: Event):
        print("received message: " + event.message.body)

        dlv = event.delivery

        dlv.update(Delivery.ACCEPTED)
        dlv.settle()

        print("count " + str(self._count))

        self._count = self._count + 1

        if self._count == 100000:
            print("closing receiver")
            event.receiver.close()
            event.connection.close()

    def on_connection_closed(self, event: Event):
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        print("link closed")

    def on_rejected(self, event: Event) -> None:
        print("rejected")


def create_connection() -> Connection:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()

    return connection


def main() -> None:

    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"
    messages_to_publish = 100000

    print("connection to amqp server")
    connection = create_connection()

    management = connection.management()

    print("declaring exchange and queue")
    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(
        QuorumQueueSpecification(
            name=queue_name, dead_letter_exchange="dead-letter-test"
        )
    )

    print("binding queue to exchange")
    bind_name = management.bind(
        BindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    addr = exchange_address(exchange_name, routing_key)

    addr_queue = queue_address(queue_name)

    print("create a publisher and publish a test message")
    publisher = connection.publisher(addr)

    print("purging the queue")
    messages_purged = management.purge_queue(queue_name)

    print("messages purged: " + str(messages_purged))
    management.close()

    # publish 10 messages
    for i in range(messages_to_publish):
        publisher.publish(Message(body="test"))

    publisher.close()

    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )
    consumer = connection.consumer(addr_queue, handler=MyMessageHandler())

    try:
        consumer.run()
    except KeyboardInterrupt:
        pass

    print("cleanup")
    # once we finish consuming we close the connection so we need to create a new one
    connection = create_connection()

    management = connection.management()
    print("unbind")
    management.unbind(bind_name)

    print("delete queue")
    management.delete_queue(queue_name)

    print("delete exchange")
    management.delete_exchange(exchange_name)

    print("closing connections")
    management.close()
    consumer.close()
    print("after management closing")
    connection.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
