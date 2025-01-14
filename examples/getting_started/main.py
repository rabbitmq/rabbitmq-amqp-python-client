from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
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
        super().__init__()

    def on_message(self, event: Event):
        print("received message: " + event.message.body)
        self.accept(event.delivery)

    def on_connection_closed(self, event: Event):
        print("connection closed")

    def on_connection_cloing(self, event: Event):
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        print("link closed")

    def on_rejected(self, event: Event) -> None:
        print("rejected")


def main() -> None:
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"
    connection = Connection("amqp://guest:guest@localhost:5672/")

    print("connection to amqp server")
    connection.dial()

    management = connection.management()

    print("declaring exchange and queue")
    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(QuorumQueueSpecification(name=queue_name))

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

    publisher.publish(Message(body="test"))

    print("purging the queue")
    messages_purged = management.purge_queue(queue_name)

    print("messages purged: " + str(messages_purged))

    for i in range(10):
        publisher.publish(Message(body="test"))

    publisher.close()

    print("create a consumer and consume the test message")

    consumer = connection.consumer(addr_queue, handler=MyMessageHandler())

    print("unbind")
    management.unbind(bind_name)



    consumer.close()
    print("delete queue")
    #management.delete_queue(queue_name)

    print("delete exchange")
    management.delete_exchange(exchange_name)

    print("closing connections")
    management.close()
    connection.close()


if __name__ == "__main__":
    main()
