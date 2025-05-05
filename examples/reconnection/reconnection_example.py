# type: ignore
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
)

# here we keep track of the objects we need to reconnect
MESSAGES_TO_PUBLISH = 50000

environment = Environment(
    uri="amqp://guest:guest@localhost:5672/",
)


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_message(self, event: Event):
        if self._count % 1000 == 0:
            print("received 100 message: " + Converter.bytes_to_string(event.message.body))

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

        self._count = self._count + 1

        if self._count == MESSAGES_TO_PUBLISH:
            print("closing receiver")
            # if you want you can add cleanup operations here

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


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


def main() -> None:
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"

    print("connection to amqp server")
    connection = create_connection()
    management = connection.management()
    publisher = None
    consumer = None

    print("declaring exchange and queue")
    management.declare_exchange(ExchangeSpecification(name=exchange_name))

    management.declare_queue(
        QuorumQueueSpecification(name=queue_name)
        # QuorumQueueSpecification(name=queue_name, dead_letter_exchange="dead-letter")
    )

    print("binding queue to exchange")
    bind_name = management.bind(
        ExchangeToQueueBindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    addr = AddressHelper.exchange_address(exchange_name, routing_key)

    addr_queue = AddressHelper.queue_address(queue_name)

    print("create a publisher and publish a test message")
    if publisher is None:
        publisher = connection.publisher(addr)

    print("purging the queue")
    messages_purged = management.purge_queue(queue_name)

    print("messages purged: " + str(messages_purged))

    # publishing messages
    while True:
        for i in range(MESSAGES_TO_PUBLISH):

            if i % 1000 == 0:
                print("published 1000 messages...")
            try:
                if publisher is not None:
                    publisher.publish(Message(body=Converter.string_to_bytes("test")))
            except ConnectionClosed:
                print("publisher closing exception, resubmitting")
                # publisher = connection.publisher(addr)
                continue

        print("closing publisher")
        try:
            if publisher is not None:
                publisher.close()
        except ConnectionClosed:
            print("publisher closing exception, resubmitting")
            continue
        break

    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )
    if consumer is None:
        consumer = connection.consumer(addr_queue, message_handler=MyMessageHandler())

    while True:
        try:
            consumer.run()
        except KeyboardInterrupt:
            pass
        except ConnectionClosed:
            continue
        except Exception as e:
            print("consumer exited for exception " + str(e))

        break

    print("cleanup")
    consumer.close()

    print("unbind")
    management.unbind(bind_name)

    print("delete queue")
    management.delete_queue(queue_name)

    print("delete exchange")
    management.delete_exchange(exchange_name)

    print("closing connections")
    management.close()
    print("after management closing")
    environment.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
