# type: ignore


import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    BindingSpecification,
    Connection,
    ConnectionClosed,
    Event,
    ExchangeSpecification,
    Message,
    QuorumQueueSpecification,
)

connection = None
management = None
publisher = None
consumer = None


def on_disconnected():

    print("disconnected")
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"

    global connection
    global management
    global publisher
    global consumer

    addr = AddressHelper.exchange_address(exchange_name, routing_key)
    addr_queue = AddressHelper.queue_address(queue_name)

    connection = create_connection()
    if management is not None:
        management = connection.management()
    if publisher is not None:
        publisher = connection.publisher(addr)
    if consumer is not None:
        consumer = connection.consumer(addr_queue, handler=MyMessageHandler())


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_message(self, event: Event):
        print("received message: " + str(event.message.annotations))

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

        print("count " + str(self._count))

        self._count = self._count + 1

        if self._count == 100:
            print("closing receiver")
            # if you want you can add cleanup operations here
            # event.receiver.close()
            # event.connection.close()

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


def create_connection() -> Connection:
    connection = Connection(
        "amqp://guest:guest@localhost:5672/", on_disconnection_handler=on_disconnected
    )
    connection.dial()

    return connection


def main() -> None:

    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"
    messages_to_publish = 50000

    global connection
    global management
    global publisher
    global consumer

    print("connection to amqp server")
    if connection is None:
        connection = create_connection()

    if management is None:
        management = connection.management()

    print("declaring exchange and queue")
    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(
        QuorumQueueSpecification(name=queue_name)
        # QuorumQueueSpecification(name=queue_name, dead_letter_exchange="dead-letter")
    )

    print("binding queue to exchange")
    bind_name = management.bind(
        BindingSpecification(
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
    # management.close()

    # publish 10 messages
    while True:
        for i in range(messages_to_publish):

            if i % 1000 == 0:
                print("publishing")
            try:
                publisher.publish(Message(body="test"))
            except ConnectionClosed:
                print("publisher closing exception, resubmitting")
                continue

        print("closing")
        try:
            publisher.close()
        except ConnectionClosed:
            print("publisher closing exception, resubmitting")
            continue
        break

    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )
    if consumer is None:
        consumer = connection.consumer(addr_queue, handler=MyMessageHandler())

    while True:
        try:
            consumer.run()
        except KeyboardInterrupt:
            pass
        except ConnectionClosed:
            time.sleep(1)
            continue
        except Exception as e:
            print("consumer exited for exception " + str(e))

        break

    print("cleanup")
    consumer.close()
    # once we finish consuming if we close the connection we need to create a new one
    # connection = create_connection()
    # management = connection.management()

    print("unbind")
    management.unbind(bind_name)

    print("delete queue")
    management.delete_queue(queue_name)

    print("delete exchange")
    management.delete_exchange(exchange_name)

    print("closing connections")
    management.close()
    print("after management closing")
    connection.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
