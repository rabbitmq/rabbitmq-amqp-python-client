# type: ignore


import time
from dataclasses import dataclass
from typing import Optional

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    BindingSpecification,
    Connection,
    ConnectionClosed,
    Consumer,
    Environment,
    Event,
    ExchangeSpecification,
    Management,
    Message,
    Publisher,
    QuorumQueueSpecification,
)

environment = Environment()


# here we keep track of the objects we need to reconnect
@dataclass
class ConnectionConfiguration:
    connection: Optional[Connection] = None
    management: Optional[Management] = None
    publisher: Optional[Publisher] = None
    consumer: Optional[Consumer] = None


connection_configuration = ConnectionConfiguration()
MESSAGES_TO_PUBLSH = 50000


# disconnection callback
# here you can cleanup or reconnect
def on_disconnection():

    print("disconnected")
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"

    global connection_configuration

    addr = AddressHelper.exchange_address(exchange_name, routing_key)
    addr_queue = AddressHelper.queue_address(queue_name)

    if connection_configuration.connection is not None:
        connection_configuration.connection = create_connection()
    if connection_configuration.management is not None:
        connection_configuration.management = (
            connection_configuration.connection.management()
        )
    if connection_configuration.publisher is not None:
        connection_configuration.publisher = (
            connection_configuration.connection.publisher(addr)
        )
    if connection_configuration.consumer is not None:
        connection_configuration.consumer = (
            connection_configuration.connection.consumer(
                addr_queue, message_handler=MyMessageHandler()
            )
        )


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_message(self, event: Event):
        if self._count % 1000 == 0:
            print("received 100 message: " + str(event.message.body))

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

        if self._count == MESSAGES_TO_PUBLSH:
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
    # for multinode specify a list of urls and fill the field uris of Connection instead of url
    # uris = [
    #    "amqp://ha_tls-rabbit_node0-1:5682/",
    #    "amqp://ha_tls-rabbit_node1-1:5692/",
    #    "amqp://ha_tls-rabbit_node2-1:5602/",
    # ]
    # connection = Connection(uris=uris, on_disconnection_handler=on_disconnected)

    connection = environment.connection(
        url="amqp://guest:guest@localhost:5672/",
        on_disconnection_handler=on_disconnection,
    )
    connection.dial()

    return connection


def main() -> None:

    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"

    global connection_configuration

    print("connection to amqp server")
    if connection_configuration.connection is None:
        connection_configuration.connection = create_connection()

    if connection_configuration.management is None:
        connection_configuration.management = (
            connection_configuration.connection.management()
        )

    print("declaring exchange and queue")
    connection_configuration.management.declare_exchange(
        ExchangeSpecification(name=exchange_name)
    )

    connection_configuration.management.declare_queue(
        QuorumQueueSpecification(name=queue_name)
        # QuorumQueueSpecification(name=queue_name, dead_letter_exchange="dead-letter")
    )

    print("binding queue to exchange")
    bind_name = connection_configuration.management.bind(
        BindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    addr = AddressHelper.exchange_address(exchange_name, routing_key)

    addr_queue = AddressHelper.queue_address(queue_name)

    print("create a publisher and publish a test message")
    if connection_configuration.publisher is None:
        connection_configuration.publisher = (
            connection_configuration.connection.publisher(addr)
        )

    print("purging the queue")
    messages_purged = connection_configuration.management.purge_queue(queue_name)

    print("messages purged: " + str(messages_purged))
    # management.close()

    # publishing messages
    while True:
        for i in range(MESSAGES_TO_PUBLSH):

            if i % 1000 == 0:
                print("published 1000 messages...")
            try:
                if connection_configuration.publisher is not None:
                    connection_configuration.publisher.publish(Message(body="test"))
            except ConnectionClosed:
                print("publisher closing exception, resubmitting")
                continue

        print("closing publisher")
        try:
            if connection_configuration.publisher is not None:
                connection_configuration.publisher.close()
        except ConnectionClosed:
            print("publisher closing exception, resubmitting")
            continue
        break

    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )
    if connection_configuration.consumer is None:
        connection_configuration.consumer = (
            connection_configuration.connection.consumer(
                addr_queue, message_handler=MyMessageHandler()
            )
        )

    while True:
        try:
            connection_configuration.consumer.run()
        except KeyboardInterrupt:
            pass
        except ConnectionClosed:
            time.sleep(1)
            continue
        except Exception as e:
            print("consumer exited for exception " + str(e))

        break

    print("cleanup")
    connection_configuration.consumer.close()
    # once we finish consuming if we close the connection we need to create a new one
    # connection = create_connection()
    # management = connection.management()

    print("unbind")
    connection_configuration.management.unbind(bind_name)

    print("delete queue")
    connection_configuration.management.delete_queue(queue_name)

    print("delete exchange")
    connection_configuration.management.delete_exchange(exchange_name)

    print("closing connections")
    connection_configuration.management.close()
    print("after management closing")
    environment.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
