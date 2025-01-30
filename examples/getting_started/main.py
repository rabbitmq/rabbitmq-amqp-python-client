# type: ignore


from rabbitmq_amqp_python_client import (  # SSlConfigurationContext,; SslConfigurationContext,; ClientCert,
    AddressHelper,
    AMQPMessagingHandler,
    BindingSpecification,
    Connection,
    Event,
    ExchangeSpecification,
    Message,
    QuorumQueueSpecification,
)


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_message(self, event: Event):
        print("received message: " + str(event.message.body))

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
    connection = Connection("amqp://guest:guest@localhost:5672/")
    # in case of SSL enablement
    # ca_cert_file = ".ci/certs/ca_certificate.pem"
    # client_cert = ".ci/certs/client_certificate.pem"
    # client_key = ".ci/certs/client_key.pem"
    # connection = Connection(
    #    "amqps://guest:guest@localhost:5671/",
    #    ssl_context=SslConfigurationContext(
    #        ca_cert=ca_cert_file,
    #        client_cert=ClientCert(client_cert=client_cert, client_key=client_key),
    #    ),
    # )
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
    publisher = connection.publisher(addr)

    print("purging the queue")
    messages_purged = management.purge_queue(queue_name)

    print("messages purged: " + str(messages_purged))
    # management.close()

    # publish 10 messages
    for i in range(messages_to_publish):
        status = publisher.publish(Message(body="test"))
        if status.ACCEPTED:
            print("message accepted")
        elif status.RELEASED:
            print("message not routed")
        elif status.REJECTED:
            print("message not rejected")

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
