# type: ignore

from rabbitmq_amqp_python_client import (  # SSlConfigurationContext,; SslConfigurationContext,; ClientCert,
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    Event,
    Message,
    OffsetSpecification,
    StreamFilterOptions,
    StreamSpecification,
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
    queue_name = "example-queue"
    messages_to_publish = 100

    print("connection to amqp server")
    connection = create_connection()

    management = connection.management()

    management.declare_queue(StreamSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    consumer_connection = create_connection()

    stream_filter_options = StreamFilterOptions()
    # can be first, last, next or an offset long
    stream_filter_options.offset(OffsetSpecification.first)

    consumer = consumer_connection.consumer(
        addr_queue,
        handler=MyMessageHandler(),
        stream_filter_options=stream_filter_options,
    )
    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )

    # print("create a publisher and publish a test message")
    publisher = connection.publisher(addr_queue)

    for i in range(messages_to_publish):
        publisher.publish(Message(body="test: " + str(i)))

    publisher.close()

    try:
        consumer.run()
    except KeyboardInterrupt:
        pass

    #
    print("delete queue")
    # management.delete_queue(queue_name)

    print("closing connections")
    management.close()
    print("after management closing")
    connection.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
