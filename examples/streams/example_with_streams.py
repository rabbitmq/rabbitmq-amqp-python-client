# type: ignore

from rabbitmq_amqp_python_client import (  # PosixSSlConfigurationContext,; PosixClientCert,
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    ConnectionClosed,
    Environment,
    Event,
    Message,
    OffsetSpecification,
    StreamOptions,
    StreamSpecification,
)

MESSAGES_TO_PUBLISH = 100


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event):
        # just messages with banana filters get received
        print(
            "received message from stream: "
            + str(event.message.body)
            + " with offset: "
            + str(event.message.annotations["x-stream-offset"])
        )

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

        if self._count == MESSAGES_TO_PUBLISH:
            print("closing receiver")
            # if you want you can add cleanup operations here

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


def create_connection(environment: Environment) -> Connection:
    connection = environment.connection()
    # in case of SSL enablement
    # ca_cert_file = ".ci/certs/ca_certificate.pem"
    # client_cert = ".ci/certs/client_certificate.pem"
    # client_key = ".ci/certs/client_key.pem"
    # connection = Connection(
    #    "amqps://guest:guest@localhost:5671/",
    #    ssl_context=PosixSslConfigurationContext(
    #        ca_cert=ca_cert_file,
    #        client_cert=PosixClientCert(client_cert=client_cert, client_key=client_key),
    #    ),
    # )
    connection.dial()

    return connection


def main() -> None:
    queue_name = "example-queue"

    print("connection to amqp server")
    environment = Environment("amqp://guest:guest@localhost:5672/")
    connection = create_connection(environment)

    management = connection.management()

    management.declare_queue(StreamSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    consumer_connection = create_connection(environment)

    consumer = consumer_connection.consumer(
        addr_queue,
        message_handler=MyMessageHandler(),
        # can be first, last, next or an offset long
        # you can also specify stream filters with methods: apply_filters and filter_match_unfiltered
        stream_filter_options=StreamOptions(
            offset_specification=OffsetSpecification.first, filters=["banana"]
        ),
    )
    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )

    # print("create a publisher and publish a test message")
    publisher = connection.publisher(addr_queue)

    # publish with a filter of apple
    for i in range(MESSAGES_TO_PUBLISH):
        publisher.publish(
            Message(
                body="apple: " + str(i), annotations={"x-stream-filter-value": "apple"}
            )
        )

    # publish with a filter of banana
    for i in range(MESSAGES_TO_PUBLISH):
        publisher.publish(
            Message(
                body="banana: " + str(i),
                annotations={"x-stream-filter-value": "banana"},
            )
        )

    publisher.close()

    while True:
        try:
            consumer.run()
        except KeyboardInterrupt:
            pass
        except ConnectionClosed:
            print("connection closed")
            continue
        except Exception as e:
            print("consumer exited for exception " + str(e))

        break

    #
    print("delete queue")
    # management.delete_queue(queue_name)

    print("closing connections")
    management.close()
    print("after management closing")
    environment.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
