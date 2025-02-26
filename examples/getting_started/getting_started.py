# type: ignore


from rabbitmq_amqp_python_client import (  # PosixSSlConfigurationContext,; PosixClientCert,
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    Environment,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
)

MESSAGES_TO_PUBLISH = 100


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event):
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
    #        client_cert=ClientCert(client_cert=client_cert, client_key=client_key),
    #    ),
    # )
    connection.dial()

    return connection


def main() -> None:

    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"

    print("connection to amqp server")
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = create_connection(environment)

    management = connection.management()

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
    publisher = connection.publisher(addr)

    print("purging the queue")
    messages_purged = management.purge_queue(queue_name)

    print("messages purged: " + str(messages_purged))
    # management.close()

    # publish 10 messages
    for i in range(MESSAGES_TO_PUBLISH):
        print("publishing")
        status = publisher.publish(Message(body="test"))
        if status.remote_state == OutcomeState.ACCEPTED:
            print("message accepted")
        elif status.remote_state == OutcomeState.RELEASED:
            print("message not routed")
        elif status.remote_state == OutcomeState.REJECTED:
            print("message not rejected")

    publisher.close()

    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )
    consumer = connection.consumer(addr_queue, message_handler=MyMessageHandler())

    try:
        consumer.run()
    except KeyboardInterrupt:
        pass

    print("cleanup")
    consumer.close()
    # once we finish consuming if we close the connection we need to create a new one
    # connection = create_connection()
    management = connection.management()

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
