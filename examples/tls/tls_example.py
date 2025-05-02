# type: ignore
import os
import sys
from traceback import print_exception

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    CurrentUserStore,
    Environment,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    LocalMachineStore,
    Message,
    PKCS12Store,
    PosixClientCert,
    PosixSslConfigurationContext,
    QuorumQueueSpecification,
    WinClientCert,
    WinSslConfigurationContext,
)
from rabbitmq_amqp_python_client.ssl_configuration import (
    FriendlyName,
)

messages_to_publish = 100


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

        if self._count == messages_to_publish:
            print("closing receiver")
            # if you want you can add cleanup operations here

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


def create_connection(environment: Environment) -> Connection:
    # in case of SSL enablement
    connection = environment.connection()
    connection.dial()

    return connection


def main() -> None:
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"
    ca_p12_store = ".ci/certs/ca.p12"
    ca_cert_file = ".ci/certs/ca_certificate.pem"
    client_cert = ".ci/certs/client_localhost_certificate.pem"
    client_key = ".ci/certs/client_localhost_key.pem"
    client_p12_store = ".ci/certs/client_localhost.p12"
    uri = "amqps://guest:guest@localhost:5671/"

    if sys.platform == "win32":
        ca_stores = [
            # names for the current user and local machine are not
            # case-sensitive
            CurrentUserStore(name="Root"),
            LocalMachineStore(name="Root"),
            PKCS12Store(path=ca_p12_store),
        ]
        client_stores = [
            # `personal` is treated as an alias for `my` by qpid proton
            # Recommended read:
            # https://github.com/apache/qpid-proton/blob/2847000fbb3732e80537e3c3ff5e097bb95bfae0/c/src/ssl/PLATFORM_NOTES.md
            CurrentUserStore(name="Personal"),
            LocalMachineStore(name="my"),
            PKCS12Store(path=client_p12_store),
        ]

        for ca_store, client_store in zip(ca_stores, client_stores):
            ssl_context = WinSslConfigurationContext(
                ca_store=ca_store,
                client_cert=WinClientCert(
                    store=client_store,
                    # qpid proton uses Windows constant CERT_NAME_FRIENDLY_DISPLAY_TYPE
                    # to retrieve the value which is compare to the one we provide
                    # If certificates have no friendly name Windows falls back to
                    # CERT_NAME_SIMPLE_DISPLAY_TYPE which has further fallbacks
                    # https://learn.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-certgetnamestringa
                    disambiguation_method=FriendlyName("1"),
                    password=None,
                ),
            )
            environment = Environment(
                uri,
                ssl_context=ssl_context,
            )

            try:
                print("connection to amqp server")
                connection = create_connection(environment)
                break
            except Exception as e:
                print_exception(e)
                continue
        else:
            raise RuntimeError(
                "connection failed. working directory should be project root"
            )
    else:
        print(" ca_cert_file exists: {}".format(os.path.isfile(ca_cert_file)))
        print(" client_cert exists: {}".format(os.path.isfile(client_cert)))
        print(" client_key exists: {}".format(os.path.isfile(client_key)))
        environment = Environment(
            uri,
            ssl_context=PosixSslConfigurationContext(
                ca_cert=ca_cert_file,
                client_cert=PosixClientCert(
                    client_cert=client_cert,
                    client_key=client_key,
                    password=None,
                ),
            ),
        )

        print("connection to amqp server")
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
    for i in range(messages_to_publish):
        status = publisher.publish(Message(body=Converter.string_to_bytes("test")))
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
    consumer = connection.consumer(addr_queue, message_handler=MyMessageHandler())

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
    environment.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
