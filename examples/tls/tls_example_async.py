# type: ignore

import asyncio
import os
import signal
import sys
from traceback import print_exception

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    AsyncEnvironment,
    Converter,
    CurrentUserStore,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    LocalMachineStore,
    Message,
    OutcomeState,
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

    def on_amqp_message(self, event: Event):
        print("received message: " + Converter.bytes_to_string(event.message.body))

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
            print("received all messages")

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


async def main() -> None:
    exchange_name = "tls-test-exchange"
    queue_name = "tls-example-queue"
    routing_key = "tls-routing-key"
    ca_p12_store = ".ci/certs/ca.p12"
    ca_cert_file = ".ci/certs/ca_certificate.pem"
    client_cert = ".ci/certs/client_localhost_certificate.pem"
    client_key = ".ci/certs/client_localhost_key.pem"
    client_p12_store = ".ci/certs/client_localhost.p12"
    uri = "amqps://guest:guest@localhost:5671/"

    environment = None

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

            try:
                print("connection to amqp server")
                environment = AsyncEnvironment(
                    uri,
                    ssl_context=ssl_context,
                )
                # Test connection
                async with environment:
                    async with await environment.connection() as connection:
                        print("Connection successful")
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
        ssl_context = PosixSslConfigurationContext(
            ca_cert=ca_cert_file,
            client_cert=PosixClientCert(
                client_cert=client_cert,
                client_key=client_key,
                password=None,
            ),
        )

        print("connection to amqp server")
        environment = AsyncEnvironment(
            uri,
            ssl_context=ssl_context,
        )

    async with environment:
        async with await environment.connection() as connection:
            async with await connection.management() as management:
                print("declaring exchange and queue")
                await management.declare_exchange(
                    ExchangeSpecification(name=exchange_name)
                )

                await management.declare_queue(
                    QuorumQueueSpecification(name=queue_name)
                    # QuorumQueueSpecification(name=queue_name, dead_letter_exchange="dead-letter")
                )

                print("binding queue to exchange")
                bind_name = await management.bind(
                    ExchangeToQueueBindingSpecification(
                        source_exchange=exchange_name,
                        destination_queue=queue_name,
                        binding_key=routing_key,
                    )
                )

                addr = AddressHelper.exchange_address(exchange_name, routing_key)
                addr_queue = AddressHelper.queue_address(queue_name)

                print("create a publisher and publish a test message")
                async with await connection.publisher(addr) as publisher:
                    print("purging the queue")
                    messages_purged = await management.purge_queue(queue_name)
                    print("messages purged: " + str(messages_purged))

                    # publish messages
                    for i in range(messages_to_publish):
                        status = await publisher.publish(
                            Message(body=Converter.string_to_bytes("test"))
                        )
                        if status.remote_state == OutcomeState.ACCEPTED:
                            print("message accepted")
                        elif status.remote_state == OutcomeState.RELEASED:
                            print("message not routed")
                        elif status.remote_state == OutcomeState.REJECTED:
                            print("message rejected")

                print(
                    "create a consumer and consume the test message - press control + c to terminate"
                )
                handler = MyMessageHandler()
                async with await connection.consumer(
                    addr_queue, message_handler=handler
                ) as consumer:
                    # Create stop event and signal handler
                    stop_event = asyncio.Event()

                    def handle_sigint():
                        print("\nCtrl+C detected, stopping consumer gracefully...")
                        stop_event.set()

                    # Register signal handler
                    loop = asyncio.get_running_loop()
                    loop.add_signal_handler(signal.SIGINT, handle_sigint)

                    try:
                        # Run consumer in background
                        consumer_task = asyncio.create_task(consumer.run())

                        # Wait for stop signal
                        await stop_event.wait()

                        # Stop consumer gracefully
                        print("Stopping consumer...")
                        await consumer.stop_processing()

                        # Wait for task to complete
                        try:
                            await asyncio.wait_for(consumer_task, timeout=3.0)
                        except asyncio.TimeoutError:
                            print("Consumer task timed out")

                    finally:
                        loop.remove_signal_handler(signal.SIGINT)

                print("cleanup")
                print("unbind")
                await management.unbind(bind_name)

                print("delete queue")
                await management.delete_queue(queue_name)

                print("delete exchange")
                await management.delete_exchange(exchange_name)


if __name__ == "__main__":
    asyncio.run(main())