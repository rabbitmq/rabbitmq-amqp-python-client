# type: ignore

import asyncio
import signal

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    AsyncEnvironment,
    ConnectionClosed,
    Converter,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    Message,
    QuorumQueueSpecification,
)

MESSAGES_TO_PUBLISH = 50000


class MyMessageHandler(AMQPMessagingHandler):
    def __init__(self):
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event):
        if self._count % 1000 == 0:
            print(
                "received 1000 messages: "
                + Converter.bytes_to_string(event.message.body)
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

        self._count = self._count + 1

        if self._count == MESSAGES_TO_PUBLISH:
            print("received all messages")

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


async def main() -> None:
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"

    print("connection to amqp server")

    async with AsyncEnvironment(
        uri="amqp://guest:guest@localhost:5672/"
    ) as environment:
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

                print("create a publisher and publish messages")
                async with await connection.publisher(addr) as publisher:
                    print("purging the queue")
                    messages_purged = await management.purge_queue(queue_name)
                    print("messages purged: " + str(messages_purged))

                    # publishing messages with reconnection handling
                    while True:
                        try:
                            for i in range(MESSAGES_TO_PUBLISH):
                                if i % 1000 == 0:
                                    print("published 1000 messages...")

                                try:
                                    await publisher.publish(
                                        Message(body=Converter.string_to_bytes("test"))
                                    )
                                except ConnectionClosed:
                                    print("publisher connection closed, retrying...")
                                    await asyncio.sleep(1)
                                    continue

                            print("all messages published successfully")
                            break

                        except ConnectionClosed:
                            print("publisher connection closed, retrying batch...")
                            await asyncio.sleep(1)
                            continue

                print(
                    "create a consumer and consume messages - press control + c to terminate"
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
                        while True:
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

                                break

                            except ConnectionClosed:
                                print("consumer connection closed, reconnecting...")
                                await asyncio.sleep(1)
                                continue
                            except Exception as e:
                                print("consumer exited for exception " + str(e))
                                break

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
