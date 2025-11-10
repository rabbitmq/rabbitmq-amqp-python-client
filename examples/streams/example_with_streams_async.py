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
    Message,
    OffsetSpecification,
    StreamConsumerOptions,
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
            + Converter.bytes_to_string(event.message.body)
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
            print("received all messages")

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


async def main() -> None:
    queue_name = "stream-example-queue"

    print("connection to amqp server")
    async with AsyncEnvironment(
        uri="amqp://guest:guest@localhost:5672/"
    ) as environment:
        async with await environment.connection() as connection:
            async with await connection.management() as management:
                print("declaring stream queue")
                await management.declare_queue(StreamSpecification(name=queue_name))

                addr_queue = AddressHelper.queue_address(queue_name)

                print("create a publisher and publish messages to stream")
                async with await connection.publisher(addr_queue) as publisher:
                    # publish with a filter of apple
                    for i in range(MESSAGES_TO_PUBLISH):
                        await publisher.publish(
                            Message(
                                body=Converter.string_to_bytes("apple: " + str(i)),
                                annotations={"x-stream-filter-value": "apple"},
                            )
                        )

                    # publish with a filter of banana
                    for i in range(MESSAGES_TO_PUBLISH):
                        await publisher.publish(
                            Message(
                                body=Converter.string_to_bytes("banana: " + str(i)),
                                annotations={"x-stream-filter-value": "banana"},
                            )
                        )

                    print(f"Published {MESSAGES_TO_PUBLISH * 2} messages to stream")

                print(
                    "create a consumer and consume messages from stream - press control + c to terminate"
                )
                handler = MyMessageHandler()

                # Create consumer with stream options
                async with await connection.consumer(
                    addr_queue,
                    message_handler=handler,
                    # can be first, last, next or an offset long
                    # you can also specify stream filters with methods: apply_filters and filter_match_unfiltered
                    consumer_options=StreamConsumerOptions(
                        offset_specification=OffsetSpecification.first
                    ),
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
                                print("connection closed, reconnecting...")
                                await asyncio.sleep(1)
                                continue
                            except Exception as e:
                                print("consumer exited for exception " + str(e))
                                break

                    finally:
                        loop.remove_signal_handler(signal.SIGINT)

                print("cleanup")
                print("delete queue")
                await management.delete_queue(queue_name)


if __name__ == "__main__":
    asyncio.run(main())