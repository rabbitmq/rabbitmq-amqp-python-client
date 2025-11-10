# type: ignore

import asyncio
import signal

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    AsyncEnvironment,
    Converter,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
)

MESSAGES_TO_PUBLISH = 100


class StopConsumerException(Exception):
    """Exception to signal consumer should stop"""

    pass


class MyMessageHandler(AMQPMessagingHandler):
    def __init__(self):
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event):
        print(
            "received message: {} ".format(
                Converter.bytes_to_string(event.message.body)
            )
        )

        self.delivery_context.accept(event)
        self._count = self._count + 1
        print("count " + str(self._count))

    def on_connection_closed(self, event: Event):
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        print("link closed")


async def main():
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
                    for i in range(MESSAGES_TO_PUBLISH):
                        status = await publisher.publish(
                            Message(
                                body=Converter.string_to_bytes(
                                    "test message {} ".format(i)
                                )
                            )
                        )
                        if status.remote_state == OutcomeState.ACCEPTED:
                            print("message accepted")

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

                print("unbind")
                await management.unbind(bind_name)

                print("delete queue")
                await management.delete_queue(queue_name)

                print("delete exchange")
                await management.delete_exchange(exchange_name)


if __name__ == "__main__":
    asyncio.run(main())
