import asyncio

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

        if self._count == MESSAGES_TO_PUBLISH:
            print("received all messages")
            # Stop the consumer by raising an exception
            raise StopConsumerException("All messages consumed")

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
                await management.declare_exchange(ExchangeSpecification(name=exchange_name))
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
                            Message(body=Converter.string_to_bytes("test message {} ".format(i)))
                        )
                        if status.remote_state == OutcomeState.ACCEPTED:
                            print("message accepted")

                print("create a consumer and consume the test message - press control + c to terminate to consume")
                handler = MyMessageHandler()
                async with await connection.consumer(addr_queue, message_handler=handler) as consumer:
                    # Run the consumer in a background task
                    consumer_task = asyncio.create_task(consumer.run())

                    try:
                        # Wait for the consumer to finish (e.g., by raising the exception)
                        await consumer_task
                    except StopConsumerException as e:
                        print(f"Consumer stopped: {e}")
                    except KeyboardInterrupt:
                        print("consumption interrupted by user, stopping consumer...")
                        await consumer.stop()

                print("unbind")
                await management.unbind(bind_name)

                print("delete queue")
                await management.delete_queue(queue_name)

                print("delete exchange")
                await management.delete_exchange(exchange_name)

if __name__ == "__main__":
    asyncio.run(main())