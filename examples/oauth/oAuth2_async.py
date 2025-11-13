# type: ignore

import asyncio
import base64
import signal
from datetime import datetime, timedelta

import jwt

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    AsyncEnvironment,
    Converter,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    Message,
    OAuth2Options,
    OutcomeState,
    QuorumQueueSpecification,
)

MESSAGES_TO_PUBLISH = 100


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

        if self._count == MESSAGES_TO_PUBLISH:
            print("received all messages")

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


async def main() -> None:
    exchange_name = "oAuth2-test-exchange"
    queue_name = "oAuth2-example-queue"
    routing_key = "oAuth2-routing-key"

    print("connection to amqp server")
    oauth_token = token(
        datetime.now() + timedelta(milliseconds=10000),
        "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH",
    )

    async with AsyncEnvironment(
        uri="amqp://localhost:5672", oauth2_options=OAuth2Options(token=oauth_token)
    ) as environment:
        async with await environment.connection() as connection:
            async with await connection.management() as management:
                # you can refresh the oauth token with the connection api
                oauth_token = token(
                    datetime.now() + timedelta(milliseconds=10000),
                    "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH",
                )
                await connection.refresh_token(oauth_token)

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
                    for i in range(MESSAGES_TO_PUBLISH):
                        status = await publisher.publish(
                            Message(body=Converter.string_to_bytes("test_{}".format(i)))
                        )
                        if status.remote_state == OutcomeState.ACCEPTED:
                            print("message: test_{} accepted".format(i))
                        elif status.remote_state == OutcomeState.RELEASED:
                            print("message: test_{} not routed".format(i))
                        elif status.remote_state == OutcomeState.REJECTED:
                            print("message: test_{} rejected".format(i))

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


def token(duration: datetime, token: str) -> str:
    # Decode the base64 key
    decoded_key = base64.b64decode(token)

    # Define the claims
    claims = {
        "iss": "unit_test",
        "aud": "rabbitmq",
        "exp": int(duration.timestamp()),
        "scope": ["rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"],
        "random": random_string(6),
    }

    # Create the token with the claims and sign it
    jwt_token = jwt.encode(
        claims, decoded_key, algorithm="HS256", headers={"kid": "token-key"}
    )

    return jwt_token


# Helper function to generate a random string (replace with your implementation)
def random_string(length: int) -> str:
    import random
    import string

    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


if __name__ == "__main__":
    asyncio.run(main())
