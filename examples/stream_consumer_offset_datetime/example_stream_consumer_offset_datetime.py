# type: ignore
import datetime
import logging
import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    ConnectionClosed,
    Converter,
    Environment,
    Event,
    Message,
    StreamConsumerOptions,
    StreamSpecification,
)

MESSAGES_TO_PUBLISH = 100


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event):
        # only messages for the second send
        self._count = self._count + 1
        logger.info(
            "Received message: {} .[Total Consumed: {}]".format(
                Converter.bytes_to_string(event.message.body),
                self._count,
            )
        )
        self.delivery_context.accept(event)

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


def create_connection(environment: Environment) -> Connection:
    connection = environment.connection()
    connection.dial()

    return connection


logging.basicConfig()
logger = logging.getLogger("[stream_consumer_offset_datetime]")
logger.setLevel(logging.INFO)


def main() -> None:
    """
    Example of using a stream queue with a time-based offset for the consumer.
    The example creates a stream queue, publishes a first set of messages,
    waits for 3 seconds, then publishes a second set of messages. The consumer is configured to start consuming
    messages from the time the second set of messages was published. Thus, it only receives the second set of messages.
    """

    queue_name = "stream-example-consumer-offset-datetime"
    logger.info("Creating connection...")
    environment = Environment("amqp://guest:guest@localhost:5672/")
    connection = create_connection(environment)
    management = connection.management()
    # delete the queue if it exists
    management.delete_queue(queue_name)
    logger.info("Creating stream queue...")
    # create a stream queue
    management.declare_queue(StreamSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    # print("create a publisher and publish a test message")
    publisher = connection.publisher(addr_queue)
    logger.info("Publishing first set of messages...")
    for i in range(MESSAGES_TO_PUBLISH):
        publisher.publish(
            Message(
                Converter.string_to_bytes("First send: " + str(i)),
            )
        )

    logger.info("Publishing done. Waiting before publishing next set of messages...")
    time.sleep(3)  # wait 3 seconds before publishing next set of messages
    starting_from_here = datetime.datetime.now()
    # consumer should only receive messages published after this timestamp
    logger.info("Publishing second set of messages...")
    for i in range(MESSAGES_TO_PUBLISH):
        publisher.publish(
            Message(
                body=Converter.string_to_bytes("Second send: " + str(i)),
            )
        )

    publisher.close()
    logger.info("starting consumer from datetime: " + str(starting_from_here))
    consumer_connection = create_connection(environment)
    consumer = consumer_connection.consumer(
        addr_queue,
        message_handler=MyMessageHandler(),
        # the consumer will only receive messages starting from the time stored in: starting_from_here
        consumer_options=StreamConsumerOptions(offset_specification=starting_from_here),
    )
    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )

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
    logger.info("consumer exited, deleting queue")
    management.delete_queue(queue_name)

    print("closing connections")
    management.close()
    print("after management closing")
    environment.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
