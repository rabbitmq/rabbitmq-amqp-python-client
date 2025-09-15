# type: ignore
import logging

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    ConnectionClosed,
    Converter,
    Environment,
    Event,
    Message,
    OffsetSpecification,
    StreamConsumerOptions,
    StreamFilterOptions,
    StreamSpecification,
)

MESSAGES_TO_PUBLISH = 100


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event):
        # only messages with banana filters and with subject yellow
        # and application property from = italy get received
        self._count = self._count + 1
        logger.info(
            "Received message: {}, subject {} application properties {} .[Total Consumed: {}]".format(
                Converter.bytes_to_string(event.message.body),
                event.message.subject,
                event.message.application_properties,
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
logger = logging.getLogger("[streams_with_filters]")
logger.setLevel(logging.INFO)


def main() -> None:
    """
    In this example we create a stream queue and a consumer with SQL filter

    See: https://www.rabbitmq.com/docs/next/stream-filtering#stage-2-amqp-filter-expressions
    """

    queue_name = "stream-example-with-sql-filter-queue"
    logger.info("Creating connection")
    environment = Environment("amqp://guest:guest@localhost:5672/")
    connection = create_connection(environment)
    management = connection.management()
    # delete the queue if it exists
    management.delete_queue(queue_name)
    # create a stream queue
    management.declare_queue(StreamSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)

    consumer_connection = create_connection(environment)
    sql = (
        "properties.subject LIKE '%in_the_filter%' "
        "AND a_in_the_filter_key = 'a_in_the_filter_value'"
    )

    consumer = consumer_connection.consumer(
        addr_queue,
        message_handler=MyMessageHandler(),
        stream_consumer_options=StreamConsumerOptions(
            offset_specification=OffsetSpecification.first,
            filter_options=StreamFilterOptions(sql=sql),
        ),
    )
    print(
        "create a consumer and consume the test message - press control + c to terminate to consume"
    )

    # print("create a publisher and publish a test message")
    publisher = connection.publisher(addr_queue)

    # publish messages won't match the filter
    for i in range(MESSAGES_TO_PUBLISH):
        publisher.publish(Message(Converter.string_to_bytes(body="apple: " + str(i))))

    # publish messages that will match the filter
    for i in range(MESSAGES_TO_PUBLISH):
        msqMatch = Message(
            body=Converter.string_to_bytes("the_right_one_sql"),
            # will match due of %
            subject="something_in_the_filter_{}".format(i),
            application_properties={"a_in_the_filter_key": "a_in_the_filter_value"},
        )
        publisher.publish(msqMatch)

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
    logger.info("consumer exited, deleting queue")
    management.delete_queue(queue_name)

    print("closing connections")
    management.close()
    print("after management closing")
    environment.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
