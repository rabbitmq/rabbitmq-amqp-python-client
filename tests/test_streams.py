import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    Converter,
    Environment,
    OffsetSpecification,
    StreamConsumerOptions,
    StreamSpecification,
)
from rabbitmq_amqp_python_client.entities import (
    MessageProperties,
    StreamFilterOptions,
)
from rabbitmq_amqp_python_client.qpid.proton import (
    Event,
    Message,
)

from .conftest import (
    ConsumerTestException,
    MyMessageHandlerAcceptStreamOffset,
    MyMessageHandlerAcceptStreamOffsetReconnect,
)
from .utils import publish_messages


def test_stream_read_from_last_default(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_validation"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue, message_handler=MyMessageHandlerAcceptStreamOffset()
        )
        publish_messages(connection, messages_to_send, stream_name)
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_last(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_validation"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            stream_consumer_options=StreamConsumerOptions(
                offset_specification=OffsetSpecification.last
            ),
        )
        publish_messages(connection, messages_to_send, stream_name)
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_offset_zero(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_validation"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # publish and then consume
    publish_messages(connection, messages_to_send, stream_name)

    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(0),
            stream_consumer_options=StreamConsumerOptions(offset_specification=0),
        )

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_offset_first(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_validation"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # publish and then consume
    publish_messages(connection, messages_to_send, stream_name)

    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(0),
            stream_consumer_options=StreamConsumerOptions(OffsetSpecification.first),
        )

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_offset_ten(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_validation"
    messages_to_send = 20

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # publish and then consume
    publish_messages(connection, messages_to_send, stream_name)

    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(10),
            stream_consumer_options=StreamConsumerOptions(offset_specification=10),
        )

        consumer.run()
    # ack to terminate the consumer
    # this will finish after 10 messages read
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_filtering(connection: Connection, environment: Environment) -> None:
    consumer = None
    stream_name = "test_stream_info_with_filtering"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()

        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            stream_consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(values=["banana"])
            ),
        )
        # send with annotations filter banana
        publish_messages(connection, messages_to_send, stream_name, ["banana"])
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_filtering_mixed(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_filtering"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            # check we are reading just from offset 10 as just banana filtering applies
            message_handler=MyMessageHandlerAcceptStreamOffset(10),
            stream_consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(values=["banana"])
            ),
        )
        # send with annotations filter apple and then banana
        # consumer will read just from offset 10
        publish_messages(connection, messages_to_send, stream_name, ["apple"])
        publish_messages(connection, messages_to_send, stream_name, ["banana"])
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_filtering_not_present(
    connection: Connection, environment: Environment
) -> None:
    raised = False
    stream_name = "test_stream_info_with_filtering"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    connection_consumer = environment.connection()
    connection_consumer.dial()

    consumer = connection_consumer.consumer(
        addr_queue,
        stream_consumer_options=StreamConsumerOptions(
            filter_options=StreamFilterOptions(values=["apple"])
        ),
    )
    # send with annotations filter banana
    publish_messages(connection, messages_to_send, stream_name, ["banana"])

    try:
        consumer.consume(timeout=1)
    except Exception:
        # valid no message should arrive with filter banana so a timeout exception is raised
        raised = True

    consumer.close()

    management.delete_queue(stream_name)

    assert raised is True


def test_stream_match_unfiltered(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_filtering"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            stream_consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    values=["banana"], match_unfiltered=True
                )
            ),
        )
        # send with annotations filter banana
        publish_messages(connection, messages_to_send, stream_name)
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_reconnection(
    connection_with_reconnect: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_filtering"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection_with_reconnect.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            # disconnection and check happens here
            message_handler=MyMessageHandlerAcceptStreamOffsetReconnect(),
            stream_consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    values=["banana"], match_unfiltered=True
                )
            ),
        )
        # send with annotations filter banana
        publish_messages(connection_with_reconnect, messages_to_send, stream_name)
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


class MyMessageHandlerMessagePropertiesFilter(AMQPMessagingHandler):
    def __init__(
        self,
    ):
        super().__init__()

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        assert event.message.subject == "important_15"
        assert event.message.group_id == "group_15"
        assert event.message.body == Converter.string_to_bytes("hello_15")
        raise ConsumerTestException("consumed")


def test_stream_filter_message_properties(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_filter_message_properties"
    messages_to_send = 30

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerMessagePropertiesFilter(),
            stream_consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    message_properties=MessageProperties(
                        subject="important_15", group_id="group_15"
                    )
                )
            ),
        )
        publisher = connection.publisher(addr_queue)
        for i in range(messages_to_send):
            msg = Message(
                body=Converter.string_to_bytes("hello_{}".format(i)),
                subject="important_{}".format(i),
                group_id="group_{}".format(i),
            )
            publisher.publish(msg)

        publisher.close()

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    if consumer is not None:
        consumer.close()

    management.delete_queue(stream_name)


class MyMessageHandlerApplicationPropertiesFilter(AMQPMessagingHandler):
    def __init__(
        self,
    ):
        super().__init__()

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        assert event.message.application_properties == {"key": "value_17"}
        raise ConsumerTestException("consumed")


def test_stream_filter_application_properties(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_application_message_properties"
    messages_to_send = 30

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerApplicationPropertiesFilter(),
            stream_consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    application_properties={"key": "value_17"},
                )
            ),
        )
        publisher = connection.publisher(addr_queue)
        for i in range(messages_to_send):
            msg = Message(
                body=Converter.string_to_bytes("hello_{}".format(i)),
                application_properties={"key": "value_{}".format(i)},
            )
            publisher.publish(msg)

        publisher.close()

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    if consumer is not None:
        consumer.close()

    management.delete_queue(stream_name)


class MyMessageHandlerMixingDifferentFilters(AMQPMessagingHandler):
    def __init__(
        self,
    ):
        super().__init__()

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        assert event.message.annotations == {"x-stream-filter-value": "my_value"}
        assert event.message.application_properties == {"key": "value_17"}
        assert event.message.subject == "important_15"
        assert event.message.body == Converter.string_to_bytes("the_right_one")
        raise ConsumerTestException("consumed")


def test_stream_filter_mixing_different(
    connection: Connection, environment: Environment
) -> None:
    consumer = None
    stream_name = "test_stream_filter_mixing_different"
    messages_to_send = 30

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = environment.connection()
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerMixingDifferentFilters(),
            stream_consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    values=["my_value"],
                    application_properties={"key": "value_17"},
                    message_properties=MessageProperties(subject="important_15"),
                )
            ),
        )
        publisher = connection.publisher(addr_queue)
        # all these messages will be filtered out
        for i in range(messages_to_send):
            msg = Message(
                body=Converter.string_to_bytes("hello_{}".format(i)),
            )
            publisher.publish(msg)

        time.sleep(
            0.5
        )  # wait a bit to ensure messages are published in different chunks
        msg = Message(
            body=Converter.string_to_bytes("the_right_one"),
            annotations={"x-stream-filter-value": "my_value"},
            application_properties={"key": "value_17"},
            subject="important_15",
        )
        publisher.publish(msg)

        publisher.close()

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    if consumer is not None:
        consumer.close()

    management.delete_queue(stream_name)
