from rabbitmq_amqp_python_client import (
    AddressHelper,
    Connection,
    OffsetSpecification,
    StreamFilterOptions,
    StreamSpecification,
)

from .conftest import (
    ConsumerTestException,
    MyMessageHandlerAcceptStreamOffset,
)
from .utils import create_connection, publish_messages


def test_stream_read_from_last_default(connection: Connection) -> None:

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
        connection_consumer = create_connection()
        consumer = connection_consumer.consumer(
            addr_queue, handler=MyMessageHandlerAcceptStreamOffset()
        )
        publish_messages(connection, messages_to_send, stream_name)
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_last(connection: Connection) -> None:

    consumer = None
    stream_name = "test_stream_info_with_validation"
    messages_to_send = 10

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    stream_filter_options = StreamFilterOptions()
    stream_filter_options.offset(OffsetSpecification.last)

    # consume and then publish
    try:
        connection_consumer = create_connection()
        consumer = connection_consumer.consumer(
            addr_queue,
            handler=MyMessageHandlerAcceptStreamOffset(),
            stream_filter_options=stream_filter_options,
        )
        publish_messages(connection, messages_to_send, stream_name)
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_offset_zero(connection: Connection) -> None:

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

    stream_filter_options = StreamFilterOptions()
    stream_filter_options.offset(0)

    try:
        connection_consumer = create_connection()
        consumer = connection_consumer.consumer(
            addr_queue,
            handler=MyMessageHandlerAcceptStreamOffset(0),
            stream_filter_options=stream_filter_options,
        )

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_offset_first(connection: Connection) -> None:

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

    stream_filter_options = StreamFilterOptions()
    stream_filter_options.offset(OffsetSpecification.first)

    try:
        connection_consumer = create_connection()
        consumer = connection_consumer.consumer(
            addr_queue,
            handler=MyMessageHandlerAcceptStreamOffset(0),
            stream_filter_options=stream_filter_options,
        )

        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)


def test_stream_read_from_offset_ten(connection: Connection) -> None:

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

    stream_filter_options = StreamFilterOptions()
    stream_filter_options.offset(10)

    try:
        connection_consumer = create_connection()
        consumer = connection_consumer.consumer(
            addr_queue,
            handler=MyMessageHandlerAcceptStreamOffset(10),
            stream_filter_options=stream_filter_options,
        )

        consumer.run()
    # ack to terminate the consumer
    # this will finish after 10 messages read
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)
