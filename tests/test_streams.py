from rabbitmq_amqp_python_client import (
    AddressHelper,
    Connection,
    Environment,
    OffsetSpecification,
    StreamOptions,
    StreamSpecification,
)

from .conftest import (
    ConsumerTestException,
    MyMessageHandlerAcceptStreamOffset,
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
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
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

    stream_filter_options = StreamOptions()
    stream_filter_options.offset(OffsetSpecification.last)

    # consume and then publish
    try:
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            stream_filter_options=stream_filter_options,
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

    stream_filter_options = StreamOptions()
    stream_filter_options.offset(0)

    try:
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(0),
            stream_filter_options=stream_filter_options,
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

    stream_filter_options = StreamOptions()
    stream_filter_options.offset(OffsetSpecification.first)

    try:
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(0),
            stream_filter_options=stream_filter_options,
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

    stream_filter_options = StreamOptions()
    stream_filter_options.offset(10)

    try:
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(10),
            stream_filter_options=stream_filter_options,
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
        stream_filter_options = StreamOptions()
        stream_filter_options.filter_values(["banana"])
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
        connection_consumer.dial()

        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            stream_filter_options=stream_filter_options,
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
        stream_filter_options = StreamOptions()
        stream_filter_options.filter_values(["banana"])
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            # check we are reading just from offset 10 as just banana filtering applies
            message_handler=MyMessageHandlerAcceptStreamOffset(10),
            stream_filter_options=stream_filter_options,
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
    stream_filter_options = StreamOptions()
    stream_filter_options.filter_values(["apple"])
    connection_consumer = environment.connection("amqp://guest:guest@localhost:5672/")
    connection_consumer.dial()

    consumer = connection_consumer.consumer(
        addr_queue, stream_filter_options=stream_filter_options
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
        stream_filter_options = StreamOptions()
        stream_filter_options.filter_values(["banana"])
        stream_filter_options.filter_match_unfiltered(True)
        connection_consumer = environment.connection(
            "amqp://guest:guest@localhost:5672/"
        )
        connection_consumer.dial()
        consumer = connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            stream_filter_options=stream_filter_options,
        )
        # send with annotations filter banana
        publish_messages(connection, messages_to_send, stream_name)
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)
