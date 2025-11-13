import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    AsyncConnection,
    AsyncEnvironment,
    Converter,
    Message,
    OffsetSpecification,
    StreamConsumerOptions,
    StreamSpecification,
    ValidationCodeException,
)
from rabbitmq_amqp_python_client.entities import (
    MessageProperties,
    StreamFilterOptions,
)
from rabbitmq_amqp_python_client.qpid.proton import Event

from ..conftest import (
    ConsumerTestException,
    MyMessageHandlerAcceptStreamOffset,
    MyMessageHandlerAcceptStreamOffsetReconnect,
)
from .fixtures import *  # noqa: F401, F403
from .utils import async_publish_messages


@pytest.mark.asyncio
async def test_stream_read_from_last_default_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_validation_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue, message_handler=MyMessageHandlerAcceptStreamOffset()
        )
        await async_publish_messages(async_connection, messages_to_send, stream_name)
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_read_from_last_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_read_from_last_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            consumer_options=StreamConsumerOptions(
                offset_specification=OffsetSpecification.last
            ),
        )
        await async_publish_messages(async_connection, messages_to_send, stream_name)
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_read_from_offset_zero_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_read_from_offset_zero_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # publish and then consume
    await async_publish_messages(async_connection, messages_to_send, stream_name)

    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(0),
            consumer_options=StreamConsumerOptions(offset_specification=0),
        )
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_read_from_offset_first_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_read_from_offset_first_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # publish and then consume
    await async_publish_messages(async_connection, messages_to_send, stream_name)

    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(0),
            consumer_options=StreamConsumerOptions(OffsetSpecification.first),
        )
        await consumer.run()
    #  ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_read_from_offset_ten_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_read_from_offset_ten_async"
    messages_to_send = 20

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # publish and then consume
    await async_publish_messages(async_connection, messages_to_send, stream_name)

    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(10),
            consumer_options=StreamConsumerOptions(offset_specification=10),
        )
        await consumer.run()
    # ack to terminate the consumer
    # this will finish after 10 messages read
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_filtering_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_filtering_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(values=["banana"])
            ),
        )

        # send with annotations filter banana
        await async_publish_messages(
            async_connection,
            messages_to_send,
            stream_name,
            ["banana"],
        )

        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_filtering_mixed_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_info_with_filtering_mixed_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            # check we are reading just from offset 10 as just banana filtering applies
            message_handler=MyMessageHandlerAcceptStreamOffset(10),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(values=["banana"])
            ),
        )

        # consume and then publish
        await async_publish_messages(
            async_connection,
            messages_to_send,
            stream_name,
            ["apple"],
        )
        await async_publish_messages(
            async_connection,
            messages_to_send,
            stream_name,
            ["banana"],
        )

        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_filtering_not_present_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    stream_name = "test_stream_filtering_not_present_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()

    consumer = await connection_consumer.consumer(
        addr_queue,
        consumer_options=StreamConsumerOptions(
            filter_options=StreamFilterOptions(values=["apple"])
        ),
    )

    # send with annotations filter banana
    await async_publish_messages(
        async_connection,
        messages_to_send,
        stream_name,
        ["banana"],
    )

    with pytest.raises(Exception):
        await consumer.consume(timeout=1)

    await consumer.close()
    await management.delete_queue(stream_name)
    await management.close()


@pytest.mark.asyncio
async def test_stream_match_unfiltered_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_match_unfiltered_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerAcceptStreamOffset(),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    values=["banana"], match_unfiltered=True
                )
            ),
        )

        # unfiltered messages
        await async_publish_messages(async_connection, messages_to_send, stream_name)
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_stream_reconnection_async(
    async_connection_with_reconnect: AsyncConnection,
    async_environment: AsyncEnvironment,
) -> None:
    consumer = None
    stream_name = "test_stream_reconnection_async"
    messages_to_send = 10

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection_with_reconnect.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    # consume and then publish
    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            # disconnection and check happens here
            message_handler=MyMessageHandlerAcceptStreamOffsetReconnect(),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    values=["banana"], match_unfiltered=True
                )
            ),
        )

        await async_publish_messages(
            async_connection_with_reconnect, messages_to_send, stream_name
        )

        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


class MyMessageHandlerMessagePropertiesFilter(AMQPMessagingHandler):
    def __init__(self):
        super().__init__()

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        assert event.message.subject == "important_15"
        assert event.message.group_id == "group_15"
        assert event.message.body == Converter.string_to_bytes("hello_15")
        raise ConsumerTestException("consumed")


@pytest.mark.asyncio
async def test_stream_filter_message_properties_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_filter_message_properties_async"
    messages_to_send = 30

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerMessagePropertiesFilter(),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    message_properties=MessageProperties(
                        subject="important_15", group_id="group_15"
                    )
                )
            ),
        )

        publisher = await async_connection.publisher(addr_queue)
        for i in range(messages_to_send):
            msg = Message(
                body=Converter.string_to_bytes(f"hello_{i}"),
                subject=f"important_{i}",
                group_id=f"group_{i}",
            )
            await publisher.publish(msg)
        await publisher.close()

        await consumer.run()
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


class MyMessageHandlerApplicationPropertiesFilter(AMQPMessagingHandler):
    def __init__(self):
        super().__init__()

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        assert event.message.application_properties == {"key": "value_17"}
        raise ConsumerTestException("consumed")


@pytest.mark.asyncio
async def test_stream_filter_application_properties_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_application_message_properties_async"
    messages_to_send = 30

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerApplicationPropertiesFilter(),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    application_properties={"key": "value_17"},
                )
            ),
        )

        publisher = await async_connection.publisher(addr_queue)
        for i in range(messages_to_send):
            msg = Message(
                body=Converter.string_to_bytes(f"hello_{i}"),
                application_properties={"key": f"value_{i}"},
            )
            await publisher.publish(msg)
        await publisher.close()

        await consumer.run()
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


class MyMessageHandlerSQLFilter(AMQPMessagingHandler):
    def __init__(self):
        super().__init__()

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        assert event.message.body == Converter.string_to_bytes("the_right_one_sql")
        assert event.message.subject == "something_in_the_filter"
        assert event.message.reply_to == "the_reply_to"
        assert (
            event.message.application_properties["a_in_the_filter_key"]
            == "a_in_the_filter_value"
        )
        raise ConsumerTestException("consumed")


@pytest.mark.asyncio
async def test_stream_filter_sql_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_filter_sql_async"
    messages_to_send = 30

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.delete_queue(stream_name)
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)
    sql = (
        "properties.subject LIKE '%in_the_filter%' AND properties.reply_to = 'the_reply_to' "
        "AND a_in_the_filter_key = 'a_in_the_filter_value'"
    )

    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerSQLFilter(),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(sql=sql)
            ),
        )

        publisher = await async_connection.publisher(addr_queue)

        # Won't match
        for i in range(messages_to_send):
            msg = Message(body=Converter.string_to_bytes(f"hello_{i}"))
            await publisher.publish(msg)

        # The only one that will match
        msg_match = Message(
            body=Converter.string_to_bytes("the_right_one_sql"),
            subject="something_in_the_filter",
            reply_to="the_reply_to",
            application_properties={"a_in_the_filter_key": "a_in_the_filter_value"},
        )
        await publisher.publish(msg_match)
        await publisher.close()

        await consumer.run()
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


class MyMessageHandlerMixingDifferentFilters(AMQPMessagingHandler):
    def __init__(self):
        super().__init__()

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        assert event.message.annotations["x-stream-filter-value"] == "the_value_filter"
        assert event.message.application_properties == {"key": "app_value_9999"}
        assert event.message.subject == "important_9999"
        assert event.message.body == Converter.string_to_bytes("the_right_one_9999")
        raise ConsumerTestException("consumed")


@pytest.mark.asyncio
async def test_stream_filter_mixing_different_async(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    consumer = None
    stream_name = "test_stream_filter_mixing_different_async"
    messages_to_send = 30

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.delete_queue(stream_name)
    await management.declare_queue(queue_specification)

    addr_queue = AddressHelper.queue_address(stream_name)

    try:
        connection_consumer = await async_environment.connection()
        await connection_consumer.dial()
        consumer = await connection_consumer.consumer(
            addr_queue,
            message_handler=MyMessageHandlerMixingDifferentFilters(),
            consumer_options=StreamConsumerOptions(
                filter_options=StreamFilterOptions(
                    values=["the_value_filter"],
                    application_properties={"key": "app_value_9999"},
                    message_properties=MessageProperties(subject="important_9999"),
                )
            ),
        )

        publisher = await async_connection.publisher(addr_queue)

        # All these messages will be filtered out
        for i in range(messages_to_send):
            msg = Message(body=Converter.string_to_bytes(f"hello_{i}"))
            await publisher.publish(msg)

        import asyncio

        await asyncio.sleep(
            1
        )  # Wait to ensure messages are published in different chunks

        msg = Message(
            body=Converter.string_to_bytes("the_right_one_9999"),
            annotations={"x-stream-filter-value": "the_value_filter"},
            application_properties={"key": "app_value_9999"},
            subject="important_9999",
        )
        await publisher.publish(msg)
        await publisher.close()

        await consumer.run()
    except ConsumerTestException:
        pass
    finally:
        if consumer is not None:
            await consumer.close()
        await management.delete_queue(stream_name)
        await management.close()


@pytest.mark.asyncio
async def test_consumer_options_validation_async() -> None:
    try:
        x = StreamConsumerOptions(filter_options=StreamFilterOptions(sql="test"))
        x.validate({"4.0.0": True, "4.1.0": False, "4.2.0": False})
        assert False
    except ValidationCodeException:
        assert True

    try:
        x = StreamConsumerOptions(
            filter_options=StreamFilterOptions(
                message_properties=MessageProperties(subject="important_9999")
            )
        )
        x.validate({"4.0.0": True, "4.1.0": True, "4.2.0": False})
        assert True
    except ValidationCodeException:
        assert False

    try:
        x = StreamConsumerOptions(
            filter_options=StreamFilterOptions(
                application_properties={"key": "app_value_9999"}
            )
        )
        x.validate({"4.0.0": True, "4.1.0": True, "4.2.0": False})
        assert True
    except ValidationCodeException:
        assert False
