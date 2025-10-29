import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    ArgumentOutOfRangeException,
    AsyncConnection,
    AsyncEnvironment,
    QuorumQueueSpecification,
)
from rabbitmq_amqp_python_client.utils import Converter

from ..conftest import (
    ConsumerTestException,
    MyMessageHandlerAccept,
    MyMessageHandlerDiscard,
    MyMessageHandlerDiscardWithAnnotations,
    MyMessageHandlerNoack,
    MyMessageHandlerRequeue,
    MyMessageHandlerRequeueWithAnnotations,
    MyMessageHandlerRequeueWithInvalidAnnotations,
)
from .fixtures import *  # noqa: F401, F403
from .utils import (
    async_cleanup_dead_lettering,
    async_publish_messages,
    async_setup_dead_lettering,
)


@pytest.mark.asyncio
async def test_async_consumer_sync_queue_accept(
    async_connection: AsyncConnection,
) -> None:
    queue_name = "test-queue-sync-accept"
    messages_to_send = 100
    management = await async_connection.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    consumer = await async_connection.consumer(
        destination=AddressHelper.queue_address(queue_name)
    )

    consumed = 0

    # publish messages_to_send messages
    await async_publish_messages(async_connection, messages_to_send, queue_name)

    # consumer synchronously without handler
    for i in range(messages_to_send):
        message = await consumer.consume()
        if Converter.bytes_to_string(message.body) == "test{}".format(i):  # type: ignore
            consumed += 1

    await consumer.close()
    await management.delete_queue(queue_name)
    await management.close()

    assert consumed == messages_to_send


@pytest.mark.asyncio
async def test_async_consumer_invalid_destination(
    async_connection: AsyncConnection,
) -> None:
    queue_name = "test-queue-sync-invalid-accept"
    consumer = None

    with pytest.raises(ArgumentOutOfRangeException):
        consumer = await async_connection.consumer(destination="/invalid/" + queue_name)

    if consumer is not None:
        await consumer.close()


@pytest.mark.asyncio
async def test_async_consumer_async_queue_accept(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    messages_to_send = 1000
    queue_name = "test-queue-async-accept"

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    await async_publish_messages(async_connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()
    consumer = await connection_consumer.consumer(
        destination=AddressHelper.queue_address(queue_name),
        message_handler=MyMessageHandlerAccept(),
    )

    try:
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    await consumer.close()

    message_count = await management.purge_queue(queue_name)
    await management.delete_queue(queue_name)
    await management.close()

    assert message_count == 0


@pytest.mark.asyncio
async def test_async_consumer_async_queue_no_ack(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    messages_to_send = 1000
    queue_name = "test-queue-async-no-ack"

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    await async_publish_messages(async_connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()
    consumer = await connection_consumer.consumer(
        destination=AddressHelper.queue_address(queue_name),
        message_handler=MyMessageHandlerNoack(),
    )

    try:
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    await consumer.close()
    message_count = await management.purge_queue(queue_name)
    await management.delete_queue(queue_name)
    await management.close()

    assert message_count == messages_to_send


@pytest.mark.asyncio
async def test_async_consumer_async_queue_with_discard(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    messages_to_send = 1000

    queue_dead_lettering = "queue-dead-letter"
    queue_name = "test-queue-async-discard"
    exchange_dead_lettering = "exchange-dead-letter"
    binding_key = "key-dead-letter"

    management = await async_connection.management()

    # configuring dead lettering
    bind_path = await async_setup_dead_lettering(management)
    addr_queue = AddressHelper.queue_address(queue_name)

    await management.declare_queue(
        QuorumQueueSpecification(
            name=queue_name,
            dead_letter_exchange=exchange_dead_lettering,
            dead_letter_routing_key=binding_key,
        )
    )

    await async_publish_messages(async_connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()
    consumer = await connection_consumer.consumer(
        destination=addr_queue,
        message_handler=MyMessageHandlerDiscard(),
    )

    try:
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    await consumer.close()

    message_count = await management.purge_queue(queue_name)
    await management.delete_queue(queue_name)

    message_count_dead_lettering = await management.purge_queue(queue_dead_lettering)
    await async_cleanup_dead_lettering(management, bind_path)

    await management.close()

    assert message_count == 0
    # check dead letter queue
    assert message_count_dead_lettering == messages_to_send


@pytest.mark.asyncio
async def test__async_consumer_async_queue_with_discard_with_annotations(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    messages_to_send = 1000

    queue_dead_lettering = "queue-dead-letter"
    queue_name = "test-queue-async-discard-annotations"
    exchange_dead_lettering = "exchange-dead-letter"
    binding_key = "key-dead-letter"

    management = await async_connection.management()

    await management.declare_queue(
        QuorumQueueSpecification(
            name=queue_name,
            dead_letter_exchange=exchange_dead_lettering,
            dead_letter_routing_key=binding_key,
        )
    )

    await async_publish_messages(async_connection, messages_to_send, queue_name)

    bind_path = await async_setup_dead_lettering(management)
    addr_queue = AddressHelper.queue_address(queue_name)
    addr_queue_dl = AddressHelper.queue_address(queue_dead_lettering)

    # we closed the connection so we need to open a new one
    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()
    consumer = await connection_consumer.consumer(
        destination=addr_queue,
        message_handler=MyMessageHandlerDiscardWithAnnotations(),
    )

    try:
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    await consumer.close()

    # check for added annotation
    new_consumer = await async_connection.consumer(addr_queue_dl)
    message = await new_consumer.consume()
    await new_consumer.close()

    message_count = await management.purge_queue(queue_name)
    await management.delete_queue(queue_name)

    message_count_dead_lettering = await management.purge_queue(queue_dead_lettering)
    await async_cleanup_dead_lettering(management, bind_path)

    await management.close()

    assert "x-opt-string" in message.annotations  # type: ignore
    assert message_count == 0
    # check dead letter queue
    assert message_count_dead_lettering == messages_to_send


@pytest.mark.asyncio
async def test_async_consumer_async_queue_with_requeue(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    messages_to_send = 1000
    queue_name = "test-queue-async-requeue"

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)
    await async_publish_messages(async_connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()

    consumer = await connection_consumer.consumer(
        destination=addr_queue,
        message_handler=MyMessageHandlerRequeue(),
    )

    try:
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    await consumer.close()

    message_count = await management.purge_queue(queue_name)

    await management.delete_queue(queue_name)
    await management.close()

    assert message_count > 0


@pytest.mark.asyncio
async def test_async_consumer_async_queue_with_requeue_with_annotations(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    messages_to_send = 1000
    queue_name = "test-queue-async-requeue"

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)
    await async_publish_messages(async_connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()

    consumer = await connection_consumer.consumer(
        destination=addr_queue,
        message_handler=MyMessageHandlerRequeueWithAnnotations(),
    )

    try:
        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    await consumer.close()

    # check for added annotation
    new_consumer = await async_connection.consumer(addr_queue)
    message = await new_consumer.consume()
    await new_consumer.close()

    message_count = await management.purge_queue(queue_name)

    await management.delete_queue(queue_name)
    await management.close()

    assert "x-opt-string" in message.annotations  # type: ignore
    assert message_count > 0


@pytest.mark.asyncio
async def test_async_consumer_async_queue_with_requeue_with_invalid_annotations(
    async_connection: AsyncConnection, async_environment: AsyncEnvironment
) -> None:
    messages_to_send = 1000
    test_failure = True
    queue_name = "test-queue-async-requeue"

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    addr_queue = AddressHelper.queue_address(queue_name)
    await async_publish_messages(async_connection, messages_to_send, queue_name)

    # we closed the connection so we need to open a new one
    connection_consumer = await async_environment.connection()
    await connection_consumer.dial()

    consumer = None
    try:
        consumer = await connection_consumer.consumer(
            destination=addr_queue,
            message_handler=MyMessageHandlerRequeueWithInvalidAnnotations(),
        )

        await consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass
    except ArgumentOutOfRangeException:
        test_failure = False

    if consumer is not None:
        await consumer.close()

    await management.delete_queue(queue_name)
    await management.close()

    assert test_failure is False
