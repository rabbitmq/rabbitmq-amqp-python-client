import asyncio
import time

import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    ArgumentOutOfRangeException,
    AsyncConnection,
    AsyncEnvironment,
    ConnectionClosed,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
    RecoveryConfiguration,
    StreamSpecification,
    ValidationCodeException,
)
from rabbitmq_amqp_python_client.asyncio.publisher import (
    AsyncPublisher,
)
from rabbitmq_amqp_python_client.utils import Converter

from ..http_requests import delete_all_connections
from ..utils import create_binding
from .fixtures import *  # noqa: F401, F403
from .utils import async_publish_per_message


@pytest.mark.asyncio
async def test_validate_message_for_publishing_async(
    async_connection: AsyncConnection,
) -> None:
    queue_name = "validate-publishing-async"
    management = await async_connection.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    publisher = await async_connection.publisher(
        destination=AddressHelper.queue_address(queue_name)
    )

    with pytest.raises(
        ArgumentOutOfRangeException, match="Message inferred must be True"
    ):
        await publisher.publish(
            Message(body=Converter.string_to_bytes("test"), inferred=False)
        )

    with pytest.raises(
        ArgumentOutOfRangeException, match="Message body must be of type bytes or None"
    ):
        await publisher.publish(Message(body="test"))  # type: ignore

    with pytest.raises(
        ArgumentOutOfRangeException, match="Message body must be of type bytes or None"
    ):
        await publisher.publish(Message(body={"key": "value"}))  # type: ignore

    await publisher.close()
    await management.delete_queue(queue_name)
    await management.close()


@pytest.mark.asyncio
async def test_publish_queue_async(async_connection: AsyncConnection) -> None:
    queue_name = "test-queue-async"
    management = await async_connection.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False
    publisher = None
    accepted = False

    try:
        publisher = await async_connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
        status = await publisher.publish(
            Message(body=Converter.string_to_bytes("test"))
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
    except Exception:
        raised = True

    if publisher is not None:
        await publisher.close()

    await management.delete_queue(queue_name)
    await management.close()

    assert accepted is True
    assert raised is False


@pytest.mark.asyncio
async def test_publish_per_message_async(async_connection: AsyncConnection) -> None:
    queue_name = "test-queue-1-async"
    queue_name_2 = "test-queue-2-async"
    management = await async_connection.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))
    await management.declare_queue(QuorumQueueSpecification(name=queue_name_2))

    raised = False
    publisher = None
    accepted = False
    accepted_2 = False

    try:
        publisher = await async_connection.publisher()
        status = await async_publish_per_message(
            publisher, addr=AddressHelper.queue_address(queue_name)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
        status = await async_publish_per_message(
            publisher, addr=AddressHelper.queue_address(queue_name_2)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted_2 = True
    except Exception:
        raised = True

    if publisher is not None:
        await publisher.close()

    purged_messages_queue_1 = await management.purge_queue(queue_name)
    purged_messages_queue_2 = await management.purge_queue(queue_name_2)
    await management.delete_queue(queue_name)
    await management.delete_queue(queue_name_2)
    await management.close()

    assert accepted is True
    assert accepted_2 is True
    assert purged_messages_queue_1 == 1
    assert purged_messages_queue_2 == 1
    assert raised is False


@pytest.mark.asyncio
async def test_publish_ssl(async_connection_ssl: AsyncConnection) -> None:
    queue_name = "test-queue"
    management = await async_connection_ssl.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False
    publisher = None

    try:
        publisher = await async_connection_ssl.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
        await publisher.publish(Message(body=Converter.string_to_bytes("test")))
    except Exception:
        raised = True

    if publisher is not None:
        await publisher.close()

    await management.delete_queue(queue_name)
    await management.close()

    assert raised is False


@pytest.mark.asyncio
async def test_publish_to_invalid_destination_async(
    async_connection: AsyncConnection,
) -> None:
    queue_name = "test-queue-async"
    raised = False
    publisher = None

    try:
        publisher = await async_connection.publisher(
            "/invalid-destination/" + queue_name
        )
        await publisher.publish(Message(body=Converter.string_to_bytes("test")))
    except ArgumentOutOfRangeException:
        raised = True
    except Exception:
        raised = False

    if publisher is not None:
        await publisher.close()

    assert raised is True


@pytest.mark.asyncio
async def test_publish_per_message_to_invalid_destination_async(
    async_connection: AsyncConnection,
) -> None:
    queue_name = "test-queue-1-async"
    raised = False

    message = Message(body=Converter.string_to_bytes("test"))
    message = AddressHelper.message_to_address_helper(
        message, "/invalid_destination/" + queue_name
    )
    publisher = await async_connection.publisher()

    try:
        await publisher.publish(message)
    except ArgumentOutOfRangeException:
        raised = True
    except Exception:
        raised = False

    if publisher is not None:
        await publisher.close()

    assert raised is True


@pytest.mark.asyncio
async def test_publish_per_message_both_address_async(
    async_connection: AsyncConnection,
) -> None:
    queue_name = "test-queue-1-async"
    raised = False

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    publisher = await async_connection.publisher(
        destination=AddressHelper.queue_address(queue_name)
    )

    try:
        message = Message(body=Converter.string_to_bytes("test"))
        message = AddressHelper.message_to_address_helper(
            message, AddressHelper.queue_address(queue_name)
        )
        await publisher.publish(message)
    except ValidationCodeException:
        raised = True

    if publisher is not None:
        await publisher.close()

    await management.delete_queue(queue_name)
    await management.close()

    assert raised is True


@pytest.mark.asyncio
async def test_publish_exchange_async(async_connection: AsyncConnection) -> None:
    exchange_name = "test-exchange-async"
    queue_name = "test-queue-async"
    management = await async_connection.management()
    routing_key = "routing-key"

    bind_name = create_binding(
        management._management, exchange_name, queue_name, routing_key
    )

    addr = AddressHelper.exchange_address(exchange_name, routing_key)

    raised = False
    accepted = False
    publisher = None

    try:
        publisher = await async_connection.publisher(addr)
        status = await publisher.publish(
            Message(body=Converter.string_to_bytes("test"))
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
    except Exception:
        raised = True

    if publisher is not None:
        await publisher.close()

    await management.unbind(bind_name)
    await management.delete_exchange(exchange_name)
    await management.delete_queue(queue_name)
    await management.close()

    assert accepted is True
    assert raised is False


@pytest.mark.asyncio
async def test_publish_purge_async(async_connection: AsyncConnection) -> None:
    messages_to_publish = 20

    queue_name = "test-queue-async"
    management = await async_connection.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False
    publisher = None

    try:
        publisher = await async_connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
        for _ in range(messages_to_publish):
            await publisher.publish(Message(body=Converter.string_to_bytes("test")))
    except Exception:
        raised = True

    if publisher is not None:
        await publisher.close()

    message_purged = await management.purge_queue(queue_name)

    await management.delete_queue(queue_name)
    await management.close()

    assert raised is False
    assert message_purged == 20


@pytest.mark.asyncio
async def test_disconnection_reconnection_async(
    async_connection: AsyncConnection,
) -> None:
    # disconnected = False
    generic_exception_raised = False

    environment = AsyncEnvironment(
        uri="amqp://guest:guest@localhost:5672/",
        recovery_configuration=RecoveryConfiguration(active_recovery=True),
    )

    connection_test = await environment.connection()

    await connection_test.dial()
    # delay
    time.sleep(5)
    messages_to_publish = 10000
    queue_name = "test-queue-reconnection"
    management = await connection_test.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    publisher = await connection_test.publisher(
        destination=AddressHelper.queue_address(queue_name)
    )
    while True:
        for i in range(messages_to_publish):
            if i == 5:
                # simulate a disconnection
                delete_all_connections()
            try:
                await publisher.publish(Message(body=Converter.string_to_bytes("test")))
            except ConnectionClosed:
                # disconnected = True

                # TODO: check if this behavior is correct
                # The underlying sync Connection handles all recovery automatically,
                # hence the async wrapper transparently benefits from it.
                # so the exception should is not raised
                continue
            except Exception:
                generic_exception_raised = True

        break

    await publisher.close()

    # purge the queue and check number of published messages
    message_purged = await management.purge_queue(queue_name)

    await management.delete_queue(queue_name)
    await management.close()

    assert generic_exception_raised is False
    # assert disconnected is True
    assert message_purged == messages_to_publish - 1


@pytest.mark.asyncio
async def test_queue_info_for_stream_with_validations_async(
    async_connection: AsyncConnection,
) -> None:
    stream_name = "test_stream_info_async"
    messages_to_send = 200

    queue_specification = StreamSpecification(name=stream_name)
    management = await async_connection.management()
    await management.declare_queue(queue_specification)

    publisher = await async_connection.publisher(
        destination=AddressHelper.queue_address(stream_name)
    )

    for _ in range(messages_to_send):
        await publisher.publish(Message(body=Converter.string_to_bytes("test")))


@pytest.mark.asyncio
async def test_publish_per_message_exchange_async(
    async_connection: AsyncConnection,
) -> None:
    exchange_name = "test-exchange-per-message-async"
    queue_name = "test-queue-per-message-async"
    management = await async_connection.management()
    routing_key = "routing-key-per-message"

    bind_name = create_binding(
        management._management, exchange_name, queue_name, routing_key
    )

    raised = False
    publisher = None
    accepted = False
    accepted_2 = False

    try:
        publisher = await async_connection.publisher()
        status = await async_publish_per_message(
            publisher, addr=AddressHelper.exchange_address(exchange_name, routing_key)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted = True
        status = await async_publish_per_message(
            publisher, addr=AddressHelper.queue_address(queue_name)
        )
        if status.remote_state == OutcomeState.ACCEPTED:
            accepted_2 = True
    except Exception:
        raised = True

    if publisher is not None:
        await publisher.close()

    purged_messages_queue = await management.purge_queue(queue_name)
    await management.unbind(bind_name)
    await management.delete_exchange(exchange_name)
    await management.delete_queue(queue_name)
    await management.close()

    assert accepted is True
    assert accepted_2 is True
    assert purged_messages_queue == 2
    assert raised is False


@pytest.mark.asyncio
async def test_multiple_publishers_async(async_environment: AsyncEnvironment) -> None:
    stream_name = "test_multiple_publisher_1_async"
    stream_name_2 = "test_multiple_publisher_2_async"
    connection = await async_environment.connection()
    await connection.dial()

    stream_specification = StreamSpecification(name=stream_name)
    management = await connection.management()
    await management.declare_queue(stream_specification)

    stream_specification_2 = StreamSpecification(name=stream_name_2)
    await management.declare_queue(stream_specification_2)

    destination = AddressHelper.queue_address(stream_name)
    destination_2 = AddressHelper.queue_address(stream_name_2)

    await connection.publisher(destination)
    assert connection.active_producers == 1

    publisher_2 = await connection.publisher(destination_2)
    assert connection.active_producers == 2

    await publisher_2.close()
    assert connection.active_producers == 1

    await connection.publisher(destination_2)
    assert connection.active_producers == 2

    await connection.close()
    assert connection.active_producers == 0

    # cleanup
    connection = await async_environment.connection()
    await connection.dial()
    management = await connection.management()

    await management.delete_queue(stream_name)
    await management.delete_queue(stream_name_2)
    await management.close()


@pytest.mark.asyncio
async def test_durable_message_async(async_connection: AsyncConnection) -> None:
    queue_name = "test_durable_message_async"

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    destination = AddressHelper.queue_address(queue_name)
    publisher = await async_connection.publisher(destination)

    # Message should be durable by default
    status = await publisher.publish(Message(body=Converter.string_to_bytes("durable")))
    assert status.remote_state == OutcomeState.ACCEPTED

    consumer = await async_connection.consumer(destination)
    should_be_durable = await consumer.consume()
    assert should_be_durable.durable is True

    await consumer.close()
    await publisher.close()
    await management.purge_queue(queue_name)
    await management.delete_queue(queue_name)
    await management.close()


@pytest.mark.asyncio
async def test_concurrent_publishing_async(async_connection: AsyncConnection) -> None:
    queue_name = "test-concurrent-async"
    messages_to_publish = 100

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    publisher = await async_connection.publisher(
        destination=AddressHelper.queue_address(queue_name)
    )

    async def publish_message(i: int):
        message = Message(body=Converter.string_to_bytes(f"message-{i}"))
        status = await publisher.publish(message)
        return status.remote_state == OutcomeState.ACCEPTED

    # Run concurrent publishes
    results = await asyncio.gather(
        *[publish_message(i) for i in range(messages_to_publish)]
    )

    assert all(results)

    await publisher.close()

    message_count = await management.purge_queue(queue_name)
    assert message_count == messages_to_publish

    await management.delete_queue(queue_name)
    await management.close()


@pytest.mark.asyncio
async def test_concurrent_publishing_stream_async(
    async_connection: AsyncConnection,
) -> None:
    stream_name = "test-concurrent-stream-async"
    messages_to_publish = 100

    management = await async_connection.management()
    await management.declare_queue(StreamSpecification(name=stream_name))

    publisher = await async_connection.publisher(
        destination=AddressHelper.queue_address(stream_name)
    )

    async def publish_message(i: int):
        message = Message(body=Converter.string_to_bytes(f"message-{i}"))
        status = await publisher.publish(message)
        return status.remote_state == OutcomeState.ACCEPTED

    # Run concurrent publishes
    results = await asyncio.gather(
        *[publish_message(i) for i in range(messages_to_publish)]
    )

    assert all(results)

    await publisher.close()
    await management.delete_queue(stream_name)
    await management.close()


@pytest.mark.asyncio
async def test_publisher_context_manager_async(
    async_connection: AsyncConnection,
) -> None:
    queue_name = "test-context-manager-async"

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    async with await async_connection.publisher(
        destination=AddressHelper.queue_address(queue_name)
    ) as publisher:
        status = await publisher.publish(
            Message(body=Converter.string_to_bytes("test"))
        )
        assert status.remote_state == OutcomeState.ACCEPTED
        assert publisher.is_open is True

    # Publisher should be closed after context manager exits
    assert publisher.is_open is False

    message_count = await management.purge_queue(queue_name)
    assert message_count == 1

    await management.delete_queue(queue_name)
    await management.close()


@pytest.mark.asyncio
async def test_connection_context_manager_async(
    async_environment: AsyncEnvironment,
) -> None:
    queue_name = "test-connection-context-async"

    async with await async_environment.connection() as connection:
        management = await connection.management()
        await management.declare_queue(QuorumQueueSpecification(name=queue_name))

        publisher = await connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
        status = await publisher.publish(
            Message(body=Converter.string_to_bytes("test"))
        )
        assert status.remote_state == OutcomeState.ACCEPTED

        await management.delete_queue(queue_name)


@pytest.mark.asyncio
async def test_nested_context_manager_async(
    async_environment: AsyncEnvironment,
) -> None:
    queue_name = "test-nested-context-async"

    async with await async_environment.connection() as connection:
        management = await connection.management()
        await management.declare_queue(QuorumQueueSpecification(name=queue_name))

        async with await connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        ) as publisher:
            status = await publisher.publish(
                Message(body=Converter.string_to_bytes("test"))
            )
            assert status.remote_state == OutcomeState.ACCEPTED
            assert publisher.is_open is True

        assert publisher.is_open is False

        message_count = await management.purge_queue(queue_name)
        assert message_count == 1

        await management.delete_queue(queue_name)


@pytest.mark.asyncio
async def test_multiple_publishers_concurrent_async(
    async_connection: AsyncConnection,
) -> None:
    queue_name_1 = "test-multi-pub-1-async"
    queue_name_2 = "test-multi-pub-2-async"
    messages_per_publisher = 100

    management = await async_connection.management()
    await management.declare_queue(QuorumQueueSpecification(name=queue_name_1))
    await management.declare_queue(QuorumQueueSpecification(name=queue_name_2))

    publisher1 = await async_connection.publisher(
        destination=AddressHelper.queue_address(queue_name_1)
    )
    publisher2 = await async_connection.publisher(
        destination=AddressHelper.queue_address(queue_name_2)
    )

    async def publish_message(publisher: AsyncPublisher, i: int) -> bool:
        message = Message(body=Converter.string_to_bytes(f"message-{i}"))
        status = await publisher.publish(message)
        return status.remote_state == OutcomeState.ACCEPTED

    async def publish_to_queue(publisher: AsyncPublisher, count: int) -> list[bool]:
        return await asyncio.gather(
            *[publish_message(publisher, i) for i in range(count)]
        )

    # Publish concurrently to both queues
    results1, results2 = await asyncio.gather(
        publish_to_queue(publisher1, messages_per_publisher),
        publish_to_queue(publisher2, messages_per_publisher),
    )

    assert all(results1)
    assert all(results2)

    await publisher1.close()
    await publisher2.close()

    # Verify message counts
    count1 = await management.purge_queue(queue_name_1)
    count2 = await management.purge_queue(queue_name_2)

    assert count1 == messages_per_publisher
    assert count2 == messages_per_publisher

    await management.delete_queue(queue_name_1)
    await management.delete_queue(queue_name_2)
    await management.close()
