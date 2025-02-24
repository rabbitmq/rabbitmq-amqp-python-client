import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    ArgumentOutOfRangeException,
    Connection,
    ConnectionClosed,
    Environment,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
    StreamSpecification,
    ValidationCodeException,
)

import pytest
from .http_requests import delete_all_connections
from .utils import create_binding, publish_per_message

pytestmark = pytest.mark.asyncio

async def test_publish_queue(connection: Connection) -> None:

    queue_name = "test-queue"
    management = await connection.management()

    await management.declare_queue(QuorumQueueSpecification(name=queue_name))

    raised = False

    publisher = None
    accepted = False

    try:
        publisher = await connection.publisher(
            destination=AddressHelper.queue_address(queue_name)
        )
        status = await publisher.publish(Message(body="test"))
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

