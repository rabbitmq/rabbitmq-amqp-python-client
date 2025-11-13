import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from rabbitmq_amqp_python_client import (
    AsyncEnvironment,
    ConnectionClosed,
    PKCS12Store,
    PosixSslConfigurationContext,
    QuorumQueueSpecification,
    RecoveryConfiguration,
    StreamSpecification,
    ValidationCodeException,
    WinSslConfigurationContext,
)
from rabbitmq_amqp_python_client.qpid.proton import (
    ConnectionException,
)

from ..http_requests import (
    create_vhost,
    delete_all_connections,
    delete_vhost,
)
from ..utils import token
from .fixtures import *  # noqa: F401, F403


def on_disconnected():
    global disconnected
    disconnected = True


@pytest.mark.asyncio
async def test_async_connection() -> None:
    environment = AsyncEnvironment(uri="amqp://guest:guest@localhost:5672/")
    connection = await environment.connection()
    await connection.dial()
    await environment.close()


@pytest.mark.asyncio
async def test_async_environment_context_manager() -> None:
    async with AsyncEnvironment(
        uri="amqp://guest:guest@localhost:5672/"
    ) as environment:
        connection = await environment.connection()
        await connection.dial()


@pytest.mark.asyncio
async def test_async_connection_ssl(ssl_context) -> None:
    environment = AsyncEnvironment(
        "amqps://guest:guest@localhost:5671/",
        ssl_context=ssl_context,
    )
    if isinstance(ssl_context, PosixSslConfigurationContext):
        path = Path(ssl_context.ca_cert)
        assert path.is_file() is True
        assert path.exists() is True

        path = Path(ssl_context.client_cert.client_cert)  # type: ignore
        assert path.is_file() is True
        assert path.exists() is True
    elif isinstance(ssl_context, WinSslConfigurationContext):
        assert isinstance(ssl_context.ca_store, PKCS12Store)
        path = Path(ssl_context.ca_store.path)
        assert path.is_file() is True
        assert path.exists() is True

        assert isinstance(ssl_context.client_cert.store, PKCS12Store)  # type: ignore
        path = Path(ssl_context.client_cert.store.path)  # type: ignore
        assert path.is_file() is True
        assert path.exists() is True
    else:
        pytest.fail("Unsupported ssl context")

    connection = await environment.connection()
    await connection.dial()

    await environment.close()


@pytest.mark.asyncio
async def test_async_connection_oauth(async_environment_auth: AsyncEnvironment) -> None:
    connection = await async_environment_auth.connection()
    await connection.dial()
    management = await connection.management()
    await management.declare_queue(QuorumQueueSpecification(name="test-queue"))
    await management.close()
    await connection.close()


@pytest.mark.asyncio
async def test_async_connection_oauth_with_timeout(
    async_environment_auth: AsyncEnvironment,
) -> None:
    connection = await async_environment_auth.connection()
    await connection.dial()

    # let the token expire
    time.sleep(3)
    # token expired

    with pytest.raises(Exception):
        management = await connection.management()
        await management.declare_queue(QuorumQueueSpecification(name="test-queue"))
        await management.close()

    await connection.close()


@pytest.mark.asyncio
async def test_async_connection_oauth_refresh_token(
    async_environment_auth: AsyncEnvironment,
) -> None:
    connection = await async_environment_auth.connection()
    await connection.dial()

    # let the token expire
    time.sleep(1)
    # # token expired, refresh

    await connection.refresh_token(token(datetime.now() + timedelta(milliseconds=5000)))
    time.sleep(3)

    with pytest.raises(Exception):
        management = await connection.management()
        await management.declare_queue(QuorumQueueSpecification(name="test-queue"))
        await management.close()

    await connection.close()


@pytest.mark.asyncio
async def test_async_connection_oauth_refresh_token_with_disconnection(
    async_environment_auth: AsyncEnvironment,
) -> None:
    connection = await async_environment_auth.connection()
    await connection.dial()

    # let the token expire
    time.sleep(1)
    # # token expired, refresh

    await connection.refresh_token(token(datetime.now() + timedelta(milliseconds=5000)))
    delete_all_connections()
    time.sleep(3)

    with pytest.raises(Exception):
        management = await connection.management()
        await management.declare_queue(QuorumQueueSpecification(name="test-queue"))
        await management.close()

    await connection.close()


@pytest.mark.asyncio
async def test_async_environment_connections_management() -> None:
    enviroment = AsyncEnvironment(uri="amqp://guest:guest@localhost:5672/")

    connection1 = await enviroment.connection()
    await connection1.dial()
    connection2 = await enviroment.connection()
    await connection2.dial()
    connection3 = await enviroment.connection()
    await connection3.dial()

    assert enviroment.active_connections == 3

    # this shouldn't happen but we test it anyway
    await connection1.close()
    assert enviroment.active_connections == 2

    await connection2.close()
    assert enviroment.active_connections == 1

    await connection3.close()
    assert enviroment.active_connections == 0

    await enviroment.close()


@pytest.mark.asyncio
async def test_async_connection_reconnection() -> None:
    disconnected = False
    enviroment = AsyncEnvironment(
        uri="amqp://guest:guest@localhost:5672/",
        recovery_configuration=RecoveryConfiguration(active_recovery=True),
    )

    connection = await enviroment.connection()
    await connection.dial()

    # delay
    time.sleep(5)
    # simulate a disconnection
    # raise a reconnection
    management = await connection.management()

    delete_all_connections()

    stream_name = "test_stream_info_with_validation"
    queue_specification = StreamSpecification(
        name=stream_name,
    )

    try:
        await management.declare_queue(queue_specification)
    except ConnectionClosed:
        disconnected = True

    # check that we reconnected
    await management.declare_queue(queue_specification)
    await management.delete_queue(stream_name)
    await management.close()
    await enviroment.close()

    assert disconnected is True


@pytest.mark.asyncio
async def test_async_reconnection_parameters() -> None:
    enviroment = AsyncEnvironment(
        uri="amqp://guest:guest@localhost:5672/",
        recovery_configuration=RecoveryConfiguration(
            active_recovery=True,
            back_off_reconnect_interval=timedelta(milliseconds=100),
        ),
    )

    with pytest.raises(ValidationCodeException):
        await enviroment.connection()


@pytest.mark.asyncio
async def test_async_connection_vhost() -> None:
    vhost = "tmpVhost" + str(time.time())
    create_vhost(vhost)
    uri = "amqp://guest:guest@localhost:5672/{}".format(vhost)
    environment = AsyncEnvironment(uri=uri)
    connection = await environment.connection()
    await connection.dial()
    is_correct_vhost = connection._connection._conn.conn.hostname == "vhost:{}".format(vhost)  # type: ignore
    await environment.close()
    delete_vhost(vhost)

    assert is_correct_vhost is True


@pytest.mark.asyncio
async def test_async_connection_vhost_not_exists() -> None:
    vhost = "tmpVhost" + str(time.time())
    uri = "amqp://guest:guest@localhost:5672/{}".format(vhost)

    environment = AsyncEnvironment(uri=uri)

    with pytest.raises(ConnectionException):
        connection = await environment.connection()
        await connection.dial()
