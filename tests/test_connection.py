import time
from datetime import datetime, timedelta
from pathlib import Path

from rabbitmq_amqp_python_client import (
    ConnectionClosed,
    Environment,
    QuorumQueueSpecification,
    RecoveryConfiguration,
    StreamSpecification,
    ValidationCodeException,
)

from .http_requests import delete_all_connections
from .utils import token


def on_disconnected():

    global disconnected
    disconnected = True


def test_connection() -> None:
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = environment.connection()
    connection.dial()
    environment.close()


def test_environment_context_manager() -> None:
    with Environment(uri="amqp://guest:guest@localhost:5672/") as environment:
        connection = environment.connection()
        connection.dial()


def test_connection_ssl(ssl_context) -> None:
    environment = Environment(
        "amqps://guest:guest@localhost:5671/",
        ssl_context=ssl_context,
    )
    path = Path(ssl_context.ca_cert)
    assert path.is_file() is True
    assert path.exists() is True

    path = Path(ssl_context.client_cert.client_cert)
    assert path.is_file() is True

    path = Path(ssl_context.client_cert.client_key)
    assert path.is_file() is True

    connection = environment.connection()
    connection.dial()

    environment.close()


def test_connection_oauth(environment_auth: Environment) -> None:

    connection = environment_auth.connection()
    connection.dial()
    management = connection.management()
    management.declare_queue(QuorumQueueSpecification(name="test-queue"))
    management.close()
    connection.close()


def test_connection_oauth_with_timeout(environment_auth: Environment) -> None:

    connection = environment_auth.connection()
    connection.dial()
    # let the token expire
    time.sleep(3)
    raised = False
    # token expired
    try:
        management = connection.management()
        management.declare_queue(QuorumQueueSpecification(name="test-queue"))
        management.close()
    except Exception:
        raised = True

    assert raised is True

    connection.close()


def test_connection_oauth_refresh_token(environment_auth: Environment) -> None:

    connection = environment_auth.connection()
    connection.dial()
    # let the token expire
    time.sleep(1)
    raised = False
    # token expired, refresh
    connection.refresh_token(token(datetime.now() + timedelta(milliseconds=5000)))
    time.sleep(3)
    try:
        management = connection.management()
        management.declare_queue(QuorumQueueSpecification(name="test-queue"))
    except Exception:
        raised = True

    assert raised is False
    connection.close()


def test_connection_oauth_refresh_token_with_disconnection(
    environment_auth: Environment,
) -> None:

    connection = environment_auth.connection()
    connection.dial()
    # let the token expire
    time.sleep(1)
    raised = False
    # generate new token
    connection.refresh_token(token(datetime.now() + timedelta(milliseconds=5000)))
    delete_all_connections()
    time.sleep(3)
    try:
        management = connection.management()
        management.declare_queue(QuorumQueueSpecification(name="test-queue"))
    except Exception:
        raised = True

    assert raised is False
    connection.close()


def test_environment_connections_management() -> None:

    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = environment.connection()
    connection.dial()
    connection2 = environment.connection()
    connection2.dial()
    connection3 = environment.connection()
    connection3.dial()

    assert environment.active_connections == 3

    # this shouldn't happen but we test it anyway
    connection.close()

    assert environment.active_connections == 2

    connection2.close()

    assert environment.active_connections == 1

    connection3.close()

    assert environment.active_connections == 0

    environment.close()


def test_connection_reconnection() -> None:

    disconnected = False

    environment = Environment(
        "amqp://guest:guest@localhost:5672/",
        recovery_configuration=RecoveryConfiguration(active_recovery=True),
    )

    connection = environment.connection()
    connection.dial()

    # delay
    time.sleep(5)
    # simulate a disconnection
    # raise a reconnection
    management = connection.management()

    delete_all_connections()

    stream_name = "test_stream_info_with_validation"
    queue_specification = StreamSpecification(
        name=stream_name,
    )

    try:
        management.declare_queue(queue_specification)
    except ConnectionClosed:
        disconnected = True

    # check that we reconnected
    management.declare_queue(queue_specification)
    management.delete_queue(stream_name)
    management.close()
    environment.close()

    assert disconnected is True


def test_reconnection_parameters() -> None:

    exception = False

    environment = Environment(
        "amqp://guest:guest@localhost:5672/",
        recovery_configuration=RecoveryConfiguration(
            active_recovery=True,
            back_off_reconnect_interval=timedelta(milliseconds=100),
        ),
    )

    try:
        environment.connection()
    except ValidationCodeException:
        exception = True

    assert exception is True
