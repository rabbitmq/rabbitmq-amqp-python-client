import time

from rabbitmq_amqp_python_client import (
    ClientCert,
    ConnectionClosed,
    Environment,
    SslConfigurationContext,
    StreamSpecification,
)

from .http_requests import delete_all_connections


def on_disconnected():

    print("disconnected")
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


def test_connection_ssl() -> None:
    ca_cert_file = ".ci/certs/ca_certificate.pem"
    client_cert = ".ci/certs/client_certificate.pem"
    client_key = ".ci/certs/client_key.pem"

    environment = Environment(
        "amqps://guest:guest@localhost:5671/",
        ssl_context=SslConfigurationContext(
            ca_cert=ca_cert_file,
            client_cert=ClientCert(client_cert=client_cert, client_key=client_key),
        ),
    )

    connection = environment.connection()
    connection.dial()

    environment.close()


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

    reconnected = False
    connection = None
    disconnected = False

    def on_disconnected():

        nonlocal connection

        # reconnect
        if connection is not None:
            connection = environment.connection()
            connection.dial()

        nonlocal reconnected
        reconnected = True

    environment = Environment(
        "amqp://guest:guest@localhost:5672/", on_disconnection_handler=on_disconnected
    )

    connection = environment.connection()
    connection.dial()

    # delay
    time.sleep(5)
    # simulate a disconnection
    delete_all_connections()
    # raise a reconnection
    management = connection.management()
    stream_name = "test_stream_info_with_validation"
    queue_specification = StreamSpecification(
        name=stream_name,
    )

    try:
        management.declare_queue(queue_specification)
    except ConnectionClosed:
        disconnected = True

    # check that we reconnected
    management = connection.management()
    management.declare_queue(queue_specification)
    management.delete_queue(stream_name)
    environment.close()
    management.close()

    assert disconnected is True
    assert reconnected is True
