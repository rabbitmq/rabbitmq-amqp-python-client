import time

from rabbitmq_amqp_python_client import (
    ClientCert,
    Connection,
    ConnectionClosed,
    SslConfigurationContext,
    StreamSpecification,
)

from .http_requests import delete_all_connections


def on_disconnected():

    print("disconnected")
    global disconnected
    disconnected = True


def test_connection() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    connection.close()


def test_connection_ssl() -> None:
    ca_cert_file = ".ci/certs/ca_certificate.pem"
    client_cert = ".ci/certs/client_certificate.pem"
    client_key = ".ci/certs/client_key.pem"
    connection = Connection(
        "amqps://guest:guest@localhost:5671/",
        ssl_context=SslConfigurationContext(
            ca_cert=ca_cert_file,
            client_cert=ClientCert(client_cert=client_cert, client_key=client_key),
        ),
    )
    connection.dial()


def test_connection_reconnection() -> None:

    reconnected = False
    connection = None
    disconnected = False

    def on_disconnected():

        nonlocal connection

        # reconnect
        if connection is not None:
            connection = Connection("amqp://guest:guest@localhost:5672/")
            connection.dial()

        nonlocal reconnected
        reconnected = True

    connection = Connection(
        "amqp://guest:guest@localhost:5672/", on_disconnection_handler=on_disconnected
    )
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
    management.close()
    connection.close()

    assert disconnected is True
    assert reconnected is True
