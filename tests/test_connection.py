from rabbitmq_amqp_python_client import (
    ClientCert,
    Connection,
    SslConfigurationContext,
)


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
