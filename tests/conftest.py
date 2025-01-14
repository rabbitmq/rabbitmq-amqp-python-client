import pytest

from rabbitmq_amqp_python_client import Connection


@pytest.fixture()
def connection(pytestconfig):
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    try:
        yield connection

    finally:
        connection.close()


@pytest.fixture()
def management(pytestconfig):
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    try:
        management = connection.management()
        yield management

    finally:
        management.close()
        connection.close()
