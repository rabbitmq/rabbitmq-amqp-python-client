from rabbitmq_amqp_python_client import Connection


def test_connection() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    connection.close()
