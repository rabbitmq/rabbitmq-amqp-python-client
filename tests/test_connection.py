from rabbitmq_amqp_python_client import Connection


# Temporary this will be replaced by our connection Deal when we start the implementation
# For the moment we just need a test to run poetry run pytest without failing
def test_connection() -> None:
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    connection.close()
