from rabbitmq_amqp_python_client import Connection


def create_connection() -> Connection:
    connection_consumer = Connection("amqp://guest:guest@localhost:5672/")
    connection_consumer.dial()

    return connection_consumer
