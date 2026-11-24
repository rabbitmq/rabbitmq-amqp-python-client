from unittest.mock import MagicMock

from rabbitmq_amqp_python_client import Environment


def test_environment_close_closes_every_connection() -> None:
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connections = [environment.connection() for _ in range(4)]
    for connection in connections:
        connection._conn = MagicMock()

    environment.close()

    assert [c._conn.close.call_count for c in connections] == [1, 1, 1, 1]
    assert environment.active_connections == 0
