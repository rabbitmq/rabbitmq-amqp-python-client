from unittest.mock import MagicMock

import pytest

from rabbitmq_amqp_python_client import Environment
from rabbitmq_amqp_python_client.qpid.proton._exceptions import Timeout


def test_environment_close_closes_every_connection() -> None:
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connections = [environment.connection() for _ in range(4)]
    for connection in connections:
        connection._conn = MagicMock()

    environment.close()

    assert [c._conn.close.call_count for c in connections] == [1, 1, 1, 1]
    assert environment.active_connections == 0


@pytest.mark.parametrize("transport_fails", [False, True])
def test_environment_close_closes_transport_after_link_timeout(
    environment: Environment, transport_fails: bool
) -> None:
    connection = environment.connection()
    transport = MagicMock()
    connection._conn = transport
    if transport_fails:
        transport.close.side_effect = RuntimeError("transport close failed")
    sender = transport.create_sender.return_value
    sender.link.state = 0
    sender.close.side_effect = Timeout("publisher close timed out")
    connection.publisher()

    with pytest.raises(RuntimeError) as error:
        environment.close()

    transport.close.assert_called_once_with()
    assert str(error.value) == (
        "Errors closing connections: Exception closing connection: "
        "publisher close timed out"
    )
    assert environment.active_connections == 0
