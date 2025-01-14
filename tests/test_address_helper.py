from rabbitmq_amqp_python_client import (
    exchange_address,
    queue_address,
)


def test_encoding_queue_simple() -> None:
    queue = "my_queue"

    address = queue_address(queue)

    assert address == "/queues/my_queue"


def test_encoding_queue_hex() -> None:
    queue = "my_queue>"

    address = queue_address(queue)

    assert address == "/queues/my_queue%3E"


def test_encoding_exchange_hex() -> None:
    queue = "my_exchange/()"

    address = exchange_address(queue)

    assert address == "/exchanges/my_exchange%2F%28%29"
