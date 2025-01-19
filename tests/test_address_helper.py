from rabbitmq_amqp_python_client import AddressHelper


def test_encoding_queue_simple() -> None:
    queue = "my_queue"

    address = AddressHelper.queue_address(queue)

    assert address == "/queues/my_queue"


def test_encoding_queue_hex() -> None:
    queue = "my_queue>"

    address = AddressHelper.queue_address(queue)

    assert address == "/queues/my_queue%3E"


def test_encoding_exchange_hex() -> None:
    queue = "my_exchange/()"

    address = AddressHelper.exchange_address(queue)

    assert address == "/exchanges/my_exchange%2F%28%29"
