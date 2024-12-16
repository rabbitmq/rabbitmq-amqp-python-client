from rabbitmq_amqp_python_client import queue_address

def test_encoding_simple() -> None:
    queue = "my_queue"

    address = queue_address(queue)

    assert address == "/queues/my_queue"

def test_encoding_hex() -> None:
    queue = "my_queue>"

    address = queue_address(queue)

    assert address == "/queues/my_queue%3E"

