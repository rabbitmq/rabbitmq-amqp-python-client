from proton.utils import BlockingConnection


def test_connection() -> None:
    BlockingConnection("amqp://guest:guest@localhost:5672/")
