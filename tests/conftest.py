import pytest

from rabbitmq_amqp_python_client import (
    Connection,
    Delivery,
    Event,
    MessagingHandler,
)


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


class ConsumerTestException(BaseException):
    # Constructor or Initializer
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return repr(self.msg)


class MyMessageHandlerAccept(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=True, auto_settle=True)
        self._received = 0

    def on_message(self, event: Event):
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerNoack(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        self._received = self._received + 1
        if self._received == 1000:
            event.receiver.close()
            event.connection.close()
            raise ConsumerTestException("consumed")

    def on_connection_closed(self, event: Event):
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        print("link closed")

    def on_rejected(self, event: Event) -> None:
        print("rejected")


class MyMessageHandlerReject(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=True)
        self._received = 0

    def on_message(self, event: Event):
        dlv = event.delivery
        dlv.update(Delivery.REJECTED)
        dlv.settle()
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerRequeue(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=True)
        self._received = 0

    def on_message(self, event: Event):
        dlv = event.delivery
        dlv.update(Delivery.RELEASED)
        dlv.settle()
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


def create_connection() -> Connection:
    connection_consumer = Connection("amqp://guest:guest@localhost:5672/")
    connection_consumer.dial()

    return connection_consumer
