import pytest

from rabbitmq_amqp_python_client import (
    Connection,
    Event,
    MessageAck,
    MessagingHandler,
    queue_address,
    symbol,
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


@pytest.fixture()
def consumer(pytestconfig):
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    try:
        queue_name = "test-queue"
        addr_queue = queue_address(queue_name)
        consumer = connection.consumer(addr_queue)
        yield consumer

    finally:
        consumer.close()
        connection.close()


class ConsumerTestException(BaseException):
    # Constructor or Initializer
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return repr(self.msg)


class MyMessageHandlerAccept(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        MessageAck.accept(event)
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
            # Workaround to terminate the Consumer and notify the test when all messages are consumed
            raise ConsumerTestException("consumed")


class MyMessageHandlerDiscard(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        MessageAck.discard(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerDiscardWithAnnotations(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        annotations = {}
        annotations[symbol("x-opt-string")] = "x-test1"
        MessageAck.discard_with_annotations(event, annotations)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerRequeue(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        MessageAck.requeue(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerRequeueWithAnnotations(MessagingHandler):

    def __init__(self):
        super().__init__(auto_accept=False, auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        annotations = {}
        annotations[symbol("x-opt-string")] = "x-test1"
        MessageAck.requeue_with_annotations(event, annotations)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")
