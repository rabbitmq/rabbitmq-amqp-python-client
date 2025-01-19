import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    Connection,
    DeliveryConsumerHandler,
    Event,
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
        addr_queue = AddressHelper.queue_address(queue_name)
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


class MyMessageHandlerAccept(DeliveryConsumerHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        self.delivery_context.accept(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerNoack(DeliveryConsumerHandler):

    def __init__(self):
        super().__init__(auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        self._received = self._received + 1
        if self._received == 1000:
            event.receiver.close()
            event.connection.close()
            # Workaround to terminate the Consumer and notify the test when all messages are consumed
            raise ConsumerTestException("consumed")


class MyMessageHandlerDiscard(DeliveryConsumerHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        self.delivery_context.discard(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerDiscardWithAnnotations(DeliveryConsumerHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        annotations = {}
        annotations[symbol("x-opt-string")] = "x-test1"
        self.delivery_context.discard_with_annotations(event, annotations)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerRequeue(DeliveryConsumerHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        self.delivery_context.requeue(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerRequeueWithAnnotations(DeliveryConsumerHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        annotations = {}
        annotations[symbol("x-opt-string")] = "x-test1"
        self.delivery_context.requeue_with_annotations(event, annotations)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")
