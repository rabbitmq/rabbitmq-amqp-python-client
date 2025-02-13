from typing import Optional

import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    ClientCert,
    Environment,
    Event,
    SslConfigurationContext,
    symbol,
)


@pytest.fixture()
def environment(pytestconfig):
    environment = Environment()
    try:
        yield environment

    finally:
        environment.close()


@pytest.fixture()
def connection(pytestconfig):
    environment = Environment()
    connection = environment.connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    try:
        yield connection

    finally:
        environment.close()


@pytest.fixture()
def connection_ssl(pytestconfig):
    environment = Environment()
    ca_cert_file = ".ci/certs/ca_certificate.pem"
    client_cert = ".ci/certs/client_certificate.pem"
    client_key = ".ci/certs/client_key.pem"
    connection = environment.connection(
        "amqps://guest:guest@localhost:5671/",
        ssl_context=SslConfigurationContext(
            ca_cert=ca_cert_file,
            client_cert=ClientCert(client_cert=client_cert, client_key=client_key),
        ),
    )
    connection.dial()
    try:
        yield connection

    finally:
        environment.close()


@pytest.fixture()
def management(pytestconfig):
    environment = Environment()
    connection = environment.connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    try:
        management = connection.management()
        yield management

    finally:
        environment.close()


@pytest.fixture()
def consumer(pytestconfig):
    environment = Environment()
    connection = environment.connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
    try:
        queue_name = "test-queue"
        addr_queue = AddressHelper.queue_address(queue_name)
        consumer = connection.consumer(addr_queue)
        yield consumer

    finally:
        consumer.close()
        environment.close()


class ConsumerTestException(BaseException):
    # Constructor or Initializer
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return repr(self.msg)


class MyMessageHandlerAccept(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        # print("received message: " + str(event.message.body))
        self.delivery_context.accept(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerAcceptStreamOffset(AMQPMessagingHandler):

    def __init__(self, starting_offset: Optional[int] = None):
        super().__init__()
        self._received = 0
        self._starting_offset = starting_offset

    def on_message(self, event: Event):
        if self._starting_offset is not None:
            assert event.message.annotations["x-stream-offset"] == self._starting_offset
            self._starting_offset = self._starting_offset + 1
        self.delivery_context.accept(event)
        self._received = self._received + 1
        if self._received == 10:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerNoack(AMQPMessagingHandler):

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


class MyMessageHandlerDiscard(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        self.delivery_context.discard(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerDiscardWithAnnotations(AMQPMessagingHandler):

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


class MyMessageHandlerRequeue(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        self.delivery_context.requeue(event)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")


class MyMessageHandlerRequeueWithAnnotations(AMQPMessagingHandler):

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


class MyMessageHandlerRequeueWithInvalidAnnotations(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        annotations = {}
        annotations[symbol("invalid")] = "x-test1"
        self.delivery_context.requeue_with_annotations(event, annotations)
        self._received = self._received + 1
        if self._received == 1000:
            event.connection.close()
            raise ConsumerTestException("consumed")
