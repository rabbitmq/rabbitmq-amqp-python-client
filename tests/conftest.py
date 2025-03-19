import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Environment,
    Event,
    OAuth2Options,
    PKCS12Store,
    PosixClientCert,
    PosixSslConfigurationContext,
    RecoveryConfiguration,
    WinClientCert,
    WinSslConfigurationContext,
    symbol,
)
from rabbitmq_amqp_python_client.ssl_configuration import (
    FriendlyName,
)

from .http_requests import delete_all_connections
from .utils import token

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture()
def environment(pytestconfig):
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    try:
        yield environment

    finally:
        environment.close()


@pytest.fixture()
def environment_auth(pytestconfig):
    token_string = token(datetime.now() + timedelta(milliseconds=2500))
    environment = Environment(
        uri="amqp://localhost:5672",
        oauth2_options=OAuth2Options(token=token_string),
    )
    try:
        yield environment

    finally:
        environment.close()


@pytest.fixture()
def connection(pytestconfig):
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = environment.connection()
    connection.dial()
    try:
        yield connection

    finally:
        environment.close()


@pytest.fixture()
def connection_with_reconnect(pytestconfig):
    environment = Environment(
        uri="amqp://guest:guest@localhost:5672/",
        recovery_configuration=RecoveryConfiguration(active_recovery=True),
    )
    connection = environment.connection()
    connection.dial()
    try:
        yield connection

    finally:
        environment.close()


@pytest.fixture()
def ssl_context(pytestconfig):
    if sys.platform == "win32":
        return WinSslConfigurationContext(
            ca_store=PKCS12Store(path=".ci/certs/server_localhost.p12"),
            client_cert=WinClientCert(
                store=PKCS12Store(path=".ci/certs/client_localhost.p12"),
                disambiguation_method=FriendlyName(name="1"),
            ),
        )
    else:
        return PosixSslConfigurationContext(
            ca_cert=".ci/certs/ca_certificate.pem",
            client_cert=PosixClientCert(
                client_cert=".ci/certs/client_localhost_certificate.pem",
                client_key=".ci/certs/client_localhost_key.pem",
            ),
        )


@pytest.fixture()
def connection_ssl(pytestconfig, ssl_context):

    environment = Environment(
        "amqps://guest:guest@localhost:5671/",
        ssl_context=ssl_context,
    )
    connection = environment.connection()
    connection.dial()
    try:
        yield connection

    finally:
        environment.close()


@pytest.fixture()
def management(pytestconfig):
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = environment.connection()
    connection.dial()
    try:
        management = connection.management()
        yield management

    finally:
        environment.close()


@pytest.fixture()
def consumer(pytestconfig):
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = environment.connection()
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
            raise ConsumerTestException("consumed")


class MyMessageHandlerAcceptStreamOffsetReconnect(AMQPMessagingHandler):

    def __init__(self, starting_offset: Optional[int] = None):
        super().__init__()
        self._received = 0
        self._starting_offset = starting_offset

    def on_message(self, event: Event):
        if self._received == 5:
            delete_all_connections()
        self.delivery_context.accept(event)
        self._received = self._received + 1
        if self._received == 10:
            raise ConsumerTestException("consumed")


class MyMessageHandlerNoack(AMQPMessagingHandler):

    def __init__(self):
        super().__init__(auto_settle=False)
        self._received = 0

    def on_message(self, event: Event):
        self._received = self._received + 1
        if self._received == 1000:
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
            raise ConsumerTestException("consumed")


class MyMessageHandlerRequeue(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._received = 0

    def on_message(self, event: Event):
        self.delivery_context.requeue(event)
        self._received = self._received + 1
        if self._received == 1000:
            # event.connection.close()
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
            raise ConsumerTestException("consumed")
