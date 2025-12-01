from importlib import metadata

from .address_helper import AddressHelper
from .amqp_consumer_handler import AMQPMessagingHandler
from .asyncio import (
    AsyncConnection,
    AsyncConsumer,
    AsyncEnvironment,
    AsyncManagement,
    AsyncPublisher,
)
from .common import ExchangeType, QueueType
from .connection import Connection
from .consumer import Consumer
from .entities import (
    ConsumerOptions,
    DirectReplyToConsumerOptions,
    ExchangeCustomSpecification,
    ExchangeSpecification,
    ExchangeToExchangeBindingSpecification,
    ExchangeToQueueBindingSpecification,
    MessageProperties,
    OAuth2Options,
    OffsetSpecification,
    RecoveryConfiguration,
    StreamConsumerOptions,
    StreamFilterOptions,
)
from .environment import Environment
from .exceptions import (
    ArgumentOutOfRangeException,
    ValidationCodeException,
)
from .management import Management
from .publisher import Publisher
from .qpid.proton._data import symbol  # noqa: E402
from .qpid.proton._delivery import Delivery, Disposition
from .qpid.proton._events import Event
from .qpid.proton._message import Message
from .qpid.proton._utils import ConnectionClosed
from .qpid.proton.handlers import MessagingHandler
from .queues import (
    ClassicQueueSpecification,
    QuorumQueueSpecification,
    StreamSpecification,
)
from .ssl_configuration import (
    CurrentUserStore,
    LocalMachineStore,
    PKCS12Store,
    PosixClientCert,
    PosixSslConfigurationContext,
    WinClientCert,
    WinSslConfigurationContext,
)
from .utils import Converter

try:
    __version__ = metadata.version(__package__)
    __license__ = metadata.metadata(__package__)["license"]
except metadata.PackageNotFoundError:
    __version__ = "dev"
    __license__ = None

del metadata

OutcomeState = Disposition

__all__ = [
    "Connection",
    "Management",
    "ExchangeSpecification",
    "QuorumQueueSpecification",
    "ClassicQueueSpecification",
    "StreamSpecification",
    "DirectReplyToConsumerOptions",
    "ExchangeToQueueBindingSpecification",
    "ExchangeToExchangeBindingSpecification",
    "QueueType",
    "Publisher",
    "Message",
    "Consumer",
    "MessagingHandler",
    "Event",
    "Delivery",
    "symbol",
    "ExchangeType",
    "AddressHelper",
    "AMQPMessagingHandler",
    "ArgumentOutOfRangeException",
    "ValidationCodeException",
    "PosixSslConfigurationContext",
    "WinSslConfigurationContext",
    "PosixClientCert",
    "WinClientCert",
    "LocalMachineStore",
    "CurrentUserStore",
    "PKCS12Store",
    "ConnectionClosed",
    "StreamConsumerOptions",
    "StreamFilterOptions",
    "ConsumerOptions",
    "MessageProperties",
    "OffsetSpecification",
    "OutcomeState",
    "Environment",
    "ExchangeCustomSpecification",
    "RecoveryConfiguration",
    "OAuth2Options",
    "Converter",
    "AsyncConnection",
    "AsyncConsumer",
    "AsyncPublisher",
    "AsyncManagement",
    "AsyncEnvironment",
]
