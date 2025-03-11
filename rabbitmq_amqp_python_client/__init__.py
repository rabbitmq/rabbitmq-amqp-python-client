from importlib import metadata

from .address_helper import AddressHelper
from .amqp_consumer_handler import AMQPMessagingHandler
from .common import ExchangeType, QueueType
from .connection import Connection
from .consumer import Consumer
from .entities import (
    ExchangeCustomSpecification,
    ExchangeSpecification,
    ExchangeToExchangeBindingSpecification,
    ExchangeToQueueBindingSpecification,
    OffsetSpecification,
    RecoveryConfiguration,
    StreamOptions,
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
    "StreamOptions",
    "OffsetSpecification",
    "OutcomeState",
    "Environment",
    "ExchangeCustomSpecification",
    "RecoveryConfiguration",
]
