from importlib import metadata

from .address_helper import AddressHelper
from .amqp_consumer_handler import AMQPMessagingHandler
from .common import ExchangeType, QueueType
from .connection import Connection
from .consumer import Consumer
from .entities import (
    BindingSpecification,
    ExchangeSpecification,
)
from .exceptions import ArgumentOutOfRangeException
from .management import Management
from .publisher import Publisher
from .qpid.proton._data import symbol  # noqa: E402
from .qpid.proton._delivery import Delivery
from .qpid.proton._events import Event
from .qpid.proton._message import Message
from .qpid.proton.handlers import MessagingHandler
from .queues import (
    ClassicQueueSpecification,
    QuorumQueueSpecification,
    StreamSpecification,
)

try:
    __version__ = metadata.version(__package__)
    __license__ = metadata.metadata(__package__)["license"]
except metadata.PackageNotFoundError:
    __version__ = "dev"
    __license__ = None

del metadata

__all__ = [
    "Connection",
    "Management",
    "ExchangeSpecification",
    "QuorumQueueSpecification",
    "ClassicQueueSpecification",
    "StreamSpecification",
    "BindingSpecification",
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
]
