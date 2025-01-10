from importlib import metadata

from .address_helper import exchange_address, queue_address
from .common import QueueType
from .connection import Connection
from .entities import (
    BindingSpecification,
    ExchangeSpecification,
)
from .management import Management
from .publisher import Publisher
from .qpid.proton._message import Message
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
    "exchange_address",
    "queue_address",
    "Message",
]
