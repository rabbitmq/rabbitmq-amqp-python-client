from importlib import metadata

from .common import QueueType
from .connection import Connection
from .entities import (
    BindingSpecification,
    ExchangeSpecification,
    QueueSpecification,
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
    "ExchangeSpecification",
    "QueueSpecification",
    "BindingSpecification",
    "QueueType",
]
