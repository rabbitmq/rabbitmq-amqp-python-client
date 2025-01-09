from dataclasses import dataclass
from typing import Optional

from .common import ExchangeType, QueueType


@dataclass
class ExchangeSpecification:
    name: str
    arguments: dict[str, str]
    exchange_type: ExchangeType = ExchangeType.direct
    is_auto_delete: bool = False
    is_internal: bool = False
    is_durable: bool = True


@dataclass
class QueueSpecification:
    name: str
    arguments: dict[str, str]
    queue_type: QueueType = QueueType.quorum
    dead_letter_routing_key: str = ""
    is_exclusive: Optional[bool] = None
    max_len_bytes: Optional[int] = None
    dead_letter_exchange: str = ""
    is_auto_delete: bool = False
    is_durable: bool = True


@dataclass
class QueueInfo:
    name: str
    arguments: dict[str, str]
    queue_type: QueueType = QueueType.quorum
    is_exclusive: Optional[bool] = None
    is_auto_delete: bool = False
    is_durable: bool = True
    leader: str = ""
    members: str = ""
    message_count: int = 0
    consumer_count: int = 0


@dataclass
class BindingSpecification:
    source_exchange: str
    destination_queue: str
    binding_key: str
