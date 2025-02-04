from dataclasses import dataclass
from typing import Any, Optional, Dict

from .qpid.proton._data import symbol, Described
from .common import ExchangeType, QueueType

STREAM_FILTER_SPEC = "rabbitmq:stream-filter"
STREAM_OFFSET_SPEC = "rabbitmq:stream-offset-spec"
STREAM_FILTER_MATCH_UNFILTERED = "rabbitmq:stream-match-unfiltered"

@dataclass
class ExchangeSpecification:
    name: str
    arguments: dict[str, str]
    exchange_type: ExchangeType = ExchangeType.direct
    is_auto_delete: bool = False
    is_internal: bool = False
    is_durable: bool = True


@dataclass
class QueueInfo:
    name: str
    arguments: dict[str, Any]
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


class StreamFilterOptions:

    def __init__(self):
        self._filter_set: Dict[symbol, Described] = {}

    def offset(self, offset: int):
        self._filter_set[symbol('rabbitmq:stream-offset-spec')] = Described(symbol('rabbitmq:stream-offset-spec'), "first")

    def filters(self) -> Dict[symbol, Described]:
        return self._filter_set
