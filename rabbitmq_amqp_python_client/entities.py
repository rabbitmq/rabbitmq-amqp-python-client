from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Union

from .common import ExchangeType, QueueType
from .qpid.proton._data import Described, symbol

STREAM_FILTER_SPEC = "rabbitmq:stream-filter"
STREAM_OFFSET_SPEC = "rabbitmq:stream-offset-spec"
STREAM_FILTER_MATCH_UNFILTERED = "rabbitmq:stream-match-unfiltered"


@dataclass
class ExchangeSpecification:
    name: str
    arguments: dict[str, str] = field(default_factory=dict)
    exchange_type: ExchangeType = ExchangeType.direct
    is_auto_delete: bool = False
    is_internal: bool = False
    is_durable: bool = True


@dataclass
class QueueInfo:
    name: str
    arguments: dict[str, Any]
    queue_type: QueueType = QueueType.classic
    is_exclusive: Optional[bool] = None
    is_auto_delete: bool = False
    is_durable: bool = True
    leader: str = ""
    members: str = ""
    message_count: int = 0
    consumer_count: int = 0


class OffsetSpecification(Enum):
    first = ("first",)
    next = ("next",)
    last = ("last",)


@dataclass
class BindingSpecification:
    source_exchange: str
    destination_queue: str
    binding_key: str


class StreamOptions:

    def __init__(self):  # type: ignore
        self._filter_set: Dict[symbol, Described] = {}

    def offset(self, offset_spefication: Union[OffsetSpecification, int]) -> None:
        if isinstance(offset_spefication, int):
            self._filter_set[symbol(STREAM_OFFSET_SPEC)] = Described(
                symbol(STREAM_OFFSET_SPEC), offset_spefication
            )
        else:
            self._filter_set[symbol(STREAM_OFFSET_SPEC)] = Described(
                symbol(STREAM_OFFSET_SPEC), offset_spefication.name
            )

    def filter_values(self, filters: list[str]) -> None:
        self._filter_set[symbol(STREAM_FILTER_SPEC)] = Described(
            symbol(STREAM_FILTER_SPEC), filters
        )

    def filter_match_unfiltered(self, filter_match_unfiltered: bool) -> None:
        self._filter_set[symbol(STREAM_FILTER_MATCH_UNFILTERED)] = Described(
            symbol(STREAM_FILTER_MATCH_UNFILTERED), filter_match_unfiltered
        )

    def filter_set(self) -> Dict[symbol, Described]:
        return self._filter_set
