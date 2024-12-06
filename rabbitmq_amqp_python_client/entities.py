import enum
from collections import defaultdict
from dataclasses import dataclass


class ExchangeType(enum.Enum):
    direct = "direct"
    topic = "topic"
    fanout = "fanout"


@dataclass
class ExchangeSpecification:
    name: str
    arguments: dict
    is_auto_delete: bool = False
    is_durable: bool = True
    exchange_type: ExchangeType = ExchangeType.direct
