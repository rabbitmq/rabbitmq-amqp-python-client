from dataclasses import dataclass
from typing import Optional

from .common import QueueType


@dataclass
class QueueSpecification:
    name: str
    expires: Optional[int] = None
    message_ttl: Optional[int] = None
    overflow: Optional[str] = None
    single_active_consumer: Optional[bool] = None
    dead_letter_exchange: Optional[str] = None
    dead_letter_routing_key: Optional[str] = None
    max_len: Optional[int] = None
    max_len_bytes: Optional[int] = None
    leader_locator: Optional[str] = None
    is_auto_delete: bool = False
    is_durable: bool = True


@dataclass
class ClassicQueueSpecification(QueueSpecification):
    queue_type: QueueType = QueueType.classic
    maximum_priority: Optional[int] = None


@dataclass
class QuorumQueueSpecification(QueueSpecification):
    queue_type: QueueType = QueueType.quorum
    deliver_limit: Optional[str] = None
    dead_letter_strategy: Optional[str] = None
    quorum_initial_group_size: Optional[int] = None
    cluster_target_size: Optional[int] = None


@dataclass
class StreamSpecification:
    name: str
    queue_type: QueueType = QueueType.stream
    max_len_bytes: Optional[str] = None
    max_time_retention: Optional[str] = None
    max_segment_size_in_bytes: Optional[str] = None
    filter_size: Optional[int] = None
    initial_group_size: Optional[int] = None
    leader_locator: Optional[str] = None
