from dataclasses import dataclass
from datetime import timedelta
from typing import Optional


@dataclass
class QueueSpecification:
    name: str
    auto_expires: Optional[timedelta] = None
    message_ttl: Optional[timedelta] = None
    overflow_behaviour: Optional[str] = None
    single_active_consumer: Optional[bool] = None
    dead_letter_exchange: Optional[str] = None
    dead_letter_routing_key: Optional[str] = None
    max_len: Optional[int] = None
    max_len_bytes: Optional[int] = None
    leader_locator: Optional[str] = None


@dataclass
class ClassicQueueSpecification(QueueSpecification):
    max_priority: Optional[int] = None
    is_auto_delete: bool = False
    is_exclusive: bool = False
    is_durable: bool = True


@dataclass
class QuorumQueueSpecification(QueueSpecification):
    deliver_limit: Optional[int] = None
    dead_letter_strategy: Optional[str] = None
    quorum_initial_group_size: Optional[int] = None
    cluster_target_group_size: Optional[int] = None


@dataclass
class StreamSpecification:
    name: str
    max_len_bytes: Optional[int] = None
    max_age: Optional[timedelta] = None
    stream_max_segment_size_bytes: Optional[int] = None
    stream_filter_size_bytes: Optional[int] = None
    initial_group_size: Optional[int] = None
    leader_locator: Optional[str] = None
