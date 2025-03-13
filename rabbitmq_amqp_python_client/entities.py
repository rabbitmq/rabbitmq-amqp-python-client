from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, Optional, Union

from .common import ExchangeType, QueueType
from .exceptions import ValidationCodeException
from .qpid.proton._data import Described, symbol

STREAM_FILTER_SPEC = "rabbitmq:stream-filter"
STREAM_OFFSET_SPEC = "rabbitmq:stream-offset-spec"
STREAM_FILTER_MATCH_UNFILTERED = "rabbitmq:stream-match-unfiltered"


@dataclass
class ExchangeSpecification:
    """
    Specification for declaring a standard exchange in RabbitMQ.

    This class defines the properties and configuration for a standard exchange,
    including its type, durability, and other characteristics.

    Attributes:
        name: The name of the exchange
        arguments: Additional arguments for exchange configuration
        exchange_type: The type of exchange (direct, fanout, topic, etc.)
        is_auto_delete: Whether the exchange should be auto-deleted
        is_internal: Whether the exchange is internal only
        is_durable: Whether the exchange should survive broker restarts
    """

    name: str
    arguments: dict[str, str] = field(default_factory=dict)
    exchange_type: ExchangeType = ExchangeType.direct
    is_auto_delete: bool = False
    is_internal: bool = False
    is_durable: bool = True


@dataclass
class ExchangeCustomSpecification:
    """
    Specification for declaring a custom exchange type in RabbitMQ.

    Similar to ExchangeSpecification but allows for custom exchange types
    beyond the standard ones.

    Attributes:
        name: The name of the exchange
        exchange_type: The custom exchange type identifier
        arguments: Additional arguments for exchange configuration
        is_auto_delete: Whether the exchange should be auto-deleted
        is_internal: Whether the exchange is internal only
        is_durable: Whether the exchange should survive broker restarts
    """

    name: str
    exchange_type: str
    arguments: dict[str, str] = field(default_factory=dict)
    is_auto_delete: bool = False
    is_internal: bool = False
    is_durable: bool = True


@dataclass
class QueueInfo:
    """
    Information about a queue in RabbitMQ.

    This class represents the current state and configuration of a queue,
    including its properties and statistics.

    Attributes:
        name: The name of the queue
        arguments: Additional arguments used in queue configuration
        queue_type: The type of queue (classic, quorum, or stream)
        is_exclusive: Whether the queue is exclusive to one connection
        is_auto_delete: Whether the queue should be auto-deleted
        is_durable: Whether the queue survives broker restarts
        leader: The leader node for quorum queues
        members: The member nodes for quorum queues
        message_count: Current number of messages in the queue
        consumer_count: Current number of consumers
    """

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
    """
    Specification for stream offset positions.

    Defines the possible starting positions for consuming from a stream.

    Attributes:
        first: Start from the first message in the stream
        next: Start from the next message to arrive
        last: Start from the last message in the stream
    """

    first = ("first",)
    next = ("next",)
    last = ("last",)


@dataclass
class ExchangeToQueueBindingSpecification:
    """
    Specification for binding an exchange to a queue.

    Defines the relationship between a source exchange and a destination queue,
    optionally with a routing key.

    Attributes:
        source_exchange: The name of the source exchange
        destination_queue: The name of the destination queue
        binding_key: Optional routing key for the binding
    """

    source_exchange: str
    destination_queue: str
    binding_key: Optional[str] = None


@dataclass
class ExchangeToExchangeBindingSpecification:
    """
    Specification for binding an exchange to another exchange.

    Defines the relationship between two exchanges, optionally with a routing key.

    Attributes:
        source_exchange: The name of the source exchange
        destination_exchange: The name of the destination exchange
        binding_key: Optional routing key for the binding
    """

    source_exchange: str
    destination_exchange: str
    binding_key: Optional[str] = None


class StreamOptions:
    """
    Configuration options for stream queues.

    This class manages stream-specific options including filtering and offset specifications.

    Attributes:
        _filter_set: Dictionary of stream filter specifications

    Args:
        offset_specification: Either an OffsetSpecification enum value or
                                an integer offset
        filters: List of filter strings to apply to the stream
    """

    def __init__(
        self,
        offset_specification: Optional[Union[OffsetSpecification, int]] = None,
        filters: Optional[list[str]] = None,
        filter_match_unfiltered: bool = False,
    ):

        if offset_specification is None and filters is None:
            raise ValidationCodeException(
                "At least one between offset_specification and filters must be set when setting up filtering"
            )
        self._filter_set: Dict[symbol, Described] = {}
        if offset_specification is not None:
            self._offset(offset_specification)

        if filters is not None:
            self._filter_values(filters)

        if filter_match_unfiltered is True:
            self._filter_match_unfiltered(filter_match_unfiltered)

    def _offset(self, offset_specification: Union[OffsetSpecification, int]) -> None:
        """
        Set the offset specification for the stream.

        Args:
            offset_specification: Either an OffsetSpecification enum value or
                                an integer offset
        """
        if isinstance(offset_specification, int):
            self._filter_set[symbol(STREAM_OFFSET_SPEC)] = Described(
                symbol(STREAM_OFFSET_SPEC), offset_specification
            )
        else:
            self._filter_set[symbol(STREAM_OFFSET_SPEC)] = Described(
                symbol(STREAM_OFFSET_SPEC), offset_specification.name
            )

    def _filter_values(self, filters: list[str]) -> None:
        """
        Set the filter values for the stream.

        Args:
            filters: List of filter strings to apply to the stream
        """
        self._filter_set[symbol(STREAM_FILTER_SPEC)] = Described(
            symbol(STREAM_FILTER_SPEC), filters
        )

    def _filter_match_unfiltered(self, filter_match_unfiltered: bool) -> None:
        """
        Set whether to match unfiltered messages.

        Args:
            filter_match_unfiltered: Whether to match messages that don't match
                                   any filter
        """
        self._filter_set[symbol(STREAM_FILTER_MATCH_UNFILTERED)] = Described(
            symbol(STREAM_FILTER_MATCH_UNFILTERED), filter_match_unfiltered
        )

    def filter_set(self) -> Dict[symbol, Described]:
        """
        Get the current filter set configuration.

        Returns:
            Dict[symbol, Described]: The current filter set configuration
        """
        return self._filter_set


@dataclass
class RecoveryConfiguration:
    """
    Configuration options for automatic reconnection.

    This dataclass contains parameters to manage automatic reconnection

    Attributes:
    active_recovery: Define if the recovery is activated. If is not activated the connection will not try to reconnect
    back_off_reconnect_interval: the time to wait before trying to createSender after a connection is closed.
                time will be increased exponentially with each attempt.
                Default is 5 seconds, each attempt will double the time.
                The minimum value is 1 second. Avoid setting a value low values since it can cause a high
                number of reconnection attempts.
        MaxReconnectAttempts: 		MaxReconnectAttempts The maximum number of reconnection attempts.
                Default is 5.
                The minimum value is 1.
    """

    active_recovery: bool = True
    back_off_reconnect_interval: timedelta = timedelta(seconds=5)
    MaxReconnectAttempts: int = 5
