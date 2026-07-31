"""Auto-reconnection policy and the topology the recovery loop replays.

This module implements the configuration half of ``step_040_auto-reconnection.md``:
the :class:`RecoveryConfiguration` a caller puts on
:class:`~.connection.ConnectionParameters`, the :class:`BackOffDelayPolicy`
interface that paces the retry loop, and the :class:`RecordingTopologyListener`
that remembers every queue, exchange and binding declared through
:class:`~.management.Management`.

The recovery loop itself lives in :class:`~.connection.Connection` — it is the
only thing that owns a transport to redial.
"""

from __future__ import annotations

import random
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from .logging_utils import get_logger

if TYPE_CHECKING:
    from .management import ExchangeSpecification, Management, QueueSpecification

#: Attempts the default policy makes before it gives up (§2.1).
DEFAULT_MAX_ATTEMPTS = 12

#: Bounds of the default policy's per-attempt random delay, in seconds (§2.1).
DEFAULT_MIN_DELAY_SECONDS = 0.5
DEFAULT_MAX_DELAY_SECONDS = 1.5

#: How many attempts the default policy's delay multiplier grows over before it
#: falls back to 1, so a long outage keeps retrying at a bounded pace (§2.1).
MULTIPLIER_PERIOD = 5

_logger = get_logger("reconnection")


class BackOffDelayPolicy(Protocol):
    """Paces the recovery loop and decides when it gives up (§2.1).

    A caller may supply any object with this shape — a fixed delay, an unlimited
    policy whose :meth:`is_active` never returns ``False``, and so on. The loop
    calls :meth:`next_delay` once per attempt, waits that long, and only then
    consults :meth:`is_active`, so a policy is free to count attempts in either
    method.
    """

    @property
    def current_attempt(self) -> int:
        """Attempts made since the policy was created or last :meth:`reset`."""

    def next_delay(self) -> float:
        """Return how long to wait, in seconds, before the next attempt."""

    def reset(self) -> None:
        """Forget the attempts made so far, after a successful reconnect."""

    def is_active(self) -> bool:
        """Whether another attempt may be made, or the loop must give up."""


class DefaultBackOffDelayPolicy:
    """The randomized, attempt-aware default policy (§2.1).

    :meth:`next_delay` returns a random delay in
    ``[min_delay, max_delay)`` multiplied by the attempt number, and the
    multiplier falls back to 1 every :data:`MULTIPLIER_PERIOD` attempts so the
    wait grows in a bounded sawtooth rather than without limit. Safe to call
    from several threads, although the recovery loop only ever uses one.

    Example:
        >>> policy = DefaultBackOffDelayPolicy(max_attempts=3)
        >>> 0.5 <= policy.next_delay() < 1.5
        True
        >>> policy.current_attempt
        2
    """

    def __init__(
        self,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        min_delay: float = DEFAULT_MIN_DELAY_SECONDS,
        max_delay: float = DEFAULT_MAX_DELAY_SECONDS,
    ) -> None:
        """Create a policy that gives up after ``max_attempts`` attempts.

        Args:
            max_attempts: Attempt number at which :meth:`is_active` starts
                returning ``False``.
            min_delay: Lower bound of the random per-attempt delay, in seconds.
            max_delay: Upper bound, exclusive, of that delay.
        """
        self._max_attempts = max_attempts
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._lock = threading.Lock()
        self._attempt = 1

    @property
    def max_attempts(self) -> int:
        """Attempt number at which this policy stops being active."""
        return self._max_attempts

    @property
    def current_attempt(self) -> int:
        """Attempts made since creation or the last :meth:`reset`, counting from 1."""
        with self._lock:
            return self._attempt

    def next_delay(self) -> float:
        """Return this attempt's delay and count the attempt.

        Returns:
            A random delay in ``[min_delay, max_delay)`` seconds, multiplied by
            the attempt number modulo :data:`MULTIPLIER_PERIOD`.
        """
        with self._lock:
            multiplier = self._attempt % MULTIPLIER_PERIOD or MULTIPLIER_PERIOD
            self._attempt += 1
        jitter = random.random()  # noqa: S311 - jitter, not cryptography
        return (self._min_delay + jitter * (self._max_delay - self._min_delay)) * multiplier

    def reset(self) -> None:
        """Start counting attempts from 1 again."""
        with self._lock:
            self._attempt = 1

    def is_active(self) -> bool:
        """Whether :attr:`current_attempt` is still below :attr:`max_attempts`."""
        with self._lock:
            return self._attempt < self._max_attempts


@dataclass
class RecoveryConfiguration:
    """How a :class:`~.connection.Connection` reacts to losing its transport (§2).

    Read once when the connection is constructed and fixed for its lifetime.

    Attributes:
        activated: Master switch. ``False`` restores the pre-recovery behavior
            exactly — ``on_unexpected_close`` fires immediately and the
            connection is left dead. ``True`` redials and, on success,
            unconditionally re-attaches the management link pair and every
            tracked publisher's and consumer's link.
        back_off_delay_policy: Paces the retry loop and decides when to give up.
        topology: Whether a successful reconnect also redeclares the queues,
            exchanges and bindings recorded through ``Management``. ``False``
            leaves anything the broker dropped — an exclusive queue, say — gone.
    """

    activated: bool = True
    back_off_delay_policy: BackOffDelayPolicy = field(default_factory=DefaultBackOffDelayPolicy)
    topology: bool = False


@dataclass(frozen=True)
class RecordedQueue:
    """A queue declaration, as it was sent (§3.3 point 1).

    Attributes:
        name: Queue name, after any client-side generation.
        exclusive: Whether it was declared exclusive.
        auto_delete: Whether it was declared auto-delete.
        arguments: The ``x-*`` arguments it was declared with.
    """

    name: str
    exclusive: bool = False
    auto_delete: bool = False
    arguments: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecordedExchange:
    """An exchange declaration, as it was sent (§3.3 point 1).

    Attributes:
        name: Exchange name.
        exchange_type: Type it was declared with, e.g. ``"topic"``.
        auto_delete: Whether it was declared auto-delete.
        arguments: The arguments it was declared with.
    """

    name: str
    exchange_type: str = "direct"
    auto_delete: bool = False
    arguments: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecordedBinding:
    """A binding, as it was created (§3.3 point 1).

    Attributes:
        source: Source exchange name.
        destination: Destination queue or exchange name.
        binding_key: The binding/routing key.
        arguments: The binding arguments.
        to_queue: Whether ``destination`` names a queue rather than an exchange.
    """

    source: str
    destination: str
    binding_key: str = ""
    arguments: Mapping[str, Any] = field(default_factory=dict)
    to_queue: bool = True

    @property
    def key(self) -> tuple[str, str, str, bool, str]:
        """A hashable identity for this binding, arguments included."""
        return (
            self.source,
            self.destination,
            self.binding_key,
            self.to_queue,
            repr(sorted(self.arguments.items())),
        )


class RecordingTopologyListener:
    """Remembers the topology declared through ``Management`` (§3.3 point 1).

    Recording is unconditional and always on — it costs a few map entries per
    declare — while replaying it is gated behind
    :attr:`RecoveryConfiguration.topology`. What is kept is current state rather
    than a log: a deleted entity is forgotten, so :meth:`replay` never
    resurrects it, and deleting a queue or exchange also forgets the bindings
    that referenced it.

    Example:
        >>> listener = RecordingTopologyListener()
        >>> listener.record_queue_declared(management.queue("orders"))
        >>> listener.replay(management)
    """

    def __init__(self) -> None:
        """Create an empty recorder."""
        self._logger = _logger
        self._lock = threading.Lock()
        self._queues: dict[str, RecordedQueue] = {}
        self._exchanges: dict[str, RecordedExchange] = {}
        self._bindings: dict[tuple[str, str, str, bool, str], RecordedBinding] = {}

    # --- readers ---------------------------------------------------------

    @property
    def queues(self) -> tuple[RecordedQueue, ...]:
        """The queues currently recorded, in declaration order."""
        with self._lock:
            return tuple(self._queues.values())

    @property
    def exchanges(self) -> tuple[RecordedExchange, ...]:
        """The exchanges currently recorded, in declaration order."""
        with self._lock:
            return tuple(self._exchanges.values())

    @property
    def bindings(self) -> tuple[RecordedBinding, ...]:
        """The bindings currently recorded, in creation order."""
        with self._lock:
            return tuple(self._bindings.values())

    # --- recording -------------------------------------------------------

    def record_queue_declared(self, specification: QueueSpecification) -> None:
        """Record a queue that was just declared successfully.

        Args:
            specification: The builder that declared it, read *after* the
                declare so its generated name and normalised
                ``exclusive``/``auto_delete`` flags are the ones the broker saw.
        """
        recorded = RecordedQueue(
            name=specification.queue_name,
            exclusive=specification.is_exclusive,
            auto_delete=specification.is_auto_delete,
            arguments=specification.queue_arguments,
        )
        with self._lock:
            self._queues[recorded.name] = recorded

    def record_queue_deleted(self, name: str) -> None:
        """Forget a deleted queue, and every binding that pointed at it."""
        with self._lock:
            self._queues.pop(name, None)
            self._forget_bindings(lambda binding: binding.to_queue and binding.destination == name)

    def record_exchange_declared(self, specification: ExchangeSpecification) -> None:
        """Record an exchange that was just declared successfully.

        Args:
            specification: The builder that declared it.
        """
        recorded = RecordedExchange(
            name=specification.exchange_name,
            exchange_type=specification.exchange_type,
            auto_delete=specification.is_auto_delete,
            arguments=specification.exchange_arguments,
        )
        with self._lock:
            self._exchanges[recorded.name] = recorded

    def record_exchange_deleted(self, name: str) -> None:
        """Forget a deleted exchange, and every binding it was an end of."""
        with self._lock:
            self._exchanges.pop(name, None)
            self._forget_bindings(
                lambda binding: binding.source == name or (not binding.to_queue and binding.destination == name)
            )

    def record_binding_created(
        self,
        *,
        source: str,
        destination: str,
        binding_key: str = "",
        arguments: Mapping[str, Any] | None = None,
        to_queue: bool = True,
    ) -> None:
        """Record a binding that was just created successfully.

        Args:
            source: Source exchange name.
            destination: Destination queue or exchange name.
            binding_key: The binding/routing key.
            arguments: The binding arguments.
            to_queue: Whether ``destination`` names a queue.
        """
        recorded = RecordedBinding(
            source=source,
            destination=destination,
            binding_key=binding_key,
            arguments=dict(arguments or {}),
            to_queue=to_queue,
        )
        with self._lock:
            self._bindings[recorded.key] = recorded

    def record_binding_deleted(
        self,
        *,
        source: str,
        destination: str,
        binding_key: str = "",
        arguments: Mapping[str, Any] | None = None,
        to_queue: bool = True,
    ) -> None:
        """Forget a binding that was just removed successfully.

        Args:
            source: Source exchange name.
            destination: Destination queue or exchange name.
            binding_key: The binding/routing key.
            arguments: The arguments the binding was created with.
            to_queue: Whether ``destination`` names a queue.
        """
        recorded = RecordedBinding(
            source=source,
            destination=destination,
            binding_key=binding_key,
            arguments=dict(arguments or {}),
            to_queue=to_queue,
        )
        with self._lock:
            self._bindings.pop(recorded.key, None)

    # --- replay ----------------------------------------------------------

    def replay(self, management: Management) -> None:
        """Redeclare everything currently recorded (§3.3 point 3.2).

        Queues go first, then exchanges, then bindings, mirroring the dependency
        between them. One entity failing is logged and skipped so the rest are
        still replayed (§3.3 point 3.3) — a caller depending on it finds out the
        same way it would find out about any missing entity.

        Args:
            management: The freshly re-attached management endpoint to declare
                through.
        """
        queues, exchanges, bindings = self.queues, self.exchanges, self.bindings
        self._logger.debug(
            "replaying %d queue(s), %d exchange(s) and %d binding(s)", len(queues), len(exchanges), len(bindings)
        )
        for queue in queues:
            try:
                self._declare_queue(management, queue)
            except Exception as error:  # one entity must not abort the replay (§3.3 point 3.3)
                self._log_failure(f"queue {queue.name!r}", error)
        for exchange in exchanges:
            try:
                self._declare_exchange(management, exchange)
            except Exception as error:
                self._log_failure(f"exchange {exchange.name!r}", error)
        for binding in bindings:
            try:
                self._create_binding(management, binding)
            except Exception as error:
                self._log_failure(f"binding {binding.source!r} -> {binding.destination!r}", error)

    # --- internals -------------------------------------------------------

    def _log_failure(self, description: str, error: BaseException) -> None:
        """Report one entity that could not be recreated."""
        self._logger.warning("could not recreate %s during topology recovery: %s", description, error)

    @staticmethod
    def _declare_queue(management: Management, queue: RecordedQueue) -> None:
        """Redeclare one recorded queue through ``management``."""
        (
            management.queue(queue.name)
            .exclusive(queue.exclusive)
            .auto_delete(queue.auto_delete)
            .arguments(queue.arguments)
            .declare()
        )

    @staticmethod
    def _declare_exchange(management: Management, exchange: RecordedExchange) -> None:
        """Redeclare one recorded exchange through ``management``."""
        (
            management.exchange(exchange.name)
            .type(exchange.exchange_type)
            .auto_delete(exchange.auto_delete)
            .arguments(exchange.arguments)
            .declare()
        )

    @staticmethod
    def _create_binding(management: Management, binding: RecordedBinding) -> None:
        """Recreate one recorded binding through ``management``."""
        management.bind(
            source=binding.source,
            destination=binding.destination,
            binding_key=binding.binding_key,
            arguments=binding.arguments,
            to_queue=binding.to_queue,
        )

    def _forget_bindings(self, matches: Callable[[RecordedBinding], bool]) -> None:
        """Drop every recorded binding ``matches`` accepts; call with the lock held."""
        for key, binding in list(self._bindings.items()):
            if matches(binding):
                del self._bindings[key]


__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_DELAY_SECONDS",
    "DEFAULT_MIN_DELAY_SECONDS",
    "MULTIPLIER_PERIOD",
    "BackOffDelayPolicy",
    "DefaultBackOffDelayPolicy",
    "RecordedBinding",
    "RecordedExchange",
    "RecordedQueue",
    "RecordingTopologyListener",
    "RecoveryConfiguration",
]
