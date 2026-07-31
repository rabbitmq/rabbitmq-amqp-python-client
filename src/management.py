"""RabbitMQ topology management over AMQP 1.0 — queues, exchanges and bindings.

RabbitMQ exposes a pseudo-REST API on the well-known node address
``/management``. This module drives it exactly as described in
``step_001_management.md``: a dedicated session carries a **link pair** (one
sender and one receiver sharing the link name :data:`MANAGEMENT_LINK_NAME` and a
``{"paired": true}`` properties entry), and every operation is one request
message answered by one response message, correlated by
``Properties.message-id`` ↔ ``Properties.correlation-id``.

Three layers live here:

* :class:`Management` — the link pair, the correlation table, and the raw
  verb/path/body operations.
* :class:`QueueSpecification` (with its ``stream``/``quorum``/``classic``
  sub-builders) and :class:`ExchangeSpecification` — chainable builders that
  validate every ``x-*`` argument locally before a frame is ever sent.
* :class:`QueueInfo` — the parsed declare/get response.

Nothing here is HTTP: the verbs and paths are message properties, not a URL on a
separate connection.
"""

from __future__ import annotations

import base64
import hashlib
import threading
import urllib.parse
import uuid
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

from .constants import (
    MANAGEMENT_LINK_CREDIT,
    MANAGEMENT_LINK_NAME,
    MANAGEMENT_NODE_ADDRESS,
    MANAGEMENT_REPLY_TO,
)
from .exceptions import AMQPError, AMQPTimeoutError, ManagementError, ProtocolError, ValidationError
from .link import ReceiverLink, SenderLink
from .logging_utils import get_logger
from .wire import (
    EXPIRY_POLICY_LINK_DETACH,
    EXPIRY_POLICY_SESSION_END,
    RCV_SETTLE_MODE_FIRST,
    SND_SETTLE_MODE_SETTLED,
    Accepted,
    AmqpSequence,
    AmqpValue,
    Data,
    Long,
    Message,
    Properties,
    Source,
    Target,
)

if TYPE_CHECKING:
    from .connection import Connection
    from .link import Delivery
    from .reconnection import RecordingTopologyListener
    from .session import Session

# --- verbs (§3) ---------------------------------------------------------

VERB_PUT = "PUT"
VERB_GET = "GET"
VERB_POST = "POST"
VERB_DELETE = "DELETE"

# --- status codes (§4, §7) ----------------------------------------------

STATUS_OK = 200
STATUS_CREATED = 201
STATUS_NO_CONTENT = 204
STATUS_BAD_REQUEST = 400
STATUS_NOT_FOUND = 404
STATUS_CONFLICT = 409

#: Codes that always raise, whatever the operation listed as acceptable (§4.2).
ALWAYS_ERROR_CODES: dict[int, str] = {
    STATUS_BAD_REQUEST: "bad request",
    STATUS_NOT_FOUND: "not found",
    STATUS_CONFLICT: "precondition failed",
}

# Expected-code sets, one per row of §8's endpoint summary. ``409`` appears in
# the two declare sets to match the spec tables even though §4.2 short-circuits
# it into a precondition-failed error before the set is ever consulted.
EXPECTED_DECLARE_QUEUE = frozenset({STATUS_OK, STATUS_CREATED, STATUS_CONFLICT})
EXPECTED_QUEUE_INFO = frozenset({STATUS_OK})
EXPECTED_PURGE_QUEUE = frozenset({STATUS_OK})
EXPECTED_DELETE_QUEUE = frozenset({STATUS_OK})
EXPECTED_DECLARE_EXCHANGE = frozenset({STATUS_CREATED, STATUS_NO_CONTENT, STATUS_CONFLICT})
EXPECTED_DELETE_EXCHANGE = frozenset({STATUS_NO_CONTENT})
EXPECTED_BIND = frozenset({STATUS_NO_CONTENT})
EXPECTED_LIST_BINDINGS = frozenset({STATUS_OK})
EXPECTED_UNBIND = frozenset({STATUS_NO_CONTENT})

# --- timing -------------------------------------------------------------

DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0

#: How long the response pump blocks per poll before re-checking for shutdown.
RESPONSE_POLL_INTERVAL_SECONDS = 0.2

PUMP_JOIN_TIMEOUT_SECONDS = 5.0

# --- request/response shape --------------------------------------------

#: ``attach.properties`` that makes the broker treat the two links as one pair.
PAIRED_LINK_PROPERTIES: dict[str, Any] = {"paired": True}

#: Prefix of a client-generated queue name (§5.1 Declare).
GENERATED_NAME_PREFIX = "client.gen-"

# --- queue argument names (§5.4) ---------------------------------------

ARG_QUEUE_TYPE = "x-queue-type"
ARG_DEAD_LETTER_EXCHANGE = "x-dead-letter-exchange"
ARG_DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key"
ARG_OVERFLOW = "x-overflow"
ARG_MAX_LENGTH_BYTES = "x-max-length-bytes"
ARG_MAX_LENGTH = "x-max-length"
ARG_MESSAGE_TTL = "x-message-ttl"
ARG_EXPIRES = "x-expires"
ARG_QUEUE_LEADER_LOCATOR = "x-queue-leader-locator"
ARG_SINGLE_ACTIVE_CONSUMER = "x-single-active-consumer"
ARG_MAX_AGE = "x-max-age"
ARG_STREAM_MAX_SEGMENT_SIZE_BYTES = "x-stream-max-segment-size-bytes"
ARG_INITIAL_CLUSTER_SIZE = "x-initial-cluster-size"
ARG_STREAM_FILE_SIZE_PER_CHUNK = "x-stream-file-size-per-chunk"
ARG_DEAD_LETTER_STRATEGY = "x-dead-letter-strategy"
ARG_DELIVERY_LIMIT = "x-delivery-limit"
ARG_QUORUM_INITIAL_GROUP_SIZE = "x-quorum-initial-group-size"
ARG_QUORUM_TARGET_GROUP_SIZE = "x-quorum-target-group-size"
ARG_DELAYED_RETRY_TYPE = "x-delayed-retry-type"
ARG_DELAYED_RETRY_MIN = "x-delayed-retry-min"
ARG_DELAYED_RETRY_MAX = "x-delayed-retry-max"
ARG_MAX_PRIORITY = "x-max-priority"
ARG_QUEUE_MODE = "x-queue-mode"
ARG_QUEUE_VERSION = "x-queue-version"

#: Upper bound the broker accepts for ``x-message-ttl``/``x-expires`` (§5.4).
TEN_YEARS_MS = 10 * 365 * 24 * 60 * 60 * 1000

MIN_MAX_PRIORITY = 1
MAX_MAX_PRIORITY = 255

_logger = get_logger("management")


class QueueType(Enum):
    """Value of ``x-queue-type``."""

    QUORUM = "quorum"
    CLASSIC = "classic"
    STREAM = "stream"


class OverflowStrategy(Enum):
    """Value of ``x-overflow``."""

    DROP_HEAD = "drop-head"
    REJECT_PUBLISH = "reject-publish"
    REJECT_PUBLISH_DLX = "reject-publish-dlx"


class LeaderLocatorStrategy(Enum):
    """Value of ``x-queue-leader-locator``."""

    CLIENT_LOCAL = "client-local"
    BALANCED = "balanced"


class QuorumQueueDeadLetterStrategy(Enum):
    """Value of ``x-dead-letter-strategy``; quorum queues only."""

    AT_MOST_ONCE = "at-most-once"
    AT_LEAST_ONCE = "at-least-once"


class QuorumQueueDelayedRetryType(Enum):
    """Value of ``x-delayed-retry-type``; quorum queues only, RabbitMQ 4.3+."""

    DISABLED = "disabled"
    ALL = "all"
    FAILED = "failed"
    RETURNED = "returned"


class ClassicQueueMode(Enum):
    """Value of ``x-queue-mode``; classic queues only."""

    DEFAULT = "default"
    LAZY = "lazy"


class ClassicQueueVersion(Enum):
    """Value of ``x-queue-version``; classic queues only, sent as ``1``/``2``."""

    V1 = 1
    V2 = 2


class ExchangeType(Enum):
    """Built-in exchange types; a plugin type is passed to ``type()`` as a string."""

    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"


# --- encoding helpers (§6) ---------------------------------------------


def encode_path_segment(value: str) -> str:
    """Percent-encode ``value`` for use inside a management path.

    Every byte of the UTF-8 representation outside the unreserved set
    ``[A-Za-z0-9\\-._~]`` becomes ``%`` plus two uppercase hex digits, so a space
    becomes ``%20`` — this is *not* form encoding.

    Args:
        value: The raw segment, e.g. a queue name.

    Returns:
        The percent-encoded segment.
    """
    return urllib.parse.quote(value, safe="")


def encode_query_value(value: str) -> str:
    """Form-encode ``value`` for use in a management query string.

    Standard ``application/x-www-form-urlencoded`` rules apply, so a space
    becomes ``+``. Mixing this up with :func:`encode_path_segment` yields a path
    the broker cannot resolve, usually surfacing as an unexpected ``404``.

    Args:
        value: The raw query value.

    Returns:
        The form-encoded value.
    """
    return urllib.parse.quote_plus(value)


def queue_path(name: str) -> str:
    """Return ``/queues/{name}`` with ``name`` percent-encoded."""
    return f"/queues/{encode_path_segment(name)}"


def queue_messages_path(name: str) -> str:
    """Return ``/queues/{name}/messages``, the purge path."""
    return f"/queues/{encode_path_segment(name)}/messages"


def exchange_path(name: str) -> str:
    """Return ``/exchanges/{name}`` with ``name`` percent-encoded."""
    return f"/exchanges/{encode_path_segment(name)}"


#: Collection path every ``POST`` binding request is sent to.
BINDINGS_PATH = "/bindings"


def _destination_key(to_queue: bool) -> str:
    """Return the binding path/query key for a queue or exchange destination."""
    return "dstq" if to_queue else "dste"


def unbind_path(source: str, destination: str, binding_key: str, *, to_queue: bool) -> str:
    """Return the ``DELETE`` path that removes a binding with no arguments.

    The trailing ``args=`` is always present and always empty; a binding *with*
    arguments cannot be addressed this way and needs the list-then-delete dance
    in :meth:`Management.unbind`.

    Args:
        source: Source exchange name.
        destination: Destination queue or exchange name.
        binding_key: The binding key.
        to_queue: Whether the destination is a queue rather than an exchange.

    Returns:
        The semicolon-delimited, percent-encoded binding path.
    """
    return (
        f"{BINDINGS_PATH}/src={encode_path_segment(source)}"
        f";{_destination_key(to_queue)}={encode_path_segment(destination)}"
        f";key={encode_path_segment(binding_key)};args="
    )


def bindings_query_path(source: str, destination: str, binding_key: str, *, to_queue: bool) -> str:
    """Return the ``GET /bindings?...`` path that lists matching bindings.

    Args:
        source: Source exchange name.
        destination: Destination queue or exchange name.
        binding_key: The binding key.
        to_queue: Whether the destination is a queue rather than an exchange.

    Returns:
        The path with form-encoded query values.
    """
    return (
        f"{BINDINGS_PATH}?src={encode_query_value(source)}"
        f"&{_destination_key(to_queue)}={encode_query_value(destination)}"
        f"&key={encode_query_value(binding_key)}"
    )


def generate_queue_name() -> str:
    """Return a fresh client-generated queue name.

    Names take the ``client.gen-`` + unpadded base64url(md5(uuid4)) form §5.1
    suggests, so a queue declared without a name can still be tracked and
    deleted by the client that created it.
    """
    digest = hashlib.md5(uuid.uuid4().bytes, usedforsecurity=False).digest()
    return GENERATED_NAME_PREFIX + base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


# --- local validation (§5.4) -------------------------------------------


def _require_positive(name: str, value: int) -> int:
    """Return ``value``, refusing anything that is not strictly positive."""
    if value <= 0:
        raise ValidationError(f"{name} must be > 0, got {value}")
    return value


def _require_within(name: str, value: int, low: int, high: int) -> int:
    """Return ``value``, refusing anything outside ``low..high`` inclusive."""
    if not low <= value <= high:
        raise ValidationError(f"{name} must be in {low}..{high}, got {value}")
    return value


def _milliseconds(value: int | timedelta) -> int:
    """Normalise a duration given as milliseconds or a :class:`~datetime.timedelta`."""
    if isinstance(value, timedelta):
        return int(value.total_seconds() * 1000)
    return int(value)


def _seconds(value: int | timedelta) -> int:
    """Normalise a duration given as seconds or a :class:`~datetime.timedelta`."""
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    return int(value)


# --- results -----------------------------------------------------------


@dataclass(frozen=True)
class QueueInfo:
    """The state of one queue, as reported by declare or get (§5.1).

    Attributes:
        name: Queue name.
        durable: Whether the queue survives a broker restart; always ``True``,
            since this client never declares a transient queue.
        auto_delete: Whether the broker deletes the queue once unused.
        exclusive: Whether the queue is bound to its declaring connection.
        queue_type: Which queue implementation backs it.
        arguments: The ``x-*`` arguments the broker reports.
        leader: Node hosting the queue leader, when the broker reports one.
        replicas: Nodes hosting replicas, when the broker reports them.
        message_count: Messages ready in the queue.
        consumer_count: Consumers attached to the queue.
    """

    name: str
    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False
    queue_type: QueueType = QueueType.CLASSIC
    arguments: dict[str, Any] = field(default_factory=dict)
    leader: str = ""
    replicas: tuple[str, ...] = ()
    message_count: int = 0
    consumer_count: int = 0

    @classmethod
    def from_body(cls, body: Mapping[str, Any]) -> QueueInfo:
        """Build queue info from a decoded declare/get response body.

        Args:
            body: The response map.

        Returns:
            The parsed queue info.

        Raises:
            ProtocolError: If the broker reports a queue type this client does
                not know.
        """
        raw_type = body.get("type")
        try:
            queue_type = QueueType.CLASSIC if raw_type is None else QueueType(str(raw_type))
        except ValueError as error:
            raise ProtocolError(f"the broker reported unknown queue type {raw_type!r}") from error
        return cls(
            name=str(body.get("name", "")),
            durable=bool(body.get("durable", True)),
            auto_delete=bool(body.get("auto_delete", False)),
            exclusive=bool(body.get("exclusive", False)),
            queue_type=queue_type,
            arguments={str(key): value for key, value in (body.get("arguments") or {}).items()},
            leader=str(body.get("leader") or ""),
            replicas=tuple(str(item) for item in (body.get("replicas") or ())),
            message_count=int(body.get("message_count") or 0),
            consumer_count=int(body.get("consumer_count") or 0),
        )


# --- request/response plumbing (§3, §4) --------------------------------


def build_request(message_id: str, verb: str, path: str, body: Any) -> Message:
    """Build the request message for one management operation.

    An ``amqp-value`` section is always present, holding ``null`` for the
    operations that carry no payload: RabbitMQ matches the decoded body against
    ``null`` and answers an ``amqp:internal-error`` when the section is missing
    altogether, so "no body" means "a body encoding ``null``", not "no section".

    Args:
        message_id: Correlation id, unique among in-flight requests.
        verb: ``PUT``, ``GET``, ``POST`` or ``DELETE``.
        path: Resource path, already encoded per §6.
        body: Payload for the ``amqp-value`` body; ``None`` encodes as ``null``.

    Returns:
        The message to send on the sender half of the link pair.
    """
    return Message(
        body=AmqpValue(body),
        properties=Properties(
            message_id=message_id,
            to=path,
            subject=verb,
            reply_to=MANAGEMENT_REPLY_TO,
        ),
    )


def validate_response(message_id: str, response: Message, expected_codes: Collection[int]) -> Any:
    """Apply §4's five checks, in order, and return the decoded response body.

    The order is load-bearing: a ``400``/``404``/``409`` is reported as its own
    condition even when the operation lists that code as acceptable, and a
    correlation mismatch is reported before the code is compared against the
    expected set, so a body that belongs to another request is never trusted.

    Args:
        message_id: ``message-id`` of the request this response answers.
        response: The response message.
        expected_codes: Codes this operation accepts.

    Returns:
        The decoded body: a ``dict``/``list`` for the operations that carry one,
        ``None`` for the ones that do not.

    Raises:
        ProtocolError: If ``Subject`` is missing or not an integer, if
            ``CorrelationId`` does not echo ``message_id``, or if the body is
            not a shape the management API uses.
        ManagementError: If the code is ``400``/``404``/``409``, or is outside
            ``expected_codes``.
    """
    properties = response.properties
    code = _status_code(properties)
    condition = ALWAYS_ERROR_CODES.get(code)
    if condition is not None:
        raise ManagementError(f"the broker answered {code} ({condition})", status_code=code)
    correlation_id = None if properties is None else properties.correlation_id
    if _id_key(correlation_id) != message_id:
        raise ProtocolError(
            f"management response correlation-id {correlation_id!r} does not match request message-id {message_id!r}"
        )
    if code not in expected_codes:
        expected = ", ".join(str(item) for item in sorted(expected_codes))
        raise ManagementError(f"the broker answered {code}, expected one of {expected}", status_code=code)
    return _response_body(response)


def _status_code(properties: Properties | None) -> int:
    """Parse the response ``Subject`` as a decimal status code.

    Raises:
        ProtocolError: If it is missing or not an integer.
    """
    subject = None if properties is None else properties.subject
    if subject is None:
        raise ProtocolError("management response carries no subject, so it has no status code")
    try:
        return int(subject)
    except ValueError as error:
        raise ProtocolError(f"management response subject {subject!r} is not an integer status code") from error


def _response_body(response: Message) -> Any:
    """Extract the payload of a response body, tolerating a body-less response.

    Raises:
        ProtocolError: If the body is a shape the management API never uses.
    """
    body = response.body
    if body is None:
        return None
    if isinstance(body, AmqpValue):
        return body.value
    if isinstance(body, AmqpSequence):
        return body.value
    if isinstance(body, Data) and not body.value:
        return None
    raise ProtocolError(f"management response body has unexpected shape {type(body).__name__}")


def _id_key(value: Any) -> str | None:
    """Normalise a ``message-id``/``correlation-id`` to the correlation-table key."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _as_map(value: Any, context: str) -> dict[str, Any]:
    """Coerce a decoded response body to a string-keyed map.

    Raises:
        ProtocolError: If the body is not a map.
    """
    if not isinstance(value, Mapping):
        raise ProtocolError(f"expected a map in the {context} response, got {type(value).__name__}")
    return {str(key): item for key, item in value.items()}


def _as_maps(value: Any, context: str) -> list[dict[str, Any]]:
    """Coerce a decoded response body to a list of string-keyed maps.

    Raises:
        ProtocolError: If the body is not a list of maps.
    """
    if not isinstance(value, list):
        raise ProtocolError(f"expected a list in the {context} response, got {type(value).__name__}")
    return [_as_map(item, context) for item in value]


def _normalized_arguments(value: Any) -> dict[str, Any]:
    """Return a string-keyed copy of an arguments map, treating ``None`` as empty."""
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _binding_location(
    entries: Iterable[Mapping[str, Any]], binding_key: str, arguments: Mapping[str, Any]
) -> str | None:
    """Find the opaque ``location`` of the listed binding matching key and arguments.

    Args:
        entries: Binding maps returned by ``GET /bindings?...``.
        binding_key: The binding key to match exactly.
        arguments: The arguments map to match by equality.

    Returns:
        The broker-assigned location path, or ``None`` when nothing matches.
    """
    wanted = _normalized_arguments(arguments)
    for entry in entries:
        if str(entry.get("binding_key") or "") != binding_key:
            continue
        if _normalized_arguments(entry.get("arguments")) != wanted:
            continue
        location = entry.get("location")
        if location is not None:
            return str(location)
    return None


class _PendingRequest:
    """One in-flight request, waiting for the response that echoes its id."""

    def __init__(self, message_id: str) -> None:
        self.message_id = message_id
        self._resolved = threading.Event()
        self._response: Message | None = None
        self._failure: BaseException | None = None

    def complete(self, response: Message) -> None:
        """Record the response and wake the waiting caller."""
        self._response = response
        self._resolved.set()

    def fail(self, error: BaseException) -> None:
        """Record that no response will ever arrive and wake the waiting caller."""
        self._failure = error
        self._resolved.set()

    def wait(self, timeout: float) -> Message:
        """Block until the response arrives.

        Raises:
            AMQPTimeoutError: If ``timeout`` elapses first.
            AMQPError: Whatever failure made the response unreachable.
        """
        if not self._resolved.wait(timeout):
            raise AMQPTimeoutError(f"no management response for request {self.message_id!r} within {timeout:g}s")
        if self._failure is not None:
            raise self._failure
        if self._response is None:  # pragma: no cover - complete()/fail() always set one
            raise ProtocolError(f"management request {self.message_id!r} resolved without a response")
        return self._response


def _management_source() -> Source:
    """Build the ``source`` terminus both halves of the link pair use (§2)."""
    return Source(
        address=MANAGEMENT_NODE_ADDRESS,
        expiry_policy=EXPIRY_POLICY_LINK_DETACH,
        timeout=0,
        dynamic=False,
    )


def _management_target() -> Target:
    """Build the ``target`` terminus both halves of the link pair use (§2)."""
    return Target(
        address=MANAGEMENT_NODE_ADDRESS,
        expiry_policy=EXPIRY_POLICY_SESSION_END,
        timeout=0,
        dynamic=False,
    )


class Management:
    """The management link pair and the operations that ride on it.

    Obtain one from :meth:`~.connection.Connection.management`, which keeps a
    single instance per connection and closes it during connection teardown.
    Requests from several threads may be in flight at once: each carries a
    unique ``message-id`` and is resolved from the correlation table by the
    background pump that drains the receiver half.

    Example:
        >>> management = connection.management()
        >>> info = management.queue("orders").quorum().delivery_limit(5).queue().declare()
        >>> management.exchange("events").type(ExchangeType.TOPIC).declare()
        >>> management.bind(source="events", destination=info.name, binding_key="orders.#")
    """

    def __init__(
        self,
        connection: Connection,
        *,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        link_credit: int = MANAGEMENT_LINK_CREDIT,
        topology_listener: RecordingTopologyListener | None = None,
    ) -> None:
        """Create a closed management endpoint; call :meth:`open` to use it.

        Args:
            connection: Connection to open the management session on.
            request_timeout: Seconds a request waits for its response.
            link_credit: Credit granted on the receiver half, so the broker can
                push responses without a per-call ``flow``.
            topology_listener: Told about every successful declare, delete, bind
                and unbind, so auto-reconnection can replay them
                (step_040 §3.3 point 1). Recording is always on when one is
                given; only replaying it is opt-in.
        """
        self._connection = connection
        self._request_timeout = request_timeout
        self._link_credit = max(1, link_credit)
        self._topology_listener = topology_listener
        self._logger = _logger
        self._lock = threading.Lock()
        self._pending: dict[str, _PendingRequest] = {}
        self._session: Session | None = None
        self._sender: SenderLink | None = None
        self._receiver: ReceiverLink | None = None
        self._pump: threading.Thread | None = None
        self._stopping = threading.Event()
        self._opened = False

    # --- lifecycle ------------------------------------------------------

    @property
    def is_open(self) -> bool:
        """Whether the link pair is attached and usable."""
        with self._lock:
            return self._opened

    @property
    def request_timeout(self) -> float:
        """Seconds a request waits for its response."""
        return self._request_timeout

    def open(self) -> None:
        """Begin the management session and attach the link pair.

        Both links share :data:`~.constants.MANAGEMENT_LINK_NAME` and carry
        ``{"paired": true}``; the sender is attached first so it takes handle
        ``0`` and the receiver handle ``1``. Idempotent.

        Raises:
            ProtocolError: If the connection is not open, or the broker refuses
                either half of the pair.
            AMQPTimeoutError: If the broker does not answer ``begin``/``attach``.
        """
        with self._lock:
            if self._opened:
                return
        session = self._connection.open_session()
        sender = SenderLink(MANAGEMENT_LINK_NAME)
        receiver = ReceiverLink(MANAGEMENT_LINK_NAME)
        try:
            for link in (sender, receiver):
                link.attach(
                    session,
                    source=_management_source(),
                    target=_management_target(),
                    snd_settle_mode=SND_SETTLE_MODE_SETTLED,
                    rcv_settle_mode=RCV_SETTLE_MODE_FIRST,
                    properties=dict(PAIRED_LINK_PROPERTIES),
                )
            receiver.flow(self._link_credit)
        except BaseException:
            session.end()
            raise
        pump = threading.Thread(
            target=self._pump_responses,
            args=(receiver,),
            name="amqp-management-responses",
            daemon=True,
        )
        with self._lock:
            self._session = session
            self._sender = sender
            self._receiver = receiver
            self._pump = pump
            self._stopping.clear()
            self._opened = True
        pump.start()
        self._logger.debug("management link pair attached on channel %s", session.channel)

    def _reopen(self) -> None:
        """Re-attach this same endpoint on the connection's new transport.

        Called by the recovery loop (step_040 §3.3 point 2.1) after a successful
        reconnect: the caller still holds this instance, so the session and link
        pair are replaced underneath it rather than a new endpoint being built.
        Whatever the dead transport left behind is discarded first — the old
        session and links are unusable, and no ``detach``/``end`` can reach the
        broker over a socket that is already gone.

        Raises:
            ProtocolError: If the connection refuses the new session, or the
                broker refuses either half of the pair.
            AMQPTimeoutError: If the broker does not answer ``begin``/``attach``.
        """
        with self._lock:
            self._opened = False
            self._session = self._sender = self._receiver = None
            pump, self._pump = self._pump, None
        self._stopping.set()
        if pump is not None and pump is not threading.current_thread():
            pump.join(PUMP_JOIN_TIMEOUT_SECONDS)
        self._fail_pending(ManagementError("the connection was lost before this request was answered"))
        self.open()

    def close(self) -> None:
        """Detach the link pair, end its session and fail every pending request.

        Idempotent, and never raises: teardown of a half-dead link pair must
        always complete.
        """
        with self._lock:
            if not self._opened:
                return
            self._opened = False
            session, sender, receiver, pump = self._session, self._sender, self._receiver, self._pump
            self._session = self._sender = self._receiver = self._pump = None
        self._stopping.set()
        self._fail_pending(ManagementError("the management link pair was closed"))
        for link in (sender, receiver):
            if link is not None:
                link.detach()
        if session is not None:
            session.end()
        if pump is not None and pump is not threading.current_thread():
            pump.join(PUMP_JOIN_TIMEOUT_SECONDS)
        self._logger.debug("management link pair closed")

    # --- builders -------------------------------------------------------

    def queue(self, name: str = "") -> QueueSpecification:
        """Start building a queue declaration.

        Args:
            name: Queue name; when left empty, :meth:`QueueSpecification.declare`
                generates one before sending.

        Returns:
            The chainable builder.
        """
        return QueueSpecification(self, name)

    def exchange(self, name: str = "") -> ExchangeSpecification:
        """Start building an exchange declaration.

        Args:
            name: Exchange name; must be non-empty before declaring or deleting.

        Returns:
            The chainable builder.
        """
        return ExchangeSpecification(self, name)

    # --- queue operations (§5.1) ----------------------------------------

    def queue_info(self, name: str) -> QueueInfo:
        """Read a queue's current state with ``GET /queues/{name}``.

        Args:
            name: Queue name.

        Returns:
            The queue's state.

        Raises:
            ValidationError: If ``name`` is empty.
            ManagementError: If the queue does not exist (``404``) or the broker
                answers an unexpected code.
        """
        _require_name("queue", name)
        body = self._request(VERB_GET, queue_path(name), None, EXPECTED_QUEUE_INFO)
        return QueueInfo.from_body(_as_map(body, "queue info"))

    def _declare_queue(self, specification: QueueSpecification) -> QueueInfo:
        """Issue ``PUT /queues/{name}`` for ``specification`` and parse the reply."""
        name = specification.queue_name
        _require_name("queue", name)
        body = specification.declare_body()
        response = self._request(VERB_PUT, queue_path(name), body, EXPECTED_DECLARE_QUEUE)
        if self._topology_listener is not None:
            self._topology_listener.record_queue_declared(specification)
        return QueueInfo.from_body(_as_map(response, "queue declare"))

    def _purge_queue(self, name: str) -> int:
        """Issue ``DELETE /queues/{name}/messages`` and return the purged count."""
        _require_name("queue", name)
        body = self._request(VERB_DELETE, queue_messages_path(name), None, EXPECTED_PURGE_QUEUE)
        return int(_as_map(body, "queue purge").get("message_count") or 0)

    def _delete_queue(self, name: str) -> QueueInfo:
        """Issue ``DELETE /queues/{name}`` and return a name-only stub."""
        _require_name("queue", name)
        self._request(VERB_DELETE, queue_path(name), None, EXPECTED_DELETE_QUEUE)
        if self._topology_listener is not None:
            self._topology_listener.record_queue_deleted(name)
        return QueueInfo(name=name)

    # --- exchange operations (§5.2) -------------------------------------

    def _declare_exchange(self, specification: ExchangeSpecification) -> None:
        """Issue ``PUT /exchanges/{name}`` for ``specification``."""
        name = specification.exchange_name
        _require_name("exchange", name)
        self._request(VERB_PUT, exchange_path(name), specification.declare_body(), EXPECTED_DECLARE_EXCHANGE)
        if self._topology_listener is not None:
            self._topology_listener.record_exchange_declared(specification)

    def _delete_exchange(self, name: str) -> None:
        """Issue ``DELETE /exchanges/{name}``."""
        _require_name("exchange", name)
        self._request(VERB_DELETE, exchange_path(name), None, EXPECTED_DELETE_EXCHANGE)
        if self._topology_listener is not None:
            self._topology_listener.record_exchange_deleted(name)

    # --- binding operations (§5.3) --------------------------------------

    def bind(
        self,
        *,
        source: str,
        destination: str,
        binding_key: str = "",
        arguments: Mapping[str, Any] | None = None,
        to_queue: bool = True,
    ) -> None:
        """Bind ``source`` to ``destination`` with ``POST /bindings``.

        Args:
            source: Source exchange name.
            destination: Destination queue or exchange name.
            binding_key: Routing/binding key.
            arguments: Binding arguments, e.g. a headers-exchange match set.
            to_queue: Whether ``destination`` names a queue rather than an
                exchange; exactly one of ``destination_queue`` /
                ``destination_exchange`` goes on the wire.

        Raises:
            ValidationError: If ``source`` or ``destination`` is empty.
            ManagementError: If the broker rejects the binding.
        """
        _require_name("binding source", source)
        _require_name("binding destination", destination)
        body: dict[str, Any] = {
            "source": source,
            "binding_key": binding_key,
            "arguments": _normalized_arguments(arguments),
        }
        body["destination_queue" if to_queue else "destination_exchange"] = destination
        self._request(VERB_POST, BINDINGS_PATH, body, EXPECTED_BIND)
        if self._topology_listener is not None:
            self._topology_listener.record_binding_created(
                source=source,
                destination=destination,
                binding_key=binding_key,
                arguments=arguments,
                to_queue=to_queue,
            )

    def list_bindings(
        self,
        *,
        source: str,
        destination: str,
        binding_key: str = "",
        to_queue: bool = True,
    ) -> list[dict[str, Any]]:
        """List the bindings matching source, destination and key.

        Args:
            source: Source exchange name.
            destination: Destination queue or exchange name.
            binding_key: Binding key to filter on.
            to_queue: Whether ``destination`` names a queue.

        Returns:
            One map per matching binding, each carrying at least
            ``binding_key``, ``arguments`` and the opaque ``location`` that
            :meth:`unbind` needs.

        Raises:
            ValidationError: If ``source`` or ``destination`` is empty.
        """
        _require_name("binding source", source)
        _require_name("binding destination", destination)
        path = bindings_query_path(source, destination, binding_key, to_queue=to_queue)
        body = self._request(VERB_GET, path, None, EXPECTED_LIST_BINDINGS)
        return _as_maps(body or [], "list bindings")

    def unbind(
        self,
        *,
        source: str,
        destination: str,
        binding_key: str = "",
        arguments: Mapping[str, Any] | None = None,
        to_queue: bool = True,
    ) -> bool:
        """Remove a binding.

        A binding without arguments is fully identified by source, destination
        and key, so it is deleted directly. A binding *with* arguments carries a
        broker-assigned ``location`` that cannot be reconstructed client-side,
        so it is listed first and the matching entry's ``location`` is deleted.

        Args:
            source: Source exchange name.
            destination: Destination queue or exchange name.
            binding_key: Binding key.
            arguments: The arguments the binding was created with.
            to_queue: Whether ``destination`` names a queue.

        Returns:
            Whether a binding was deleted. The no-arguments path always reports
            ``True``, because the broker answers ``204`` whether or not the
            binding existed; the with-arguments path reports ``False`` when no
            listed binding matched, which is a no-op rather than an error.

        Raises:
            ValidationError: If ``source`` or ``destination`` is empty.
        """
        wanted = _normalized_arguments(arguments)
        if not wanted:
            _require_name("binding source", source)
            _require_name("binding destination", destination)
            path = unbind_path(source, destination, binding_key, to_queue=to_queue)
            self._request(VERB_DELETE, path, None, EXPECTED_UNBIND)
            self._record_binding_deleted(source, destination, binding_key, wanted, to_queue)
            return True
        entries = self.list_bindings(
            source=source,
            destination=destination,
            binding_key=binding_key,
            to_queue=to_queue,
        )
        location = _binding_location(entries, binding_key, wanted)
        if location is None:
            return False
        self._request(VERB_DELETE, location, None, EXPECTED_UNBIND)
        self._record_binding_deleted(source, destination, binding_key, wanted, to_queue)
        return True

    def _record_binding_deleted(
        self,
        source: str,
        destination: str,
        binding_key: str,
        arguments: Mapping[str, Any],
        to_queue: bool,
    ) -> None:
        """Tell the topology listener, if any, that a binding is gone."""
        if self._topology_listener is None:
            return
        self._topology_listener.record_binding_deleted(
            source=source,
            destination=destination,
            binding_key=binding_key,
            arguments=arguments,
            to_queue=to_queue,
        )

    # --- request/response engine (§3, §4) -------------------------------

    def _request(self, verb: str, path: str, body: Any, expected_codes: Collection[int]) -> Any:
        """Send one request, wait for its response, and validate it.

        Args:
            verb: ``PUT``, ``GET``, ``POST`` or ``DELETE``.
            path: Resource path, already encoded per §6.
            body: Payload for the ``amqp-value`` body, or ``None``.
            expected_codes: Status codes this operation accepts.

        Returns:
            The decoded response body.

        Raises:
            ManagementError: If the link pair is closed, or the broker answers
                an error/unexpected code.
            AMQPTimeoutError: If no response arrives within ``request_timeout``.
            ProtocolError: If the response is malformed or mismatched.
        """
        sender = self._require_open()
        message_id = uuid.uuid4().hex
        pending = _PendingRequest(message_id)
        with self._lock:
            self._pending[message_id] = pending
        try:
            sender.send_transfer(message_id.encode("ascii"), build_request(message_id, verb, path, body), settled=True)
            response = pending.wait(self._request_timeout)
        finally:
            with self._lock:
                self._pending.pop(message_id, None)
        return validate_response(message_id, response, expected_codes)

    def _require_open(self) -> SenderLink:
        """Return the sender half, refusing a closed link pair.

        Raises:
            ManagementError: If the link pair is not open.
        """
        with self._lock:
            if not self._opened or self._sender is None:
                raise ManagementError("the management link pair is not open")
            return self._sender

    def _pump_responses(self, receiver: ReceiverLink) -> None:
        """Drain responses off the receiver half until the pair is closed.

        Runs on its own thread because :meth:`~.link.ReceiverLink.receive`
        blocks; a failure that kills the link is propagated to every caller
        currently waiting on a response.
        """
        while not self._stopping.is_set():
            try:
                delivery = receiver.receive(timeout=RESPONSE_POLL_INTERVAL_SECONDS)
            except AMQPError as error:
                if not self._stopping.is_set():
                    self._logger.warning("the management receiver link failed: %s", error)
                self._fail_pending(error)
                return
            if delivery is None:
                continue
            self._settle(receiver, delivery)
            self._on_response(delivery.message)

    def _settle(self, receiver: ReceiverLink, delivery: Delivery) -> None:
        """Accept a response the broker did not pre-settle, then top up credit."""
        if not delivery.settled:
            try:
                receiver.settle(delivery.delivery_id, Accepted())
            except AMQPError as error:
                self._logger.debug("could not accept a management response: %s", error)
        if receiver.credit > self._link_credit // 2:
            return
        try:
            receiver.flow(self._link_credit)
        except AMQPError as error:
            self._logger.debug("could not replenish management link credit: %s", error)

    def _on_response(self, response: Message) -> None:
        """Hand one response to the request whose ``message-id`` it echoes."""
        properties = response.properties
        key = _id_key(None if properties is None else properties.correlation_id)
        pending = None
        if key is not None:
            with self._lock:
                pending = self._pending.pop(key, None)
        if pending is None:
            self._logger.warning("dropping a management response with unmatched correlation-id %r", key)
            return
        pending.complete(response)

    def _fail_pending(self, error: BaseException) -> None:
        """Fail every request still waiting for a response."""
        with self._lock:
            waiting = list(self._pending.values())
            self._pending.clear()
        for pending in waiting:
            pending.fail(error)


def _require_name(kind: str, name: str) -> str:
    """Return ``name``, refusing an empty one.

    Raises:
        ValidationError: If ``name`` is empty.
    """
    if not name:
        raise ValidationError(f"a non-empty {kind} name is required")
    return name


class QueueSpecification:
    """Chainable builder for one queue declaration (§5.1).

    Every setter mutates the builder and returns it, so a whole declaration
    reads as one expression. :meth:`stream`, :meth:`quorum` and :meth:`classic`
    return sub-builders that are *views* over this same object — each one sets
    ``x-queue-type`` immediately and exposes ``queue()`` to come back here.
    Numeric bounds from §5.4 are checked by the setter that writes them, so an
    out-of-range value raises before any frame is sent.

    Example:
        >>> management.queue("orders").quorum().delivery_limit(5).queue().declare()
    """

    def __init__(self, management: Management, name: str = "") -> None:
        """Create a builder for the queue called ``name``.

        Args:
            management: The management endpoint the requests go out on.
            name: Queue name, or ``""`` to have :meth:`declare` generate one.
        """
        self._management = management
        self._name = name
        self._exclusive = False
        self._auto_delete = False
        self._arguments: dict[str, Any] = {}

    # --- readers --------------------------------------------------------

    @property
    def queue_name(self) -> str:
        """The name set so far, or ``""`` when none was set."""
        return self._name

    @property
    def is_exclusive(self) -> bool:
        """Whether the queue will be declared exclusive."""
        return self._exclusive

    @property
    def is_auto_delete(self) -> bool:
        """Whether the queue will be declared auto-delete."""
        return self._auto_delete

    @property
    def queue_arguments(self) -> dict[str, Any]:
        """A copy of the ``x-*`` arguments accumulated so far."""
        return dict(self._arguments)

    # --- generic setters ------------------------------------------------

    def name(self, name: str) -> QueueSpecification:
        """Set the queue name, used both as the path segment and by declare."""
        self._name = name
        return self

    def exclusive(self, exclusive: bool = True) -> QueueSpecification:
        """Set ``exclusive``; forced back to ``False`` for quorum/stream queues."""
        self._exclusive = exclusive
        return self

    def auto_delete(self, auto_delete: bool = True) -> QueueSpecification:
        """Set ``auto_delete``; forced back to ``False`` for quorum/stream queues."""
        self._auto_delete = auto_delete
        return self

    def arguments(self, arguments: Mapping[str, Any]) -> QueueSpecification:
        """Merge ``arguments`` into the argument map.

        The escape hatch for any ``x-*`` key without a typed setter below;
        values pass through unvalidated.
        """
        self._arguments.update({str(key): value for key, value in arguments.items()})
        return self

    def _set_argument(self, key: str, value: Any) -> QueueSpecification:
        """Set one argument and return the builder, for the sub-builders' use."""
        self._arguments[key] = value
        return self

    # --- typed setters (§5.4) -------------------------------------------

    def type(self, queue_type: QueueType) -> QueueSpecification:
        """Set ``x-queue-type``."""
        return self._set_argument(ARG_QUEUE_TYPE, queue_type.value)

    def dead_letter_exchange(self, name: str) -> QueueSpecification:
        """Set ``x-dead-letter-exchange``."""
        return self._set_argument(ARG_DEAD_LETTER_EXCHANGE, name)

    def dead_letter_routing_key(self, key: str) -> QueueSpecification:
        """Set ``x-dead-letter-routing-key``."""
        return self._set_argument(ARG_DEAD_LETTER_ROUTING_KEY, key)

    def overflow_strategy(self, strategy: OverflowStrategy) -> QueueSpecification:
        """Set ``x-overflow``."""
        return self._set_argument(ARG_OVERFLOW, strategy.value)

    def max_length_bytes(self, max_length_bytes: int) -> QueueSpecification:
        """Set ``x-max-length-bytes``.

        Raises:
            ValidationError: If it is not > 0.
        """
        return self._set_argument(ARG_MAX_LENGTH_BYTES, Long(_require_positive(ARG_MAX_LENGTH_BYTES, max_length_bytes)))

    def max_length(self, max_length: int) -> QueueSpecification:
        """Set ``x-max-length``.

        Raises:
            ValidationError: If it is not > 0.
        """
        return self._set_argument(ARG_MAX_LENGTH, Long(_require_positive(ARG_MAX_LENGTH, max_length)))

    def message_ttl(self, message_ttl: int | timedelta) -> QueueSpecification:
        """Set ``x-message-ttl``, in milliseconds or as a timedelta.

        Raises:
            ValidationError: If it is negative or above ten years.
        """
        value = _milliseconds(message_ttl)
        return self._set_argument(ARG_MESSAGE_TTL, Long(_require_within(ARG_MESSAGE_TTL, value, 0, TEN_YEARS_MS)))

    def expires(self, expires: int | timedelta) -> QueueSpecification:
        """Set ``x-expires``, in milliseconds or as a timedelta.

        Raises:
            ValidationError: If it is not > 0, or is above ten years.
        """
        value = _milliseconds(expires)
        return self._set_argument(ARG_EXPIRES, Long(_require_within(ARG_EXPIRES, value, 1, TEN_YEARS_MS)))

    def leader_locator(self, strategy: LeaderLocatorStrategy) -> QueueSpecification:
        """Set ``x-queue-leader-locator``."""
        return self._set_argument(ARG_QUEUE_LEADER_LOCATOR, strategy.value)

    def single_active_consumer(self, single_active_consumer: bool = True) -> QueueSpecification:
        """Set ``x-single-active-consumer``."""
        return self._set_argument(ARG_SINGLE_ACTIVE_CONSUMER, single_active_consumer)

    # --- type-specific sub-builders -------------------------------------

    def stream(self) -> StreamSpecification:
        """Set ``x-queue-type`` to ``stream`` and return the stream sub-builder."""
        self.type(QueueType.STREAM)
        return StreamSpecification(self)

    def quorum(self) -> QuorumQueueSpecification:
        """Set ``x-queue-type`` to ``quorum`` and return the quorum sub-builder."""
        self.type(QueueType.QUORUM)
        return QuorumQueueSpecification(self)

    def classic(self) -> ClassicQueueSpecification:
        """Set ``x-queue-type`` to ``classic`` and return the classic sub-builder."""
        self.type(QueueType.CLASSIC)
        return ClassicQueueSpecification(self)

    # --- terminal operations --------------------------------------------

    @property
    def is_replicated(self) -> bool:
        """Whether the queue type forbids ``exclusive``/``auto_delete``."""
        return self._arguments.get(ARG_QUEUE_TYPE) in (QueueType.QUORUM.value, QueueType.STREAM.value)

    def declare_body(self) -> dict[str, Any]:
        """Build the ``PUT /queues/{name}`` request body.

        ``durable`` is always ``True`` — there is no transient-queue declare —
        and ``exclusive``/``auto_delete`` are normalised to ``False`` for
        quorum and stream queues, which the broker rejects otherwise. The
        normalisation is written back to this builder, so the readers agree with
        what was sent.

        Returns:
            The declare body.

        Raises:
            ValidationError: If ``x-delayed-retry-min``/``x-delayed-retry-max``
                were set without ``x-delayed-retry-type``.
        """
        self._validate()
        if self.is_replicated:
            self._exclusive = False
            self._auto_delete = False
        return {
            "durable": True,
            "exclusive": self._exclusive,
            "auto_delete": self._auto_delete,
            "arguments": dict(self._arguments),
        }

    def declare(self) -> QueueInfo:
        """Declare the queue with ``PUT /queues/{name}``.

        Generates a name first when none was set, so the queue stays trackable
        and deletable by this client.

        Returns:
            The queue's state as the broker reports it.

        Raises:
            ValidationError: If the accumulated arguments are inconsistent.
            ManagementError: If the queue already exists with conflicting
                properties (``409``), or the broker answers an unexpected code.
        """
        if not self._name:
            self._name = generate_queue_name()
        return self._management._declare_queue(self)

    def purge(self) -> int:
        """Purge the queue with ``DELETE /queues/{name}/messages``.

        Returns:
            How many messages were discarded.

        Raises:
            ValidationError: If no name was set.
            ManagementError: If the queue does not exist (``404``).
        """
        return self._management._purge_queue(self._name)

    def delete(self) -> QueueInfo:
        """Delete the queue with ``DELETE /queues/{name}``.

        Returns:
            A stub holding only the name: the delete response carries no queue
            state worth parsing.

        Raises:
            ValidationError: If no name was set.
            ManagementError: If the queue does not exist (``404``).
        """
        return self._management._delete_queue(self._name)

    # --- internals ------------------------------------------------------

    def _validate(self) -> None:
        """Check the cross-argument rules the broker is never asked to enforce."""
        bounds = (ARG_DELAYED_RETRY_MIN, ARG_DELAYED_RETRY_MAX)
        set_bounds = [key for key in bounds if key in self._arguments]
        if set_bounds and ARG_DELAYED_RETRY_TYPE not in self._arguments:
            raise ValidationError(f"{' and '.join(set_bounds)} require {ARG_DELAYED_RETRY_TYPE} to be set as well")


class StreamSpecification:
    """Stream-only settings, as a view over the parent :class:`QueueSpecification`."""

    def __init__(self, parent: QueueSpecification) -> None:
        """Wrap ``parent``; every setter here writes to that same builder."""
        self._parent = parent

    def queue(self) -> QueueSpecification:
        """Return the parent builder, to resume chaining or call ``declare()``."""
        return self._parent

    def max_age(self, max_age: int | timedelta) -> StreamSpecification:
        """Set ``x-max-age``, in seconds or as a timedelta, encoded as ``"<seconds>s"``.

        Raises:
            ValidationError: If it is not > 0.
        """
        seconds = _require_positive(ARG_MAX_AGE, _seconds(max_age))
        self._parent._set_argument(ARG_MAX_AGE, f"{seconds}s")
        return self

    def max_segment_size_bytes(self, max_segment_size_bytes: int) -> StreamSpecification:
        """Set ``x-stream-max-segment-size-bytes``.

        Raises:
            ValidationError: If it is not > 0.
        """
        value = _require_positive(ARG_STREAM_MAX_SEGMENT_SIZE_BYTES, max_segment_size_bytes)
        self._parent._set_argument(ARG_STREAM_MAX_SEGMENT_SIZE_BYTES, Long(value))
        return self

    def initial_cluster_size(self, initial_cluster_size: int) -> StreamSpecification:
        """Set ``x-initial-cluster-size``.

        Raises:
            ValidationError: If it is not > 0.
        """
        value = _require_positive(ARG_INITIAL_CLUSTER_SIZE, initial_cluster_size)
        self._parent._set_argument(ARG_INITIAL_CLUSTER_SIZE, value)
        return self

    def file_size_per_chunk(self, file_size_per_chunk: int) -> StreamSpecification:
        """Set ``x-stream-file-size-per-chunk``.

        Raises:
            ValidationError: If it is not > 0.
        """
        value = _require_positive(ARG_STREAM_FILE_SIZE_PER_CHUNK, file_size_per_chunk)
        self._parent._set_argument(ARG_STREAM_FILE_SIZE_PER_CHUNK, Long(value))
        return self


class QuorumQueueSpecification:
    """Quorum-only settings, as a view over the parent :class:`QueueSpecification`."""

    def __init__(self, parent: QueueSpecification) -> None:
        """Wrap ``parent``; every setter here writes to that same builder."""
        self._parent = parent

    def queue(self) -> QueueSpecification:
        """Return the parent builder, to resume chaining or call ``declare()``."""
        return self._parent

    def dead_letter_strategy(self, strategy: QuorumQueueDeadLetterStrategy) -> QuorumQueueSpecification:
        """Set ``x-dead-letter-strategy``."""
        self._parent._set_argument(ARG_DEAD_LETTER_STRATEGY, strategy.value)
        return self

    def delivery_limit(self, delivery_limit: int) -> QuorumQueueSpecification:
        """Set ``x-delivery-limit``.

        Raises:
            ValidationError: If it is not > 0.
        """
        self._parent._set_argument(ARG_DELIVERY_LIMIT, _require_positive(ARG_DELIVERY_LIMIT, delivery_limit))
        return self

    def quorum_initial_group_size(self, size: int) -> QuorumQueueSpecification:
        """Set ``x-quorum-initial-group-size``.

        Raises:
            ValidationError: If it is not > 0.
        """
        self._parent._set_argument(
            ARG_QUORUM_INITIAL_GROUP_SIZE, _require_positive(ARG_QUORUM_INITIAL_GROUP_SIZE, size)
        )
        return self

    def quorum_target_group_size(self, size: int) -> QuorumQueueSpecification:
        """Set ``x-quorum-target-group-size``.

        Raises:
            ValidationError: If it is not > 0.
        """
        self._parent._set_argument(ARG_QUORUM_TARGET_GROUP_SIZE, _require_positive(ARG_QUORUM_TARGET_GROUP_SIZE, size))
        return self

    def delayed_retry_type(self, retry_type: QuorumQueueDelayedRetryType) -> QuorumQueueSpecification:
        """Set ``x-delayed-retry-type``; required alongside either bound below."""
        self._parent._set_argument(ARG_DELAYED_RETRY_TYPE, retry_type.value)
        return self

    def delayed_retry_min(self, delayed_retry_min: int | timedelta) -> QuorumQueueSpecification:
        """Set ``x-delayed-retry-min``, in milliseconds or as a timedelta.

        Raises:
            ValidationError: If it is not > 0.
        """
        value = _require_positive(ARG_DELAYED_RETRY_MIN, _milliseconds(delayed_retry_min))
        self._parent._set_argument(ARG_DELAYED_RETRY_MIN, Long(value))
        return self

    def delayed_retry_max(self, delayed_retry_max: int | timedelta) -> QuorumQueueSpecification:
        """Set ``x-delayed-retry-max``, in milliseconds or as a timedelta.

        Raises:
            ValidationError: If it is not > 0.
        """
        value = _require_positive(ARG_DELAYED_RETRY_MAX, _milliseconds(delayed_retry_max))
        self._parent._set_argument(ARG_DELAYED_RETRY_MAX, Long(value))
        return self


class ClassicQueueSpecification:
    """Classic-only settings, as a view over the parent :class:`QueueSpecification`."""

    def __init__(self, parent: QueueSpecification) -> None:
        """Wrap ``parent``; every setter here writes to that same builder."""
        self._parent = parent

    def queue(self) -> QueueSpecification:
        """Return the parent builder, to resume chaining or call ``declare()``."""
        return self._parent

    def max_priority(self, max_priority: int) -> ClassicQueueSpecification:
        """Set ``x-max-priority``.

        Raises:
            ValidationError: If it is outside ``1..255``.
        """
        value = _require_within(ARG_MAX_PRIORITY, max_priority, MIN_MAX_PRIORITY, MAX_MAX_PRIORITY)
        self._parent._set_argument(ARG_MAX_PRIORITY, value)
        return self

    def mode(self, mode: ClassicQueueMode) -> ClassicQueueSpecification:
        """Set ``x-queue-mode``."""
        self._parent._set_argument(ARG_QUEUE_MODE, mode.value)
        return self

    def version(self, version: ClassicQueueVersion) -> ClassicQueueSpecification:
        """Set ``x-queue-version``, sent as the integer ``1`` or ``2``."""
        self._parent._set_argument(ARG_QUEUE_VERSION, version.value)
        return self


class ExchangeSpecification:
    """Chainable builder for one exchange declaration (§5.2).

    There is no type-specific sub-builder: an exchange type is a plain value,
    not a family of differently-shaped argument sets. :meth:`declare` and
    :meth:`delete` return nothing, matching the responses the broker sends.

    Example:
        >>> management.exchange("events").type(ExchangeType.TOPIC).declare()
    """

    def __init__(self, management: Management, name: str = "") -> None:
        """Create a builder for the exchange called ``name``.

        Args:
            management: The management endpoint the requests go out on.
            name: Exchange name; must be non-empty before declaring or deleting.
        """
        self._management = management
        self._name = name
        self._auto_delete = False
        self._type = ExchangeType.DIRECT.value
        self._arguments: dict[str, Any] = {}

    # --- readers --------------------------------------------------------

    @property
    def exchange_name(self) -> str:
        """The name set so far, or ``""`` when none was set."""
        return self._name

    @property
    def is_auto_delete(self) -> bool:
        """Whether the exchange will be declared auto-delete."""
        return self._auto_delete

    @property
    def exchange_type(self) -> str:
        """The type set so far; ``"direct"`` when never set."""
        return self._type

    @property
    def exchange_arguments(self) -> dict[str, Any]:
        """A copy of the arguments accumulated so far."""
        return dict(self._arguments)

    # --- setters --------------------------------------------------------

    def name(self, name: str) -> ExchangeSpecification:
        """Set the exchange name, used both as the path segment and by declare."""
        self._name = name
        return self

    def auto_delete(self, auto_delete: bool = True) -> ExchangeSpecification:
        """Set ``auto_delete``."""
        self._auto_delete = auto_delete
        return self

    def type(self, exchange_type: ExchangeType | str) -> ExchangeSpecification:
        """Set the exchange type.

        Args:
            exchange_type: A built-in type, or a plugin type name such as
                ``"x-consistent-hash"``.
        """
        self._type = exchange_type.value if isinstance(exchange_type, ExchangeType) else exchange_type
        return self

    def argument(self, key: str, value: Any) -> ExchangeSpecification:
        """Set one entry in the arguments map."""
        self._arguments[key] = value
        return self

    def arguments(self, arguments: Mapping[str, Any]) -> ExchangeSpecification:
        """Merge ``arguments`` into the arguments map."""
        self._arguments.update({str(key): value for key, value in arguments.items()})
        return self

    # --- terminal operations --------------------------------------------

    def declare_body(self) -> dict[str, Any]:
        """Build the ``PUT /exchanges/{name}`` request body.

        ``durable`` is always ``True`` and the type is lower-cased, as §5.2
        requires.
        """
        return {
            "durable": True,
            "auto_delete": self._auto_delete,
            "type": self._type.lower(),
            "arguments": dict(self._arguments),
        }

    def declare(self) -> None:
        """Declare the exchange with ``PUT /exchanges/{name}``.

        Raises:
            ValidationError: If no name was set.
            ManagementError: If the exchange already exists with conflicting
                properties (``409``), or the broker answers an unexpected code.
        """
        self._management._declare_exchange(self)

    def delete(self) -> None:
        """Delete the exchange with ``DELETE /exchanges/{name}``.

        Raises:
            ValidationError: If no name was set.
            ManagementError: If the broker answers an unexpected code.
        """
        self._management._delete_exchange(self._name)


__all__ = [
    "ALWAYS_ERROR_CODES",
    "BINDINGS_PATH",
    "DEFAULT_REQUEST_TIMEOUT_SECONDS",
    "GENERATED_NAME_PREFIX",
    "TEN_YEARS_MS",
    "VERB_DELETE",
    "VERB_GET",
    "VERB_POST",
    "VERB_PUT",
    "ClassicQueueMode",
    "ClassicQueueSpecification",
    "ClassicQueueVersion",
    "ExchangeSpecification",
    "ExchangeType",
    "LeaderLocatorStrategy",
    "Management",
    "OverflowStrategy",
    "QueueInfo",
    "QueueSpecification",
    "QueueType",
    "QuorumQueueDeadLetterStrategy",
    "QuorumQueueDelayedRetryType",
    "QuorumQueueSpecification",
    "StreamSpecification",
    "bindings_query_path",
    "build_request",
    "encode_path_segment",
    "encode_query_value",
    "exchange_path",
    "generate_queue_name",
    "queue_messages_path",
    "queue_path",
    "unbind_path",
    "validate_response",
]
