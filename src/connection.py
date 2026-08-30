"""The underlying AMQP 1.0 connection: transport, bootstrap and frame dispatch.

This module owns one socket and the background frame-reader thread that turns
inbound bytes into performatives and routes them to the :class:`~.session.Session`
that owns the frame's channel. It is the wire-level "connection" of
``amqp-core-overview-v1.0-os``; the user-facing facade that adds
``Management()``/``PublisherBuilder()``/``ConsumerBuilder()`` is layered on top of
it in a later step.
"""

from __future__ import annotations

import contextlib
import socket
import ssl
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from .constants import (
    DEFAULT_AMQP_PORT,
    DEFAULT_AMQPS_PORT,
    DEFAULT_CHANNEL_MAX,
    DEFAULT_HOST,
    DEFAULT_IDLE_TIMEOUT_MS,
    DEFAULT_MAX_FRAME_SIZE,
    DEFAULT_PASSWORD,
    DEFAULT_USER,
    DEFAULT_VIRTUAL_HOST,
)
from .consumer import Consumer, ConsumerBuilder
from .exceptions import AMQPError, AuthenticationError, ProtocolError, ValidationError
from .logging_utils import get_logger
from .management import Management
from .publisher import Publisher, PublisherBuilder
from .reconnection import RecordingTopologyListener, RecoveryConfiguration
from .session import Session
from .wire import (
    AMQP_PROTOCOL_HEADER,
    AMQP_SASL_HEADER,
    EMPTY_FRAME,
    FRAME_TYPE_AMQP,
    FRAME_TYPE_SASL,
    MECHANISM_ANONYMOUS,
    MECHANISM_EXTERNAL,
    MECHANISM_PLAIN,
    Begin,
    Close,
    Error,
    Open,
    Performative,
    SaslChallenge,
    SaslInit,
    SaslMechanisms,
    SaslOutcome,
    SaslResponse,
    build_plain_initial_response,
    decode_frame_body,
    read_frame,
    read_protocol_header,
    write_frame,
    write_protocol_header,
)

#: Prefix of a generated ``Open.container-id``.
CONTAINER_ID_PREFIX = "rabbitmq-amqp-python-client"

#: Condition RabbitMQ uses when it rejects the credentials or the virtual host.
UNAUTHORIZED_ACCESS_CONDITION = "amqp:unauthorized-access"

CONNECT_TIMEOUT_SECONDS = 30.0
HANDSHAKE_TIMEOUT_SECONDS = 30.0
CLOSE_TIMEOUT_SECONDS = 10.0
THREAD_JOIN_TIMEOUT_SECONDS = 5.0
MIN_HEARTBEAT_INTERVAL_SECONDS = 1.0

#: Callback invoked at most once when the connection dies without ``close()``.
#: Receives the failure that killed it, or ``None`` when the peer closed
#: cleanly (a ``close`` performative carrying no ``error``).
UnexpectedCloseCallback = Callable[[BaseException | None], None]

_logger = get_logger("connection")


class ConnectionState(Enum):
    """Lifecycle of a :class:`Connection` (step_040 §3).

    ``RECONNECTING`` is entered when the transport dies while
    :attr:`~.reconnection.RecoveryConfiguration.activated` is set, and left for
    ``OPEN`` once everything has been re-attached or for ``CLOSED`` once the
    back-off policy gives up.
    """

    OPEN = "open"
    RECONNECTING = "reconnecting"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class ConnectionParameters:
    """Everything needed to dial and bootstrap one AMQP 1.0 connection.

    Attributes:
        host: TCP host to dial.
        port: TCP port to dial; when ``None`` it resolves to 5672, or 5671 if
            ``tls`` is set.
        virtual_host: RabbitMQ virtual host, encoded into ``Open.hostname`` as
            ``vhost:{virtual_host}`` unless it is the default ``"/"``.
        user: SASL PLAIN username; PLAIN is selected whenever ``user`` or
            ``password`` is non-empty, ANONYMOUS when both are empty.
        password: SASL PLAIN password.
        container_id: ``Open.container-id``; a unique one is generated when empty.
        tls: SSL context used to wrap the socket before the protocol header
            handshake; ``None`` disables TLS.
        sasl_external: Use SASL EXTERNAL — authenticating by the identity of
            the client certificate presented during the TLS handshake —
            instead of PLAIN/ANONYMOUS (step_110_tls_transport.md §2.1).
            Requires ``tls`` to be set and to carry a client certificate;
            ``tls`` being unset raises ``ValidationError`` eagerly, since that
            half is checkable without a network round trip. RabbitMQ's
            ``rabbitmq_auth_mechanism_ssl`` plugin enforces the rest.
        max_frame_size: ``Open.max-frame-size`` this client advertises.
        channel_max: ``Open.channel-max`` this client advertises.
        idle_timeout: ``Open.idle-time-out`` in milliseconds; 0 disables it.
        on_unexpected_close: Invoked at most once when the connection dies for
            any reason other than :meth:`Connection.close` — deferred, and
            skipped altogether, when auto-reconnection recovers instead
            (step_040 §5).
        recovery_configuration: Whether and how the connection redials itself
            after an unexpected closure (step_040 §2). Read once here and fixed
            for the connection's lifetime.
    """

    host: str = DEFAULT_HOST
    port: int | None = None
    virtual_host: str = DEFAULT_VIRTUAL_HOST
    user: str = DEFAULT_USER
    password: str = DEFAULT_PASSWORD
    container_id: str = ""
    tls: ssl.SSLContext | None = None
    sasl_external: bool = False
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE
    channel_max: int = DEFAULT_CHANNEL_MAX
    idle_timeout: int = DEFAULT_IDLE_TIMEOUT_MS
    on_unexpected_close: UnexpectedCloseCallback | None = None
    recovery_configuration: RecoveryConfiguration = field(default_factory=RecoveryConfiguration)

    def __post_init__(self) -> None:
        if self.sasl_external and self.tls is None:
            raise ValidationError("sasl_external requires tls to be set")
        if self.port is None:
            self.port = DEFAULT_AMQPS_PORT if self.tls is not None else DEFAULT_AMQP_PORT
        if not self.container_id:
            self.container_id = f"{CONTAINER_ID_PREFIX}-{uuid.uuid4().hex}"

    @property
    def resolved_port(self) -> int:
        """The port to dial, defaulted from whether TLS is in use."""
        if self.port is not None:
            return self.port
        return DEFAULT_AMQPS_PORT if self.tls is not None else DEFAULT_AMQP_PORT

    @property
    def open_hostname(self) -> str | None:
        """``Open.hostname`` for this virtual host, or ``None`` for the default one."""
        if self.virtual_host == DEFAULT_VIRTUAL_HOST:
            return None
        return f"vhost:{self.virtual_host}"

    @property
    def sasl_mechanism(self) -> str:
        """``EXTERNAL`` if requested, else ``PLAIN`` when any credential is set, else ``ANONYMOUS``."""
        if self.sasl_external:
            return MECHANISM_EXTERNAL
        if not self.user and not self.password:
            return MECHANISM_ANONYMOUS
        return MECHANISM_PLAIN


def _connect_socket(parameters: ConnectionParameters) -> socket.socket:
    """Dial ``parameters.host``/``parameters.resolved_port``, wrapping in TLS if configured."""
    sock = socket.create_connection(
        (parameters.host, parameters.resolved_port),
        timeout=CONNECT_TIMEOUT_SECONDS,
    )
    with contextlib.suppress(OSError):  # a non-TCP transport has no such option
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if parameters.tls is None:
        return sock
    try:
        return parameters.tls.wrap_socket(sock, server_hostname=parameters.host)
    except BaseException:
        sock.close()
        raise


class Connection:
    """One AMQP 1.0 connection: socket, bootstrap, and inbound frame dispatch.

    Constructing a ``Connection`` performs the whole bootstrap synchronously —
    TCP/TLS connect, SASL negotiation, ``open``/``open`` — and then starts the
    background frame-reader thread. No session is opened; sessions are created
    on demand with :meth:`open_session`.

    Attributes:
        parameters: The parameters this connection was built from, with defaults
            already applied.

    Example:
        >>> connection = Connection(ConnectionParameters(host="localhost"))
        >>> session = connection.open_session()
        >>> connection.close()
    """

    def __init__(self, parameters: ConnectionParameters | None = None) -> None:
        """Connect, authenticate, exchange ``open`` and start the frame reader.

        Args:
            parameters: Connection settings; defaults are used when omitted.

        Raises:
            AuthenticationError: If SASL negotiation is rejected, or the broker
                refuses the credentials/virtual host while opening.
            ProtocolError: If the broker refuses a protocol layer or sends
                something other than the expected performative.
            OSError: If the TCP/TLS connect itself fails.
        """
        self._parameters = parameters if parameters is not None else ConnectionParameters()
        self._logger = _logger
        self._state = ConnectionState.CLOSED
        self._state_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._sessions: dict[int, Session] = {}
        self._sessions_by_remote_channel: dict[int, Session] = {}
        self._next_channel = 0
        self._close_requested = threading.Event()
        self._shutdown = threading.Event()
        self._inbound_finished = threading.Event()
        self._unexpected_close_notified = False
        self._remote_open: Open | None = None
        self._remote_close: Close | None = None
        self._max_frame_size = self._parameters.max_frame_size
        self._channel_max = self._parameters.channel_max
        self._remote_idle_timeout = 0
        self._reader: threading.Thread | None = None
        self._heartbeat: threading.Thread | None = None
        self._management: Management | None = None
        self._management_lock = threading.Lock()
        self._shared_session: Session | None = None
        self._pub_sub_lock = threading.Lock()
        self._publishers: dict[str, Publisher] = {}
        self._publishers_lock = threading.Lock()
        self._consumers: dict[str, Consumer] = {}
        self._consumers_lock = threading.Lock()
        self._recovery = self._parameters.recovery_configuration
        self._topology_listener = RecordingTopologyListener()
        self._recovery_thread: threading.Thread | None = None
        self._recovery_cancelled = threading.Event()
        self._transport_epoch = 0

        self._socket = _connect_socket(self._parameters)
        try:
            self._bootstrap()
        except BaseException:
            self._close_socket()
            raise
        self._state = ConnectionState.OPEN
        self._start_threads()

    # --- public surface -------------------------------------------------

    @property
    def parameters(self) -> ConnectionParameters:
        """The settings this connection was built from."""
        return self._parameters

    @property
    def recovery_configuration(self) -> RecoveryConfiguration:
        """How this connection reacts to losing its transport (step_040 §2)."""
        return self._recovery

    @property
    def topology_listener(self) -> RecordingTopologyListener:
        """The always-on recorder of everything declared through :meth:`management`."""
        return self._topology_listener

    @property
    def state(self) -> ConnectionState:
        """Current lifecycle state."""
        with self._state_lock:
            return self._state

    @property
    def is_open(self) -> bool:
        """Whether the connection is usable."""
        return self.state is ConnectionState.OPEN

    @property
    def container_id(self) -> str:
        """The ``container-id`` this client announced."""
        return self._parameters.container_id

    @property
    def max_frame_size(self) -> int:
        """Effective ``max-frame-size``: the minimum of both peers' values."""
        return self._max_frame_size

    @property
    def channel_max(self) -> int:
        """Effective ``channel-max``: the minimum of both peers' values."""
        return self._channel_max

    @property
    def remote_open(self) -> Open | None:
        """The peer's ``open`` performative, available once bootstrap succeeded."""
        return self._remote_open

    def open_session(self) -> Session:
        """Begin a new session on the next free channel.

        Callers needing non-default flow-control windows build a
        :class:`~.session.Session` themselves and call ``begin(connection)``.

        Returns:
            The begun session.

        Raises:
            ProtocolError: If the connection is not open.
            AMQPTimeoutError: If the broker does not reply to ``begin``.
        """
        session = Session()
        session.begin(self)
        return session

    def management(self) -> Management:
        """Return this connection's management endpoint, opening it on first use.

        One :class:`~.management.Management` instance is shared per connection:
        it owns a dedicated session and one link pair to ``/management``, both
        torn down by :meth:`close`. A management endpoint that was closed
        explicitly is replaced by a fresh one on the next call.

        Returns:
            The open management endpoint.

        Raises:
            ProtocolError: If the connection is not open, or the broker refuses
                either half of the link pair.
            AMQPTimeoutError: If the broker does not answer ``begin``/``attach``.
        """
        with self._management_lock:
            management = self._management
            if management is not None and management.is_open:
                return management
            management = Management(self, topology_listener=self._topology_listener)
            management.open()
            self._management = management
            return management

    def publisher_builder(self) -> PublisherBuilder:
        """Return a fresh builder for one :class:`~.publisher.Publisher`.

        Unlike :meth:`management`, nothing is cached: every call builds a
        distinct publisher. The shared pub/sub session the built publisher
        attaches on *is* cached, and is opened by whichever builder's
        :meth:`~.publisher.PublisherBuilder.build` runs first.

        Returns:
            A builder bound to this connection.
        """
        return PublisherBuilder(self)

    def consumer_builder(self) -> ConsumerBuilder:
        """Return a fresh builder for one :class:`~.consumer.Consumer`.

        Nothing is cached: every call builds a distinct consumer, with its own
        receiver link and its own delivery thread. The shared pub/sub session
        those links attach on *is* cached, and is opened by whichever publisher
        or consumer is built first.

        Returns:
            A builder bound to this connection.
        """
        return ConsumerBuilder(self)

    def send_frame(self, channel: int, performative: Performative, payload: bytes = b"") -> None:
        """Write one AMQP frame carrying ``performative`` on ``channel``.

        Args:
            channel: Channel number the frame applies to.
            performative: Performative to encode as the frame body.
            payload: Raw bytes appended after the performative, for ``transfer``.

        Raises:
            ProtocolError: If the connection is no longer writable, or the
                socket fails while sending.
        """
        if self._shutdown.is_set() or self._state is ConnectionState.CLOSED:
            raise ProtocolError("the connection is closed")
        body = performative.encode() + payload
        with self._write_lock:
            write_frame(self._socket, FRAME_TYPE_AMQP, channel, body)

    def allocate_channel(self, session: Session) -> int:
        """Reserve the next free channel number for ``session``.

        Scanning resumes from the last channel handed out rather than from zero,
        so a channel is not reused immediately after its session ended and a
        broker that has not finished processing the ``end`` cannot confuse the
        two sessions.

        Args:
            session: Session to register; inbound frames on the returned channel
                are dispatched to it.

        Returns:
            The channel number reserved.

        Raises:
            ProtocolError: If the connection is neither open nor recovering.
            AMQPError: If every channel up to ``channel-max`` is in use.
        """
        with self._state_lock:
            # A recovering connection allocates channels for the sessions the
            # recovery loop itself re-opens (step_040 §3.3); a caller racing it
            # still fails, one layer down, on the socket that is not back yet.
            if self._state not in (ConnectionState.OPEN, ConnectionState.RECONNECTING):
                raise ProtocolError(f"cannot open a session while the connection is {self._state.value}")
            span = self._channel_max + 1
            for offset in range(span):
                candidate = (self._next_channel + offset) % span
                if candidate not in self._sessions:
                    self._sessions[candidate] = session
                    self._next_channel = (candidate + 1) % span
                    return candidate
        raise AMQPError(f"every channel up to channel-max {self._channel_max} is already in use")

    def release_channel(self, channel: int) -> None:
        """Unregister the session holding ``channel``; unknown channels are ignored."""
        with self._state_lock:
            session = self._sessions.pop(channel, None)
            if session is None:
                return
            for remote_channel, registered in list(self._sessions_by_remote_channel.items()):
                if registered is session:
                    del self._sessions_by_remote_channel[remote_channel]

    def close(self, error: Error | None = None) -> None:
        """Close the connection; safe to call more than once.

        Closes every publisher, consumer and session, sends ``close``, waits a
        bounded time for the peer's ``close``, then closes the socket and joins
        the background threads. ``on_unexpected_close`` is never invoked for this
        path.

        Only the first call does anything, and it runs the whole teardown even
        when the transport already died on its own — otherwise a connection that
        dropped unexpectedly would leave its publishers and consumers tracked
        forever. Everything it then tries to send fails harmlessly and is logged.

        A close while the connection is ``RECONNECTING`` first cancels the
        recovery loop (step_040 §4), so it never waits out a back-off delay or an
        in-flight redial, and then tears down whatever happens to be open — which
        may be nothing at all.

        Args:
            error: Optional ``error`` to put on the outgoing ``close``.
        """
        with self._close_lock:
            if self._close_requested.is_set():
                return
            self._close_requested.set()
            self._cancel_recovery()
            with self._state_lock:
                self._state = ConnectionState.CLOSING
            self._close_management()
            self._close_publishers()
            self._close_consumers()
            self._release_pub_sub_session()
            self._end_sessions()
            self._send_close(error)
            if not self._inbound_finished.wait(CLOSE_TIMEOUT_SECONDS):
                self._logger.warning("broker did not reply to close within %.1fs", CLOSE_TIMEOUT_SECONDS)
            self._shutdown.set()
            self._close_socket()
            self._join_threads()
            with self._state_lock:
                self._state = ConnectionState.CLOSED
            self._logger.debug("connection to %s closed", self._parameters.host)

    # --- pub/sub session and publisher/consumer registries -------------

    def _pub_sub_session(self) -> Session:
        """Return the session every publisher and consumer shares, opening it once.

        The first :meth:`~.publisher.PublisherBuilder.build` (or, later, the
        first consumer built) on this connection begins it; every later one
        reuses it. It is ended only by :meth:`close`, never when the last
        publisher on it closes.

        Returns:
            The open pub/sub session.

        Raises:
            ProtocolError: If the connection is not open.
            AMQPTimeoutError: If the broker does not reply to ``begin``.
        """
        with self._pub_sub_lock:
            session = self._shared_session
            if session is not None and session.is_open:
                return session
            session = self.open_session()
            self._shared_session = session
            return session

    def _register_publisher(self, publisher: Publisher) -> None:
        """Track ``publisher`` so :meth:`close` can close it."""
        with self._publishers_lock:
            self._publishers[publisher.id] = publisher

    def _unregister_publisher(self, publisher: Publisher) -> None:
        """Forget ``publisher``; an already-forgotten one is ignored."""
        with self._publishers_lock:
            if self._publishers.get(publisher.id) is publisher:
                del self._publishers[publisher.id]

    def _register_consumer(self, consumer: Consumer) -> None:
        """Track ``consumer`` so :meth:`close` can close it.

        This registry is also what a later auto-reconnection step walks to
        re-attach every consumer's receiver link after an unexpected disconnect.
        """
        with self._consumers_lock:
            self._consumers[consumer.id] = consumer

    def _unregister_consumer(self, consumer: Consumer) -> None:
        """Forget ``consumer``; an already-forgotten one is ignored."""
        with self._consumers_lock:
            if self._consumers.get(consumer.id) is consumer:
                del self._consumers[consumer.id]

    # --- bootstrap -----------------------------------------------------

    def _bootstrap(self) -> None:
        """Run the SASL and ``open`` handshakes on the freshly connected socket."""
        self._socket.settimeout(HANDSHAKE_TIMEOUT_SECONDS)
        self._negotiate_sasl()
        remote_open = self._negotiate_open()
        self._remote_open = remote_open
        self._max_frame_size = min(self._parameters.max_frame_size, remote_open.max_frame_size)
        self._channel_max = min(self._parameters.channel_max, remote_open.channel_max)
        self._remote_idle_timeout = remote_open.idle_time_out or 0
        self._socket.settimeout(None)
        self._logger.debug(
            "connected to %s:%d as container %r (max-frame-size=%d, channel-max=%d)",
            self._parameters.host,
            self._parameters.resolved_port,
            self._parameters.container_id,
            self._max_frame_size,
            self._channel_max,
        )

    def _negotiate_sasl(self) -> None:
        """Enter the SASL layer, authenticate, and leave it on success."""
        write_protocol_header(self._socket, AMQP_SASL_HEADER)
        header = read_protocol_header(self._socket)
        if header != AMQP_SASL_HEADER:
            raise ProtocolError(f"broker refused the SASL layer and offered {header!r} instead")

        mechanisms = self._read_sasl_frame()
        if not isinstance(mechanisms, SaslMechanisms):
            raise ProtocolError(f"expected sasl-mechanisms, got {type(mechanisms).__name__}")
        mechanism = self._parameters.sasl_mechanism
        if mechanism not in mechanisms.server_mechanisms:
            raise AuthenticationError(
                f"broker does not offer SASL {mechanism}; offered mechanisms are {mechanisms.server_mechanisms}"
            )

        initial_response = b""
        if mechanism == MECHANISM_PLAIN:
            initial_response = build_plain_initial_response(self._parameters.user, self._parameters.password)
        self._write_sasl(SaslInit(mechanism=mechanism, initial_response=initial_response))

        while True:
            body = self._read_sasl_frame()
            if isinstance(body, SaslChallenge):
                self._write_sasl(SaslResponse(response=b""))
                continue
            if not isinstance(body, SaslOutcome):
                raise ProtocolError(f"expected sasl-outcome, got {type(body).__name__}")
            if not body.succeeded:
                raise AuthenticationError(f"SASL {mechanism} rejected: {body.describe()}")
            return

    def _negotiate_open(self) -> Open:
        """Re-enter the AMQP layer and exchange ``open`` performatives."""
        write_protocol_header(self._socket, AMQP_PROTOCOL_HEADER)
        header = read_protocol_header(self._socket)
        if header != AMQP_PROTOCOL_HEADER:
            raise ProtocolError(f"broker refused the AMQP layer and offered {header!r} instead")

        parameters = self._parameters
        local_open = Open(
            container_id=parameters.container_id,
            hostname=parameters.open_hostname,
            max_frame_size=parameters.max_frame_size,
            channel_max=parameters.channel_max,
            idle_time_out=parameters.idle_timeout or None,
        )
        write_frame(self._socket, FRAME_TYPE_AMQP, 0, local_open.encode())

        while True:
            frame_type, _channel, body = read_frame(self._socket)
            if frame_type != FRAME_TYPE_AMQP:
                raise ProtocolError(f"expected an AMQP frame while opening, got frame type 0x{frame_type:02x}")
            performative, _payload = decode_frame_body(frame_type, body)
            if performative is None:
                continue
            if isinstance(performative, Open):
                return performative
            if isinstance(performative, Close):
                raise _close_failure(performative, "broker closed the connection instead of replying to open")
            raise ProtocolError(f"expected open, got {type(performative).__name__}")

    def _read_sasl_frame(self) -> SaslMechanisms | SaslInit | SaslChallenge | SaslResponse | SaslOutcome:
        """Read one SASL frame body, skipping empty frames."""
        while True:
            frame_type, _channel, body = read_frame(self._socket)
            if frame_type != FRAME_TYPE_SASL:
                raise ProtocolError(f"expected a SASL frame, got frame type 0x{frame_type:02x}")
            frame_body, _payload = decode_frame_body(frame_type, body)
            if frame_body is None:
                continue
            if isinstance(frame_body, (SaslMechanisms, SaslInit, SaslChallenge, SaslResponse, SaslOutcome)):
                return frame_body
            raise ProtocolError(f"expected a SASL frame body, got {type(frame_body).__name__}")

    def _write_sasl(self, body: SaslInit | SaslResponse) -> None:
        """Write one SASL frame on channel 0."""
        write_frame(self._socket, FRAME_TYPE_SASL, 0, body.encode())

    # --- background threads --------------------------------------------

    def _start_threads(self) -> None:
        """Start the frame reader and, when the peer wants one, the heartbeat writer."""
        self._reader = threading.Thread(target=self._read_frames, name="amqp-frame-reader", daemon=True)
        self._reader.start()
        interval = self._heartbeat_interval()
        if interval is not None:
            self._heartbeat = threading.Thread(
                target=self._write_heartbeats,
                args=(interval, self._transport_epoch),
                name="amqp-heartbeat",
                daemon=True,
            )
            self._heartbeat.start()

    def _heartbeat_interval(self) -> float | None:
        """Seconds between empty frames, or ``None`` when the peer wants no heartbeat."""
        if self._remote_idle_timeout <= 0:
            return None
        return max(MIN_HEARTBEAT_INTERVAL_SECONDS, self._remote_idle_timeout / 2000.0)

    def _write_heartbeats(self, interval: float, epoch: int) -> None:
        """Send an empty frame every ``interval`` seconds until shutdown.

        Args:
            interval: Seconds between empty frames.
            epoch: Which transport this writer belongs to; it stops as soon as a
                reconnect has replaced that transport, leaving the heartbeat to
                the writer the new one started.
        """
        while not self._shutdown.wait(interval):
            if epoch != self._transport_epoch:
                return
            try:
                with self._write_lock:
                    self._socket.sendall(EMPTY_FRAME)
            except OSError as error:
                self._logger.debug("heartbeat write failed: %s", error)
                return

    def _read_frames(self) -> None:
        """Read and dispatch frames until shutdown, EOF, or a protocol failure."""
        failure: BaseException | None = None
        try:
            while not self._shutdown.is_set():
                frame_type, channel, body = read_frame(self._socket)
                frame_body, payload = decode_frame_body(frame_type, body)
                if frame_body is None:
                    continue
                if isinstance(frame_body, (SaslMechanisms, SaslInit, SaslChallenge, SaslResponse, SaslOutcome)):
                    raise ProtocolError(f"unexpected {type(frame_body).__name__} on an open connection")
                self._dispatch(channel, frame_body, payload)
        except BaseException as error:  # noqa: BLE001 - the reader must report, never propagate
            failure = error
        self._inbound_finished.set()
        self._reader_finished(failure)

    def _reader_finished(self, failure: BaseException | None) -> None:
        """Decide whether the reader's exit was expected, and report it if not."""
        if self._close_requested.is_set():
            self._logger.debug("frame reader stopped during a local close")
            return
        if failure is None and self._remote_close is not None:
            failure = _close_failure_or_none(self._remote_close)
        self._handle_unexpected_close(failure)

    def _handle_unexpected_close(self, error: BaseException | None) -> None:
        """React to a dead transport: recover it, or mark the connection dead.

        Everything that notices the transport died funnels through here. With
        recovery activated (step_040 §3.1 point 3) the connection enters
        ``RECONNECTING`` and a dedicated thread redials it, so
        ``on_unexpected_close`` is deferred and only ever fires if that loop
        gives up. Otherwise the pre-recovery behavior applies unchanged: the
        connection is dead and the callback fires once.

        Args:
            error: What killed the connection, or ``None`` when the peer closed
                cleanly without an ``error``.
        """
        with self._state_lock:
            if self._unexpected_close_notified or self._state is ConnectionState.RECONNECTING:
                return
            recovering = self._recovery.activated and not self._close_requested.is_set()
            if recovering:
                self._state = ConnectionState.RECONNECTING
            else:
                self._unexpected_close_notified = True
                self._state = ConnectionState.CLOSED
        self._logger.warning("connection to %s closed unexpectedly: %s", self._parameters.host, error)
        self._abandon_transport(error)
        if not recovering:
            self._notify_unexpected_close(error)
            return
        self._recovery_thread = threading.Thread(
            target=self._recover,
            args=(error,),
            name="amqp-recovery",
            daemon=True,
        )
        self._recovery_thread.start()

    def _abandon_transport(self, error: BaseException | None) -> None:
        """Close the dead socket and fail every session and link that rode on it."""
        self._shutdown.set()
        self._close_socket()
        with self._state_lock:
            sessions = list(self._sessions.values())
        lost = error if error is not None else ProtocolError("the broker closed the connection")
        for session in sessions:
            session.transport_lost(lost)

    def _notify_unexpected_close(self, error: BaseException | None) -> None:
        """Invoke ``on_unexpected_close``, absorbing whatever it raises."""
        callback = self._parameters.on_unexpected_close
        if callback is None:
            return
        try:
            callback(error)
        except Exception:
            self._logger.exception("on_unexpected_close callback raised")

    # --- recovery loop (step_040 §3) ------------------------------------

    def _recover(self, error: BaseException | None) -> None:
        """Run the recovery loop, refusing to die quietly (step_040 §3.2).

        A recovery thread that vanished on an unexpected failure would leave the
        connection stuck in ``RECONNECTING`` with nothing left to move it, so
        anything the loop itself did not handle ends as a give-up instead.

        Args:
            error: What killed the transport, reported to
                ``on_unexpected_close`` if the loop ends up giving up.
        """
        try:
            self._recovery_loop(error)
        except BaseException as failure:  # noqa: BLE001 - the thread must not exit silently
            self._logger.exception("the recovery loop failed unexpectedly")
            self._give_up_recovering(failure, self._recovery.back_off_delay_policy.current_attempt)

    def _recovery_loop(self, error: BaseException | None) -> None:
        """Wait, redial, and rebuild, until it works or the policy runs out.

        Waits out the back-off policy's delay before each attempt, so a broker
        that is still down is not hammered, and gives up as soon as the policy
        stops being active — or as soon as :meth:`close` cancels the loop.

        Args:
            error: What killed the transport, reported to
                ``on_unexpected_close`` if the loop ends up giving up.
        """
        policy = self._recovery.back_off_delay_policy
        while not self._recovery_stopped():
            delay = policy.next_delay()
            if self._recovery_cancelled.wait(delay):
                self._logger.debug("recovery cancelled while waiting to redial")
                return
            if not policy.is_active():
                self._give_up_recovering(error, policy.current_attempt)
                return
            if self._recovery_stopped():
                return
            if not self._redial(policy.current_attempt):
                continue
            policy.reset()
            self._recover_endpoints()
            return

    def _recovery_stopped(self) -> bool:
        """Whether :meth:`close` asked the recovery loop to stop."""
        return self._recovery_cancelled.is_set() or self._close_requested.is_set()

    def _redial(self, attempt: int) -> bool:
        """Run one whole bootstrap against the original parameters (step_040 §3.2 point 3).

        Args:
            attempt: The policy's attempt number, for logging only.

        Returns:
            Whether the connection is back up, with its frame reader running.
        """
        self._logger.debug(
            "reconnect attempt %d to %s:%d", attempt, self._parameters.host, self._parameters.resolved_port
        )
        try:
            sock = _connect_socket(self._parameters)
        except OSError as error:
            self._logger.warning("reconnect attempt %d could not dial the broker: %s", attempt, error)
            return False
        self._reset_for_new_transport(sock)
        try:
            self._bootstrap()
        except BaseException as error:  # noqa: BLE001 - any bootstrap failure is just a failed attempt
            self._logger.warning("reconnect attempt %d failed to bootstrap: %s", attempt, error)
            self._close_socket()
            return False
        # Only now may callers write again: a frame squeezed in during the
        # handshake above would land in the middle of it.
        self._shutdown.clear()
        self._start_threads()
        self._logger.info("reconnected to %s:%d", self._parameters.host, self._parameters.resolved_port)
        return True

    def _reset_for_new_transport(self, sock: socket.socket) -> None:
        """Adopt ``sock`` and forget everything the dead transport owned.

        Sessions are unregistered rather than ended: their channels only ever
        meant anything on the socket that is already gone, and the fresh one
        starts numbering from zero again. ``_shutdown`` stays set until the
        handshake is through, so nothing writes into the middle of it.

        Args:
            sock: The freshly dialed socket.
        """
        with self._state_lock:
            self._sessions.clear()
            self._sessions_by_remote_channel.clear()
            self._next_channel = 0
            self._transport_epoch += 1
        with self._pub_sub_lock:
            self._shared_session = None
        self._remote_close = None
        self._inbound_finished.clear()
        self._socket = sock

    def _recover_endpoints(self) -> None:
        """Re-attach management, replay topology, re-attach links (step_040 §3.3).

        Every step is best-effort per object: one management pair, publisher or
        consumer that cannot be re-attached is logged and left detached, which
        neither restarts the retry loop nor stops the others from recovering.
        """
        if self._recover_management() and self._recovery.topology:
            management = self._management
            if management is not None:
                self._topology_listener.replay(management)
        self._recover_publishers_and_consumers()
        with self._state_lock:
            if self._close_requested.is_set() or self._recovery_cancelled.is_set():
                return
            self._state = ConnectionState.OPEN
        self._logger.info("connection to %s recovered", self._parameters.host)

    def _recover_management(self) -> bool:
        """Re-open the management link pair, if one was ever opened (§3.3 point 2.1).

        The endpoint's lock is held throughout, so a caller racing this with
        :meth:`management` cannot see the half-re-opened pair as closed and
        replace it with a second endpoint.

        Returns:
            Whether a management endpoint is now usable — ``False`` both when
            none was ever created and when re-opening it failed, since neither
            can replay any topology.
        """
        with self._management_lock:
            management = self._management
            if management is None:
                return False
            try:
                management._reopen()
            except Exception as error:  # a failed pair must not abort the rest of recovery
                self._logger.warning("could not re-open the management link pair: %s", error)
                return False
        return True

    def _recover_publishers_and_consumers(self) -> None:
        """Re-attach every still-tracked publisher and consumer (§3.3 point 2.3)."""
        with self._publishers_lock:
            publishers = list(self._publishers.values())
        with self._consumers_lock:
            consumers = list(self._consumers.values())
        if not publishers and not consumers:
            return
        try:
            session = self._pub_sub_session()
        except AMQPError as error:
            self._logger.warning("could not re-open the pub/sub session, links stay detached: %s", error)
            return
        for publisher in publishers:
            try:
                publisher._reattach(session)
            except Exception as error:  # one link must not abort the rest (§3.3 point 2.4)
                self._logger.warning("could not re-attach publisher %r: %s", publisher.id, error)
        for consumer in consumers:
            try:
                consumer._reattach(session)
            except Exception as error:
                self._logger.warning("could not re-attach consumer %r: %s", consumer.id, error)

    def _give_up_recovering(self, error: BaseException | None, attempts: int) -> None:
        """Abandon recovery: the connection is dead for good (step_040 §3.4).

        ``on_unexpected_close`` fires here, with the failure that started the
        recovery — a caller that never enabled recovery and one whose recovery
        gave up both see it exactly once.

        Args:
            error: What killed the transport in the first place.
            attempts: How many attempts the policy had made, for logging.
        """
        with self._state_lock:
            if self._unexpected_close_notified:
                return
            self._unexpected_close_notified = True
            self._state = ConnectionState.CLOSED
        self._shutdown.set()
        self._logger.warning("giving up reconnecting to %s after %d attempt(s)", self._parameters.host, attempts)
        self._notify_unexpected_close(error)

    def _cancel_recovery(self) -> None:
        """Stop the recovery loop and wait, briefly, for it to notice (step_040 §4).

        Called with the close lock held, from :meth:`close`. The loop's back-off
        wait is interruptible, so the common case returns immediately; a loop
        caught mid-redial is left to unwind on its own rather than blocking the
        close on the network.
        """
        self._recovery_cancelled.set()
        thread = self._recovery_thread
        if thread is None or thread is threading.current_thread():
            return
        try:
            thread.join(THREAD_JOIN_TIMEOUT_SECONDS)
        except RuntimeError:
            # Registered but not started yet, so it has not run a single step;
            # the cancellation set above already makes its first check bail out.
            return
        if thread.is_alive():
            self._logger.warning("the recovery loop did not stop within %.1fs", THREAD_JOIN_TIMEOUT_SECONDS)

    # --- inbound dispatch ----------------------------------------------

    def _dispatch(self, channel: int, performative: Performative, payload: bytes) -> None:
        """Route one inbound performative to the connection or to a session."""
        if isinstance(performative, Close):
            self._on_remote_close(performative)
            return
        if isinstance(performative, Open):
            self._logger.warning("ignoring a second open performative from the broker")
            return
        session = self._session_for(channel, performative)
        if session is None:
            self._logger.warning("dropping %s received on unknown channel %d", type(performative).__name__, channel)
            return
        session.handle_frame(performative, payload)

    def _session_for(self, channel: int, performative: Performative) -> Session | None:
        """Find the session a frame belongs to, learning the peer's channel from ``begin``."""
        with self._state_lock:
            if isinstance(performative, Begin):
                local_channel = performative.remote_channel if performative.remote_channel is not None else channel
                session = self._sessions.get(local_channel)
                if session is not None:
                    self._sessions_by_remote_channel[channel] = session
                return session
            session = self._sessions_by_remote_channel.get(channel)
            if session is not None:
                return session
            return self._sessions.get(channel)

    def _on_remote_close(self, performative: Close) -> None:
        """Handle the peer's ``close``: echo it when we did not ask for it."""
        self._remote_close = performative
        if not self._close_requested.is_set():
            try:
                self.send_frame(0, Close())
            except AMQPError as error:
                self._logger.debug("could not echo the broker's close: %s", error)
        self._shutdown.set()
        self._inbound_finished.set()

    # --- teardown helpers ----------------------------------------------

    def _close_management(self) -> None:
        """Detach the management link pair, if one was ever opened.

        Runs before :meth:`_end_sessions` so the pair is detached properly and
        its response pump stopped, rather than being torn down implicitly by the
        session's ``end``.
        """
        with self._management_lock:
            management, self._management = self._management, None
        if management is None:
            return
        try:
            management.close()
        except Exception as error:  # teardown continues even if the pair misbehaves
            self._logger.warning("ignoring error while closing the management link pair: %s", error)

    def _close_publishers(self) -> None:
        """Detach every still-open publisher's sender link.

        Runs after :meth:`_close_management` and before :meth:`_end_sessions`, so
        each link is detached properly rather than being torn down implicitly by
        the pub/sub session's ``end``.
        """
        with self._publishers_lock:
            publishers = list(self._publishers.values())
            self._publishers.clear()
        for publisher in publishers:
            try:
                publisher.close()
            except Exception as error:  # teardown continues even if one publisher misbehaves
                self._logger.warning("ignoring error while closing publisher %r: %s", publisher.id, error)

    def _close_consumers(self) -> None:
        """Stop every still-open consumer's delivery loop and detach its link.

        Runs after :meth:`_close_publishers` and before :meth:`_end_sessions`, so
        each link is detached properly rather than being torn down implicitly by
        the pub/sub session's ``end``.
        """
        with self._consumers_lock:
            consumers = list(self._consumers.values())
            self._consumers.clear()
        for consumer in consumers:
            try:
                consumer.close()
            except Exception as error:  # teardown continues even if one consumer misbehaves
                self._logger.warning("ignoring error while closing consumer %r: %s", consumer.id, error)

    def _release_pub_sub_session(self) -> None:
        """Forget the shared pub/sub session, once everything on it is detached.

        The session object itself stays registered, so :meth:`_end_sessions` ends
        it like any other.
        """
        with self._pub_sub_lock:
            self._shared_session = None

    def _end_sessions(self) -> None:
        """End every registered session, logging rather than raising failures."""
        with self._state_lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            try:
                session.end()
            except Exception as error:  # teardown continues even if one session misbehaves
                self._logger.warning("ignoring error while ending session on channel %s: %s", session.channel, error)

    def _send_close(self, error: Error | None) -> None:
        """Send the connection-level ``close``, logging rather than raising failures."""
        try:
            self.send_frame(0, Close(error=error))
        except AMQPError as failure:
            self._logger.debug("could not send close: %s", failure)

    def _close_socket(self) -> None:
        """Shut down and close the socket, ignoring errors from an already-dead one."""
        with contextlib.suppress(OSError):
            self._socket.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            self._socket.close()

    def _join_threads(self) -> None:
        """Join the background threads, unless we are running on one of them."""
        current = threading.current_thread()
        for thread in (self._reader, self._heartbeat):
            if thread is not None and thread is not current:
                thread.join(THREAD_JOIN_TIMEOUT_SECONDS)


def _close_failure(performative: Close, prefix: str) -> ProtocolError | AuthenticationError:
    """Turn a peer ``close`` into the exception that best describes it."""
    error = performative.error
    if error is None:
        return ProtocolError(f"{prefix} (no error given)")
    detail = f"{prefix}: {error.condition}"
    if error.description:
        detail = f"{detail}: {error.description}"
    if error.condition == UNAUTHORIZED_ACCESS_CONDITION:
        return AuthenticationError(detail)
    return ProtocolError(detail)


def _close_failure_or_none(performative: Close) -> BaseException | None:
    """Describe a peer ``close``, or ``None`` when it carried no ``error``."""
    if performative.error is None:
        return None
    return _close_failure(performative, "broker closed the connection")


__all__ = [
    "CLOSE_TIMEOUT_SECONDS",
    "CONTAINER_ID_PREFIX",
    "Connection",
    "ConnectionParameters",
    "ConnectionState",
    "UnexpectedCloseCallback",
]
