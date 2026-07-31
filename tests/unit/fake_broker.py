"""A scripted AMQP 1.0 peer, driven over one end of a :func:`socket.socketpair`.

The real :class:`~src.Connection` talks to this exactly
as it talks to RabbitMQ — protocol headers, SASL frames, ``open``, then whatever
performatives the test drives — so unit tests exercise the actual bootstrap and
frame-dispatch code paths instead of a stub.
"""

from __future__ import annotations

import contextlib
import queue
import socket
import threading

from src.wire import (
    AMQP_PROTOCOL_HEADER,
    AMQP_SASL_HEADER,
    FRAME_TYPE_AMQP,
    FRAME_TYPE_SASL,
    Accepted,
    Attach,
    Begin,
    Close,
    Detach,
    Disposition,
    End,
    Error,
    Flow,
    Open,
    Performative,
    SaslInit,
    SaslMechanisms,
    SaslOutcome,
    Source,
    Target,
    Transfer,
    decode_frame_body,
    read_frame,
    read_protocol_header,
    write_frame,
    write_protocol_header,
)

DEFAULT_WINDOW = 1000


class FakeBroker:
    """Answers a client's handshake and, optionally, its session/link frames."""

    def __init__(
        self,
        sock,
        *,
        mechanisms=("PLAIN", "ANONYMOUS"),
        outcome_code=0,
        container_id="fake-broker",
        max_frame_size=1024 * 1024,
        channel_max=64,
        idle_timeout=None,
        handle_max=255,
        auto_respond=True,
        refuse_attach=False,
        refusal_sends_attach=True,
        refusal_condition="amqp:not-found",
        initial_credit=0,
        auto_settle=True,
        sasl_layer=True,
        receiver_flow_properties=None,
    ):
        self._sock = sock
        self._sock.settimeout(5.0)
        self.mechanisms = list(mechanisms)
        self.outcome_code = outcome_code
        self.container_id = container_id
        self.max_frame_size = max_frame_size
        self.channel_max = channel_max
        self.idle_timeout = idle_timeout
        self.handle_max = handle_max
        self.auto_respond = auto_respond
        self.refuse_attach = refuse_attach
        self.refusal_sends_attach = refusal_sends_attach
        self.refusal_condition = refusal_condition
        self.initial_credit = initial_credit
        self.auto_settle = auto_settle
        self.sasl_layer = sasl_layer
        self.receiver_flow_properties = receiver_flow_properties

        self.sasl_init: SaslInit | None = None
        self.remote_open: Open | None = None
        self.received: queue.Queue = queue.Queue()
        self.errors: list[BaseException] = []
        self.handshake_done = threading.Event()
        self._write_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="fake-broker", daemon=True)

    # --- lifecycle ------------------------------------------------------

    def start(self) -> FakeBroker:
        """Start serving in the background."""
        self._thread.start()
        return self

    def stop(self) -> None:
        """Stop serving and close this end of the socket pair."""
        self._stop.set()
        with contextlib.suppress(OSError):
            self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()
        self._thread.join(0.5)

    def drop_connection(self) -> None:
        """Close the socket abruptly, without any ``close`` performative."""
        self._stop.set()
        self._sock.close()

    # --- writing --------------------------------------------------------

    def send(self, channel: int, performative: Performative, payload: bytes = b"") -> None:
        """Write one AMQP frame to the client."""
        with self._write_lock:
            write_frame(self._sock, FRAME_TYPE_AMQP, channel, performative.encode() + payload)

    def send_sasl(self, body) -> None:
        """Write one SASL frame to the client."""
        with self._write_lock:
            write_frame(self._sock, FRAME_TYPE_SASL, 0, body.encode())

    def send_transfer(
        self,
        channel: int,
        handle: int,
        payload: bytes,
        *,
        delivery_id: int | None = 0,
        delivery_tag: bytes | None = b"broker-tag",
        more: bool = False,
        settled: bool = False,
        aborted: bool = False,
    ) -> None:
        """Write one ``transfer`` frame to the client."""
        self.send(
            channel,
            Transfer(
                handle=handle,
                delivery_id=delivery_id,
                delivery_tag=delivery_tag,
                settled=settled,
                more=more,
                aborted=aborted,
            ),
            payload,
        )

    # --- reading --------------------------------------------------------

    def wait_for(self, performative_type, timeout: float = 5.0):
        """Return the first received performative of ``performative_type``.

        Raises:
            AssertionError: If none arrives within ``timeout``.
        """
        deadline = timeout
        seen = []
        while deadline > 0:
            try:
                channel, performative, payload = self.received.get(timeout=min(0.2, deadline))
            except queue.Empty:
                deadline -= 0.2
                continue
            seen.append(performative)
            if isinstance(performative, performative_type):
                return channel, performative, payload
        raise AssertionError(f"no {performative_type.__name__} received; saw {[type(p).__name__ for p in seen]}")

    def all_received(self, performative_type):
        """Return every already-received performative of ``performative_type``."""
        collected = []
        while True:
            try:
                _channel, performative, _payload = self.received.get_nowait()
            except queue.Empty:
                return collected
            if isinstance(performative, performative_type):
                collected.append(performative)

    # --- internals ------------------------------------------------------

    def _run(self) -> None:
        try:
            self._handshake()
            self.handshake_done.set()
            self._serve()
        except BaseException as error:  # surfaced by tests through .errors
            self.errors.append(error)
            self.handshake_done.set()

    def _handshake(self) -> None:
        header = read_protocol_header(self._sock)
        if self.sasl_layer:
            assert header == AMQP_SASL_HEADER, header
            write_protocol_header(self._sock, AMQP_SASL_HEADER)
            self.send_sasl(SaslMechanisms(server_mechanisms=self.mechanisms))
            body = self._read_frame_body(FRAME_TYPE_SASL)
            assert isinstance(body, SaslInit), body
            self.sasl_init = body
            self.send_sasl(SaslOutcome(code=self.outcome_code))
            if self.outcome_code != 0:
                return
            header = read_protocol_header(self._sock)
        else:
            write_protocol_header(self._sock, AMQP_PROTOCOL_HEADER)
            return
        assert header == AMQP_PROTOCOL_HEADER, header
        write_protocol_header(self._sock, AMQP_PROTOCOL_HEADER)
        performative = self._read_frame_body(FRAME_TYPE_AMQP)
        assert isinstance(performative, Open), performative
        self.remote_open = performative
        self.send(
            0,
            Open(
                container_id=self.container_id,
                max_frame_size=self.max_frame_size,
                channel_max=self.channel_max,
                idle_time_out=self.idle_timeout,
            ),
        )

    def _read_frame_body(self, expected_type: int):
        while True:
            frame_type, _channel, body = read_frame(self._sock)
            assert frame_type == expected_type, frame_type
            performative, _payload = decode_frame_body(frame_type, body)
            if performative is not None:
                return performative

    def _serve(self) -> None:
        while not self._stop.is_set():
            frame_type, channel, body = read_frame(self._sock)
            performative, payload = decode_frame_body(frame_type, body)
            if performative is None:
                continue
            self.received.put((channel, performative, payload))
            if self.auto_respond:
                self._respond(channel, performative)

    def _respond(self, channel: int, performative) -> None:
        if isinstance(performative, Begin):
            self.send(
                channel,
                Begin(
                    remote_channel=channel,
                    next_outgoing_id=0,
                    incoming_window=DEFAULT_WINDOW,
                    outgoing_window=DEFAULT_WINDOW,
                    handle_max=self.handle_max,
                ),
            )
        elif isinstance(performative, Attach):
            self._respond_to_attach(channel, performative)
        elif isinstance(performative, Detach):
            self.send(channel, Detach(handle=performative.handle, closed=True))
        elif isinstance(performative, End):
            self.send(channel, End())
        elif isinstance(performative, Close):
            self.send(0, Close())
            self._stop.set()
        elif isinstance(performative, Transfer):
            self._respond_to_transfer(channel, performative)

    def _respond_to_attach(self, channel: int, performative: Attach) -> None:
        client_is_sender = performative.role is False
        if self.refuse_attach:
            if self.refusal_sends_attach:
                self.send(
                    channel,
                    Attach(
                        name=performative.name,
                        handle=performative.handle,
                        role=not performative.role,
                        source=None,
                        target=None,
                        initial_delivery_count=None if client_is_sender else 0,
                    ),
                )
            self.send(
                channel,
                Detach(
                    handle=performative.handle,
                    closed=True,
                    error=Error(condition=self.refusal_condition, description="the broker refused this link"),
                ),
            )
            return
        self.send(
            channel,
            Attach(
                name=performative.name,
                handle=performative.handle,
                role=not performative.role,
                source=performative.source if performative.source is not None else Source(address="broker-source"),
                target=performative.target if performative.target is not None else Target(address="broker-target"),
                initial_delivery_count=None if client_is_sender else 0,
            ),
        )
        if client_is_sender and self.initial_credit:
            self.grant_credit(channel, performative.handle, self.initial_credit)
        if not client_is_sender and self.receiver_flow_properties is not None:
            # Sent straight after the attach reply, as RabbitMQ does for a
            # single-active-consumer quorum queue: it may reach the client before
            # it has finished building its Consumer (step_090 §3).
            self.grant_credit(channel, performative.handle, 0, properties=self.receiver_flow_properties)

    def grant_credit(self, channel: int, handle: int, link_credit: int, *, properties=None) -> None:
        """Send a link ``flow`` granting ``link_credit`` to the client."""
        self.send(
            channel,
            Flow(
                incoming_window=DEFAULT_WINDOW,
                next_outgoing_id=0,
                outgoing_window=DEFAULT_WINDOW,
                next_incoming_id=0,
                handle=handle,
                delivery_count=0,
                link_credit=link_credit,
                properties=properties,
            ),
        )

    def settle(self, channel: int, delivery_id: int, state=None, *, last: int | None = None) -> None:
        """Send a ``disposition`` settling one delivery-id, or a range, with ``state``.

        Args:
            channel: Channel the client's session lives on.
            delivery_id: First delivery-id the disposition applies to.
            state: Delivery state to report; ``Accepted()`` when omitted.
            last: Last delivery-id of the range; defaults to ``delivery_id``.
        """
        self.send(
            channel,
            Disposition(
                role=True,
                first=delivery_id,
                last=delivery_id if last is None else last,
                settled=True,
                state=state if state is not None else Accepted(),
            ),
        )

    def is_alive(self) -> bool:
        """Whether this broker is still serving."""
        return self._thread.is_alive() and not self._stop.is_set()

    def _respond_to_transfer(self, channel: int, performative: Transfer) -> None:
        if not self.auto_settle or performative.more or performative.delivery_id is None:
            return
        if performative.settled:
            return
        self.send(
            channel,
            Disposition(
                role=True,
                first=performative.delivery_id,
                last=performative.delivery_id,
                settled=True,
                state=Accepted(),
            ),
        )


class BrokerFarm:
    """Answers a client's *successive* dials, one scripted broker per dial.

    Stands in for :func:`~src.connection._connect_socket`
    so a test can drive auto-reconnection: the first dial is the ``Connection``'s
    own bootstrap, every later one is a redial from the recovery loop. Each dial
    gets its own socket pair and its own :class:`FakeBroker`, so the client really
    does perform a whole new handshake instead of reusing a dead peer.

    Example:
        >>> farm = BrokerFarm()
        >>> connection = Connection(ConnectionParameters())  # dial 1
        >>> farm.refuse_next()                               # the first redial fails
        >>> farm.latest.drop_connection()
    """

    def __init__(self, **broker_kwargs):
        """Answer every dial with a broker built from ``broker_kwargs``."""
        self.broker_kwargs = dict(broker_kwargs)
        self.brokers: list[FakeBroker] = []
        self.dials = 0
        self._sockets: list[socket.socket] = []
        self._refusals = 0
        self._next_kwargs: dict | None = None
        self._lock = threading.Lock()

    # --- scripting ------------------------------------------------------

    def refuse_next(self, count: int = 1) -> None:
        """Make the next ``count`` dials fail as a refused TCP connection."""
        with self._lock:
            self._refusals += count

    def configure_next(self, **broker_kwargs) -> None:
        """Build only the next broker with these overrides."""
        with self._lock:
            self._next_kwargs = dict(broker_kwargs)

    @property
    def latest(self) -> FakeBroker:
        """The broker that answered the most recent dial."""
        return self.brokers[-1]

    # --- the _connect_socket replacement --------------------------------

    def dial(self, _parameters=None) -> socket.socket:
        """Return the client end of a fresh socket pair, or refuse the dial.

        Raises:
            ConnectionRefusedError: While :meth:`refuse_next` refusals remain, so
                the recovery loop sees exactly what a down broker looks like.
        """
        with self._lock:
            self.dials += 1
            if self._refusals > 0:
                self._refusals -= 1
                raise ConnectionRefusedError(f"dial {self.dials} refused by the broker farm")
            kwargs = {**self.broker_kwargs, **(self._next_kwargs or {})}
            self._next_kwargs = None
        client_side, broker_side = socket.socketpair()
        client_side.settimeout(5.0)
        with self._lock:
            self._sockets.extend((client_side, broker_side))
            broker = FakeBroker(broker_side, **kwargs)
            self.brokers.append(broker)
        broker.start()
        return client_side

    def close(self) -> None:
        """Stop every broker and close every socket this farm handed out."""
        for broker in self.brokers:
            broker.auto_respond = True
            with contextlib.suppress(OSError):
                broker.stop()
        for sock in self._sockets:
            with contextlib.suppress(OSError):
                sock.close()
