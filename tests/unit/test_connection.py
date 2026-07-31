"""Unit tests for the connection bootstrap, frame reader and teardown."""

from __future__ import annotations

import threading

import pytest

from src.connection import (
    Connection,
    ConnectionParameters,
    ConnectionState,
)
from src.exceptions import AuthenticationError, ProtocolError
from src.wire import (
    MECHANISM_ANONYMOUS,
    MECHANISM_PLAIN,
    Begin,
    Close,
    Error,
    Flow,
    Open,
)


class TestConnectionParameters:
    def test_defaults(self):
        parameters = ConnectionParameters()
        assert parameters.host == "localhost"
        assert parameters.resolved_port == 5672
        assert parameters.virtual_host == "/"
        assert parameters.user == "guest"
        assert parameters.password == "guest"
        assert parameters.container_id.startswith("rabbitmq-amqp-python-client-")
        assert parameters.max_frame_size == 1024 * 1024
        assert parameters.channel_max == 65535
        assert parameters.idle_timeout == 0
        assert parameters.on_unexpected_close is None

    def test_tls_changes_the_default_port(self):
        import ssl

        assert ConnectionParameters(tls=ssl.create_default_context()).resolved_port == 5671

    def test_explicit_port_wins(self):
        assert ConnectionParameters(port=15672, host="h").resolved_port == 15672

    def test_generated_container_ids_are_unique(self):
        assert ConnectionParameters().container_id != ConnectionParameters().container_id

    def test_given_container_id_is_kept(self):
        assert ConnectionParameters(container_id="mine").container_id == "mine"

    def test_default_vhost_leaves_hostname_unset(self):
        assert ConnectionParameters().open_hostname is None

    def test_non_default_vhost_is_encoded(self):
        assert ConnectionParameters(virtual_host="tenant-1").open_hostname == "vhost:tenant-1"

    def test_plain_is_selected_for_the_default_credentials(self):
        assert ConnectionParameters().sasl_mechanism == MECHANISM_PLAIN

    def test_anonymous_only_when_both_credentials_are_empty(self):
        assert ConnectionParameters(user="", password="").sasl_mechanism == MECHANISM_ANONYMOUS
        assert ConnectionParameters(user="u", password="").sasl_mechanism == MECHANISM_PLAIN
        assert ConnectionParameters(user="", password="p").sasl_mechanism == MECHANISM_PLAIN


class TestBootstrap:
    def test_sends_plain_sasl_init_with_the_credentials(self, connect):
        broker, connection = connect(user="alice", password="s3cret")
        assert connection.state is ConnectionState.OPEN
        assert broker.sasl_init is not None
        assert broker.sasl_init.mechanism == MECHANISM_PLAIN
        assert broker.sasl_init.initial_response == b"\x00alice\x00s3cret"

    def test_sends_anonymous_when_no_credentials_are_given(self, connect):
        broker, _connection = connect(user="", password="")
        assert broker.sasl_init.mechanism == MECHANISM_ANONYMOUS

    def test_open_carries_the_declared_parameters(self, connect):
        broker, _connection = connect(
            container_id="tester", max_frame_size=64 * 1024, channel_max=17, idle_timeout=9000
        )
        assert broker.remote_open == Open(
            container_id="tester",
            hostname=None,
            max_frame_size=64 * 1024,
            channel_max=17,
            idle_time_out=9000,
        )

    def test_open_encodes_a_non_default_virtual_host(self, connect):
        broker, _connection = connect(virtual_host="tenant-1")
        assert broker.remote_open.hostname == "vhost:tenant-1"

    def test_idle_timeout_zero_is_sent_as_absent(self, connect):
        broker, _connection = connect(idle_timeout=0)
        assert broker.remote_open.idle_time_out is None

    def test_negotiated_limits_are_the_minimum_of_both_peers(self, connect):
        _broker, connection = connect(
            max_frame_size=1024 * 1024,
            channel_max=65535,
            broker_kwargs={"max_frame_size": 131072, "channel_max": 63},
        )
        assert connection.max_frame_size == 131072
        assert connection.channel_max == 63

    def test_remote_open_is_exposed(self, connect):
        _broker, connection = connect(broker_kwargs={"container_id": "rabbit@test"})
        assert connection.remote_open.container_id == "rabbit@test"

    def test_rejected_credentials_raise_authentication_error(self, connect):
        with pytest.raises(AuthenticationError, match="bad credentials"):
            connect(broker_kwargs={"outcome_code": 1})

    def test_unsupported_mechanism_raises_authentication_error(self, connect):
        with pytest.raises(AuthenticationError, match="does not offer SASL PLAIN"):
            connect(broker_kwargs={"mechanisms": ("EXTERNAL",)})

    def test_refused_sasl_layer_raises_protocol_error(self, connect):
        with pytest.raises(ProtocolError, match="refused the SASL layer"):
            connect(broker_kwargs={"sasl_layer": False})

    def test_a_failed_bootstrap_leaves_no_open_socket(self, broker_factory, monkeypatch):
        from src import connection as connection_module

        broker = broker_factory(outcome_code=1)
        captured = {}
        original = connection_module._connect_socket

        def spy(parameters):
            captured["socket"] = original(parameters)
            return captured["socket"]

        monkeypatch.setattr(connection_module, "_connect_socket", spy)
        with pytest.raises(AuthenticationError):
            Connection(ConnectionParameters())
        assert broker is not None
        assert captured["socket"].fileno() == -1


class TestSendFrame:
    def test_writes_on_the_requested_channel(self, connect):
        broker, connection = connect()
        connection.send_frame(4, Flow(incoming_window=1, next_outgoing_id=0, outgoing_window=1))
        channel, performative, _payload = broker.wait_for(Flow)
        assert channel == 4
        assert performative.incoming_window == 1

    def test_raises_once_the_connection_is_closed(self, connect):
        _broker, connection = connect()
        connection.close()
        with pytest.raises(ProtocolError, match="connection is closed"):
            connection.send_frame(0, Close())


class TestChannelAllocation:
    def test_allocates_sequential_channels(self, connect):
        _broker, connection = connect()
        session = _SessionDouble()
        assert connection.allocate_channel(session) == 0
        assert connection.allocate_channel(session) == 1

    def test_reuses_a_released_channel_only_after_wrapping(self, connect):
        _broker, connection = connect(broker_kwargs={"channel_max": 2})
        session = _SessionDouble()
        assert [connection.allocate_channel(session) for _ in range(3)] == [0, 1, 2]
        connection.release_channel(1)
        assert connection.allocate_channel(session) == 1

    def test_releasing_an_unknown_channel_is_a_no_op(self, connect):
        _broker, connection = connect()
        connection.release_channel(42)

    def test_refuses_to_allocate_after_close(self, connect):
        _broker, connection = connect()
        connection.close()
        with pytest.raises(ProtocolError, match="closed"):
            connection.allocate_channel(_SessionDouble())


class TestClose:
    def test_sends_close_and_reaches_the_closed_state(self, connect):
        broker, connection = connect()
        connection.close()
        broker.wait_for(Close)
        assert connection.state is ConnectionState.CLOSED

    def test_is_idempotent(self, connect):
        broker, connection = connect()
        connection.close()
        connection.close()
        connection.close()
        assert connection.state is ConnectionState.CLOSED
        assert len(broker.all_received(Close)) == 1

    def test_does_not_invoke_the_unexpected_close_callback(self, connect):
        calls = []
        _broker, connection = connect(on_unexpected_close=calls.append)
        connection.close()
        assert calls == []

    def test_joins_the_frame_reader(self, connect):
        _broker, connection = connect()
        connection.close()
        assert connection._reader is not None
        assert not connection._reader.is_alive()

    def test_ends_open_sessions_first(self, connect):
        from src.wire import End

        broker, connection = connect()
        connection.open_session()
        connection.close()
        assert len(broker.all_received(End)) == 1

    def test_completes_even_when_the_broker_never_replies(self, connect, monkeypatch):
        from src import connection as connection_module

        monkeypatch.setattr(connection_module, "CLOSE_TIMEOUT_SECONDS", 0.2)
        _broker, connection = connect(broker_kwargs={"auto_respond": False})
        connection.close()
        assert connection.state is ConnectionState.CLOSED


class TestUnexpectedClose:
    def _callback(self):
        fired = threading.Event()
        seen: list[BaseException | None] = []

        def callback(error):
            seen.append(error)
            fired.set()

        return callback, fired, seen

    def test_peer_close_with_error_fires_the_callback_once(self, connect):
        callback, fired, seen = self._callback()
        broker, connection = connect(on_unexpected_close=callback)
        broker.send(0, Close(error=Error(condition="amqp:connection:forced", description="go away")))
        assert fired.wait(5.0)
        assert len(seen) == 1
        assert isinstance(seen[0], ProtocolError)
        assert "amqp:connection:forced" in str(seen[0])
        assert "go away" in str(seen[0])
        assert connection.state is ConnectionState.CLOSED

    def test_peer_close_is_echoed(self, connect):
        callback, fired, _seen = self._callback()
        broker, _connection = connect(on_unexpected_close=callback)
        broker.send(0, Close())
        assert fired.wait(5.0)
        _channel, performative, _payload = broker.wait_for(Close)
        assert performative.error is None

    def test_clean_peer_close_reports_no_error(self, connect):
        callback, fired, seen = self._callback()
        broker, _connection = connect(on_unexpected_close=callback)
        broker.send(0, Close())
        assert fired.wait(5.0)
        assert seen == [None]

    def test_close_after_an_unexpected_close_does_not_fire_again(self, connect):
        callback, fired, seen = self._callback()
        broker, connection = connect(on_unexpected_close=callback)
        broker.send(0, Close(error=Error(condition="amqp:internal-error")))
        assert fired.wait(5.0)
        connection.close()
        connection.close()
        assert len(seen) == 1

    def test_socket_eof_fires_the_callback_with_an_error(self, connect):
        callback, fired, seen = self._callback()
        broker, connection = connect(on_unexpected_close=callback)
        broker.drop_connection()
        assert fired.wait(5.0)
        assert len(seen) == 1
        assert isinstance(seen[0], ProtocolError)
        assert connection.state is ConnectionState.CLOSED

    def test_a_raising_callback_does_not_break_the_reader(self, connect):
        fired = threading.Event()

        def callback(_error):
            fired.set()
            raise RuntimeError("boom")

        broker, connection = connect(on_unexpected_close=callback)
        broker.drop_connection()
        assert fired.wait(5.0)
        assert connection.state is ConnectionState.CLOSED

    def test_no_callback_configured_is_fine(self, connect):
        broker, connection = connect()
        broker.drop_connection()
        for _ in range(50):
            if connection.state is ConnectionState.CLOSED:
                break
            threading.Event().wait(0.05)
        assert connection.state is ConnectionState.CLOSED


class TestFrameDispatch:
    def test_frames_on_an_unknown_channel_are_dropped(self, connect, caplog):
        broker, connection = connect()
        broker.send(9, Flow(incoming_window=1, next_outgoing_id=0, outgoing_window=1))
        broker.send(0, Close())
        for _ in range(100):
            if connection.state is ConnectionState.CLOSED:
                break
            threading.Event().wait(0.05)
        assert "unknown channel 9" in caplog.text

    def test_a_second_open_is_ignored(self, connect):
        broker, connection = connect()
        broker.send(0, Open(container_id="again"))
        connection.send_frame(0, Flow(incoming_window=1, next_outgoing_id=0, outgoing_window=1))
        broker.wait_for(Flow)
        assert connection.state is ConnectionState.OPEN

    def test_begin_is_routed_by_remote_channel(self, connect):
        broker, connection = connect(broker_kwargs={"auto_respond": False})
        session = _begin_without_reply(connection)
        broker.wait_for(Begin)
        broker.send(7, Begin(remote_channel=session.channel, next_outgoing_id=0, incoming_window=5, outgoing_window=5))
        for _ in range(100):
            if session.remote_begin is not None:
                break
            threading.Event().wait(0.05)
        assert session.remote_begin is not None
        assert connection._sessions_by_remote_channel[7] is session
        broker.auto_respond = True


class _SessionDouble:
    """The minimum surface :meth:`Connection.allocate_channel` records."""

    channel = None

    def end(self, error=None):
        """Accept the teardown call ``Connection.close`` makes."""


def _begin_without_reply(connection):
    """Start ``begin`` on a background thread so the test can script the reply."""
    from src.session import Session

    session = Session(begin_timeout=5.0)
    threading.Thread(target=session.begin, args=(connection,), daemon=True).start()
    for _ in range(100):
        if session.channel is not None:
            break
        threading.Event().wait(0.01)
    return session
