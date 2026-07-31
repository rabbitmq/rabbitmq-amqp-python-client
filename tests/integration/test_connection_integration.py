"""Connection bootstrap and teardown against a live RabbitMQ broker."""

from __future__ import annotations

import socket
import threading
import time

import pytest

from src import (
    AuthenticationError,
    Connection,
    ConnectionParameters,
    ConnectionState,
    ProtocolError,
    RecoveryConfiguration,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def connection():
    """An open connection to the local broker, closed on teardown."""
    opened = Connection(ConnectionParameters())
    try:
        yield opened
    finally:
        opened.close()


class TestBootstrap:
    def test_default_parameters_connect(self, connection):
        assert connection.state is ConnectionState.OPEN
        assert connection.is_open

    def test_the_broker_identifies_itself(self, connection):
        remote_open = connection.remote_open
        assert remote_open is not None
        assert remote_open.container_id
        assert remote_open.properties is not None
        assert remote_open.properties["product"] == "RabbitMQ"

    def test_limits_are_negotiated_down_to_the_brokers_values(self, connection):
        remote_open = connection.remote_open
        assert connection.max_frame_size == min(1024 * 1024, remote_open.max_frame_size)
        assert connection.channel_max == min(65535, remote_open.channel_max)

    def test_the_container_id_is_the_one_we_announced(self):
        parameters = ConnectionParameters(container_id="integration-test-container")
        opened = Connection(parameters)
        try:
            assert opened.container_id == "integration-test-container"
        finally:
            opened.close()

    def test_bad_credentials_raise_authentication_error(self):
        with pytest.raises(AuthenticationError):
            Connection(ConnectionParameters(user="nope", password="wrong"))

    def test_an_unknown_virtual_host_is_refused(self):
        # RabbitMQ 4.x drops the transport right after our open; older/newer
        # versions may instead answer close(amqp:unauthorized-access).
        with pytest.raises((ProtocolError, AuthenticationError)):
            Connection(ConnectionParameters(virtual_host="does-not-exist"))

    def test_a_closed_port_raises_an_os_error(self):
        with pytest.raises(OSError):
            Connection(ConnectionParameters(port=5699))


class TestSessions:
    def test_sessions_open_on_increasing_channels(self, connection):
        first = connection.open_session()
        second = connection.open_session()
        assert (first.channel, second.channel) == (0, 1)
        assert first.is_open and second.is_open
        assert first.handle_max >= 0
        first.end()
        second.end()

    def test_close_ends_still_open_sessions(self):
        opened = Connection(ConnectionParameters())
        session = opened.open_session()
        opened.close()
        assert not session.is_open
        assert opened.state is ConnectionState.CLOSED


class TestClose:
    def test_closes_cleanly_and_is_idempotent(self):
        opened = Connection(ConnectionParameters())
        opened.close()
        assert opened.state is ConnectionState.CLOSED
        opened.close()
        assert opened.state is ConnectionState.CLOSED

    def test_close_does_not_report_an_unexpected_close(self):
        seen = []
        opened = Connection(ConnectionParameters(on_unexpected_close=seen.append))
        opened.close()
        time.sleep(0.2)
        assert seen == []


class TestUnexpectedClose:
    def test_a_dead_transport_invokes_the_callback_exactly_once(self):
        """A transport that dies without ``close()`` must reach the callback.

        The failure is induced by tearing down the live socket underneath the
        client, which is what the frame reader sees for any broker-side kill or
        network partition. Recovery is switched off explicitly: it is on by
        default (step_040 §2), and a connection that recovers defers this
        callback instead of firing it — the behavior
        ``test_reconnection_integration.py`` covers.
        """
        fired = threading.Event()
        seen: list[BaseException | None] = []

        def on_unexpected_close(error):
            seen.append(error)
            fired.set()

        opened = Connection(
            ConnectionParameters(
                on_unexpected_close=on_unexpected_close,
                recovery_configuration=RecoveryConfiguration(activated=False),
            )
        )
        try:
            opened._socket.shutdown(socket.SHUT_RDWR)
            assert fired.wait(10.0), "on_unexpected_close was never invoked"
            time.sleep(0.3)
            assert len(seen) == 1
            assert opened.state is ConnectionState.CLOSED
        finally:
            opened.close()
        assert len(seen) == 1
