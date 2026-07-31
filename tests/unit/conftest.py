"""Fixtures wiring a real ``Connection`` to the in-process :class:`FakeBroker`."""

from __future__ import annotations

import contextlib
import socket

import pytest

from src import connection as connection_module
from src.connection import Connection, ConnectionParameters
from src.reconnection import RecoveryConfiguration
from tests.unit.fake_broker import BrokerFarm, FakeBroker


@pytest.fixture(autouse=True)
def short_close_timeout(monkeypatch):
    """Keep teardown fast when a test leaves the fake broker unresponsive."""
    monkeypatch.setattr(connection_module, "CLOSE_TIMEOUT_SECONDS", 0.3)


@pytest.fixture
def broker_factory(monkeypatch):
    """Return ``make(**broker_kwargs) -> FakeBroker`` and route dialing to it.

    The socket pair replaces only the ``connect()`` step: the returned broker
    then answers the real SASL/``open`` handshake the ``Connection`` performs.
    """
    created: list[FakeBroker] = []
    sockets: list[socket.socket] = []

    def make(**broker_kwargs) -> FakeBroker:
        client_side, broker_side = socket.socketpair()
        client_side.settimeout(5.0)
        sockets.extend((client_side, broker_side))
        broker = FakeBroker(broker_side, **broker_kwargs)
        created.append(broker)
        monkeypatch.setattr(connection_module, "_connect_socket", lambda parameters: client_side)
        return broker.start()

    yield make

    for broker in created:
        broker.stop()
    for sock in sockets:
        with contextlib.suppress(OSError):
            sock.close()


@pytest.fixture
def connect(broker_factory):
    """Return ``open(**parameter_overrides) -> (broker, connection)``.

    Auto-reconnection is off unless a test asks for it: this fixture answers
    every dial with the same single socket, which a recovery loop could not
    redial anyway. See the ``broker_farm`` fixture for the recovery tests.
    """
    connections: list[Connection] = []
    brokers: list[FakeBroker] = []

    def open_connection(*, broker_kwargs=None, **parameter_overrides):
        broker = broker_factory(**(broker_kwargs or {}))
        brokers.append(broker)
        parameter_overrides.setdefault("recovery_configuration", RecoveryConfiguration(activated=False))
        connection = Connection(ConnectionParameters(**parameter_overrides))
        connections.append(connection)
        return broker, connection

    yield open_connection

    for broker in brokers:
        # Let a broker a test silenced answer again, so teardown does not wait
        # out every close/end timeout.
        broker.auto_respond = True
    for connection in connections:
        with contextlib.suppress(Exception):  # teardown must never mask a test failure
            connection.close()


@pytest.fixture
def broker_farm(monkeypatch):
    """Return a :class:`BrokerFarm` answering every dial, redials included."""
    farm = BrokerFarm()
    monkeypatch.setattr(connection_module, "_connect_socket", farm.dial)
    yield farm
    farm.close()
