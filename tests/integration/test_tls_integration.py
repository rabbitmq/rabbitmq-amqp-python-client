"""TLS transport and SASL EXTERNAL authentication against a live RabbitMQ broker.

Implements the configuration matrix from 006_tls_auth/step_111_tls_test_configurations.md §2. Row 1 (plain,
unauthenticated TCP) is already covered by test_connection_integration.py and is not duplicated here.
"""

from __future__ import annotations

import ssl

import pytest

from src import AuthenticationError, Connection, ConnectionParameters, ConnectionState

from .conftest import BROKER_HOST, BROKER_TLS_PORT, CA_CERTIFICATE, CLIENT_CERTIFICATE, CLIENT_KEY

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("require_tls_broker")]


def _connect(**overrides) -> Connection:
    overrides.setdefault("host", BROKER_HOST)
    overrides.setdefault("port", BROKER_TLS_PORT)
    return Connection(ConnectionParameters(**overrides))


class TestOneWayTls:
    """§2 rows 2-4: server-certificate verification only, no client certificate."""

    def test_connects_when_the_ca_is_trusted(self):
        context = ssl.create_default_context(cafile=str(CA_CERTIFICATE))
        connection = _connect(tls=context)
        try:
            assert connection.state is ConnectionState.OPEN
        finally:
            connection.close()

    def test_fails_when_the_ca_is_not_trusted(self):
        # An empty trust store, not ssl.create_default_context() with no cafile: CI installs this
        # fixture's CA into the OS trust store too (.ci/ubuntu/gha-setup.sh's install_ca_certificate),
        # so falling back to the OS default would make this test environment-dependent.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with pytest.raises(ssl.SSLError):
            _connect(tls=context)

    def test_fails_on_hostname_mismatch(self):
        context = ssl.create_default_context(cafile=str(CA_CERTIFICATE))
        with pytest.raises(ssl.SSLCertVerificationError):
            _connect(tls=context, host="127.0.0.1")


class TestMutualTls:
    """§2 rows 5-7: a client certificate is also presented during the handshake."""

    def test_plain_auth_still_works_with_a_client_certificate(self):
        context = ssl.create_default_context(cafile=str(CA_CERTIFICATE))
        context.load_cert_chain(certfile=str(CLIENT_CERTIFICATE), keyfile=str(CLIENT_KEY))
        connection = _connect(tls=context)
        try:
            assert connection.state is ConnectionState.OPEN
        finally:
            connection.close()

    def test_sasl_external_authenticates_as_the_provisioned_user(self):
        context = ssl.create_default_context(cafile=str(CA_CERTIFICATE))
        context.load_cert_chain(certfile=str(CLIENT_CERTIFICATE), keyfile=str(CLIENT_KEY))
        connection = _connect(tls=context, sasl_external=True, user="", password="")
        try:
            assert connection.state is ConnectionState.OPEN
        finally:
            connection.close()

    def test_sasl_external_is_rejected_without_a_client_certificate(self):
        context = ssl.create_default_context(cafile=str(CA_CERTIFICATE))
        with pytest.raises(AuthenticationError):
            _connect(tls=context, sasl_external=True, user="", password="")


class TestInsecureOptOut:
    """§2 row 8: verification explicitly disabled by the caller."""

    def test_connects_with_verification_disabled(self):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        connection = _connect(tls=context)
        try:
            assert connection.state is ConnectionState.OPEN
        finally:
            connection.close()
