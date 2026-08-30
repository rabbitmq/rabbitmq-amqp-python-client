"""TLS transport and certificate-based SASL EXTERNAL authentication (006_tls_auth).

Run against a local broker with the TLS listener enabled (``.ci/ubuntu/rabbitmq.conf``, started via
``.ci/ubuntu/gha-setup.sh``)::

    PYTHONPATH=. .venv/bin/python docs/examples/tls_connection.py

Three connections, in order:

1. **One-way TLS** (the default, recommended path): the broker's certificate is verified against the CA in
   ``.ci/certs/ca_certificate.pem``; no client certificate is presented, and SASL PLAIN authenticates as usual.
2. **Mutual TLS + SASL EXTERNAL**: a client certificate (``.ci/certs/client_localhost_certificate.pem``) is also
   presented during the handshake, and ``sasl_external=True`` tells the broker to authenticate by that
   certificate's identity instead of a username/password — no credentials travel over SASL at all.
3. **Verification disabled** — kept last and clearly separate from the two paths above. This trusts *no* CA and
   accepts *any* certificate the broker happens to present, which defeats the entire point of TLS; it exists only
   because callers occasionally need it against a self-signed broker they have already vetted out of band (a
   local development instance, say), never as a default.
"""

from __future__ import annotations

import logging
import pathlib
import ssl

from src import Connection, ConnectionParameters

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")

CERTS_DIR = pathlib.Path(__file__).resolve().parents[2] / ".ci" / "certs"
CA_CERTIFICATE = CERTS_DIR / "ca_certificate.pem"
CLIENT_CERTIFICATE = CERTS_DIR / "client_localhost_certificate.pem"
CLIENT_KEY = CERTS_DIR / "client_localhost_key.pem"


def one_way_tls() -> None:
    """Verify the broker's certificate; no client certificate, plain credentials."""
    context = ssl.create_default_context(cafile=str(CA_CERTIFICATE))
    connection = Connection(ConnectionParameters(tls=context))
    try:
        logger.info(
            "one-way TLS: connected, state is %s, client certificate presented: no, SASL mechanism was %s",
            connection.state.value,
            connection.parameters.sasl_mechanism,
        )
    finally:
        connection.close()


def mutual_tls_with_sasl_external() -> None:
    """Present a client certificate and authenticate by its identity, not a password."""
    context = ssl.create_default_context(cafile=str(CA_CERTIFICATE))
    context.load_cert_chain(certfile=str(CLIENT_CERTIFICATE), keyfile=str(CLIENT_KEY))
    connection = Connection(ConnectionParameters(tls=context, sasl_external=True, user="", password=""))
    try:
        logger.info(
            "mutual TLS + SASL EXTERNAL: connected, state is %s, client certificate presented: yes, "
            "SASL mechanism was %s",
            connection.state.value,
            connection.parameters.sasl_mechanism,
        )
    finally:
        connection.close()


def verification_disabled() -> None:
    """DISCOURAGED outside local development: trust no CA, verify no hostname.

    Never use this against a broker whose certificate you have not already vetted out of band — it accepts
    literally any certificate the peer presents, which is exactly the attack TLS verification exists to prevent.
    """
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    connection = Connection(ConnectionParameters(tls=context))
    try:
        logger.info(
            "verification disabled: connected, state is %s, client certificate presented: no, SASL mechanism was %s",
            connection.state.value,
            connection.parameters.sasl_mechanism,
        )
    finally:
        connection.close()


if __name__ == "__main__":
    one_way_tls()
    mutual_tls_with_sasl_external()
    verification_disabled()
