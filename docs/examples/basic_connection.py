"""Opening and closing one connection: the smallest thing this client does (step_000).

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/basic_connection.py

Constructing a :class:`~src.Connection` dials the TCP
socket, exchanges the protocol header, runs SASL and the ``open`` handshake, and
starts the frame reader — all before ``__init__`` returns. There is no separate
``connect()`` step: a constructed connection is already ``OPEN`` and usable.

The default :class:`~src.ConnectionParameters` describe
exactly what a stock broker offers — ``localhost:5672``, ``guest``/``guest``, the
``/`` virtual host — so the no-argument form below is the shortest connection
this client can make. What the broker sent back in its own ``open`` is readable
afterwards through the negotiated ``max_frame_size`` and ``channel_max``.

``close()`` is the mirror image: it sends ``close``, waits for the broker's
answer and joins the reader thread. It is idempotent, so a ``finally`` block is
always the right place for it.
"""

from __future__ import annotations

import logging

from src import Connection, ConnectionParameters

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def basic_connection() -> None:
    """Open a connection with the defaults, report what was negotiated, close it."""
    parameters = ConnectionParameters()
    logger.info("dialling %s:%d as %r", parameters.host, parameters.resolved_port, parameters.user)

    connection = Connection(parameters)
    try:
        logger.info("state is %s", connection.state.value)
        logger.info("container-id is %r", connection.container_id)
        logger.info("negotiated max-frame-size=%d channel-max=%d", connection.max_frame_size, connection.channel_max)
    finally:
        connection.close()
    logger.info("state is %s", connection.state.value)


if __name__ == "__main__":
    basic_connection()
