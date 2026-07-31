"""Managing topology over AMQP 1.0: queues, exchanges and bindings (step_001).

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/management_example.py

There is no HTTP and no AMQP 0.9.1 channel behind any of this. The first call to
``connection.management()`` opens a dedicated session and attaches a *link pair*
— one sender and one receiver sharing the link name ``management-link-pair`` —
to the well-known ``/management`` node. Every operation below is then one
request message out and one response message back, correlated by
``message-id``/``correlation-id``, all multiplexed over that single pair.

The script walks the whole surface once:

1. ``queue(name).declare()`` declares a classic queue and returns a
   :class:`~src.QueueInfo` parsed from the broker's
   answer, so the queue's real settings are readable straight away;
2. ``exchange(name).type(ExchangeType.TOPIC).declare()`` declares a topic
   exchange;
3. ``bind(source=..., destination=..., binding_key=...)`` binds the queue to it;
4. ``list_bindings(...)`` reads the bindings back — each entry carries the
   binding key, its arguments, and the opaque ``location`` that identifies it;
5. ``unbind``, ``exchange(...).delete()`` and ``queue(...).delete()`` undo all
   three, in the reverse order.

Declaring is idempotent as long as the arguments match: re-declaring the same
queue is a successful no-op, while re-declaring it with different arguments is a
:class:`~src.ManagementError`.
"""

from __future__ import annotations

import logging
import uuid

from src import (
    Connection,
    ConnectionParameters,
    ExchangeType,
    Management,
)

#: The topic pattern the queue is bound with.
BINDING_KEY = "orders.*"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def declare_topology(management: Management, queue: str, exchange: str) -> None:
    """Declare the queue and the exchange, then bind one to the other."""
    info = management.queue(queue).declare()
    logger.info(
        "declared queue %r: type=%s durable=%s exclusive=%s messages=%d",
        info.name,
        info.queue_type.value,
        info.durable,
        info.exclusive,
        info.message_count,
    )

    management.exchange(exchange).type(ExchangeType.TOPIC).declare()
    logger.info("declared the topic exchange %r", exchange)

    management.bind(source=exchange, destination=queue, binding_key=BINDING_KEY)
    logger.info("bound %r to %r with the binding key %r", queue, exchange, BINDING_KEY)


def show_bindings(management: Management, queue: str, exchange: str) -> None:
    """List and print the bindings between the exchange and the queue."""
    bindings = management.list_bindings(source=exchange, destination=queue, binding_key=BINDING_KEY)
    logger.info("the broker reports %d binding(s) from %r to %r", len(bindings), exchange, queue)
    for binding in bindings:
        logger.info(
            "  binding_key=%r arguments=%r location=%r",
            binding.get("binding_key"),
            binding.get("arguments"),
            binding.get("location"),
        )


def delete_topology(management: Management, queue: str, exchange: str) -> None:
    """Undo everything :func:`declare_topology` created, in the reverse order."""
    management.unbind(source=exchange, destination=queue, binding_key=BINDING_KEY)
    logger.info("unbound %r from %r", queue, exchange)

    management.exchange(exchange).delete()
    logger.info("deleted the exchange %r", exchange)

    info = management.queue(queue).delete()
    logger.info("deleted the queue %r, which still held %d message(s)", info.name, info.message_count)


def management_example() -> None:
    """Declare a queue, an exchange and a binding, inspect them, then clean up."""
    connection = Connection(ConnectionParameters(container_id=f"example-management-{uuid.uuid4().hex[:8]}"))
    suffix = uuid.uuid4().hex[:8]
    queue, exchange = f"example-queue-{suffix}", f"example-exchange-{suffix}"
    try:
        management = connection.management()
        declare_topology(management, queue, exchange)
        show_bindings(management, queue, exchange)
        delete_topology(management, queue, exchange)
    finally:
        connection.close()


if __name__ == "__main__":
    management_example()
