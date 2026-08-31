"""Publishing: the three shapes a publisher's target can take (step_020 §6).

Run against a local broker::

    PYTHONPATH=. .venv/bin/python docs/examples/publisher_example.py

A publisher is a sender link attached to one node address, and the builder is
how that address is chosen. All three forms appear here:

1. ``publisher_builder().queue(name)`` — target ``/queues/{name}``, i.e. one
   queue, bypassing the exchange machinery altogether;
2. ``publisher_builder().exchange(name).key(key)`` — target
   ``/exchanges/{name}/{key}``, so the broker routes by binding as usual;
3. ``publisher_builder()`` with neither — an **anonymous** publisher, attached to
   a null target. It has no address of its own, so every message must name one
   in ``properties.to``, which lets a single link fan out over as many
   destinations as it likes. That is what the third publisher does: two messages,
   two different addresses, one link.

Every ``publish`` blocks until the broker settles the message and returns a
:class:`~rabbitmq_amqp_python_client.PublishResult` pairing the message with its
outcome. ``RELEASED`` is worth noticing: it is not an error and not a rejection,
it is the broker saying the message routed nowhere — which is exactly what
happens when an exchange has no binding matching the routing key.

Publishers are closed before the connection, so their links detach cleanly
instead of being torn down with the socket.
"""

from __future__ import annotations

import logging
import uuid

from rabbitmq_amqp_python_client import (
    Connection,
    ConnectionParameters,
    ExchangeType,
    Management,
    Message,
    Properties,
    Publisher,
    exchange_address,
    queue_address,
)

#: The routing key the exchange is bound to the second queue with.
ROUTING_KEY = "orders"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("example")


def publish_one(publisher: Publisher, label: str, message: Message) -> None:
    """Publish one message and log the outcome the broker reported."""
    result = publisher.publish(message)
    outcome = result.outcome
    logger.info("%s: %r -> %s", label, result.message.body_as_string(), outcome.state.value)


def declare_topology(management: Management, direct_queue: str, routed_queue: str, exchange: str) -> None:
    """Declare both queues and the exchange, and bind the exchange to one of them."""
    management.queue(direct_queue).declare()
    management.queue(routed_queue).declare()
    management.exchange(exchange).type(ExchangeType.DIRECT).declare()
    management.bind(source=exchange, destination=routed_queue, binding_key=ROUTING_KEY)
    logger.info("declared %r, %r and %r, bound with the key %r", direct_queue, routed_queue, exchange, ROUTING_KEY)


def delete_topology(management: Management, direct_queue: str, routed_queue: str, exchange: str) -> None:
    """Report what each queue holds, then remove everything that was declared."""
    for name in (direct_queue, routed_queue):
        logger.info("queue %r holds %d message(s)", name, management.queue_info(name).message_count)
    management.unbind(source=exchange, destination=routed_queue, binding_key=ROUTING_KEY)
    management.exchange(exchange).delete()
    management.queue(direct_queue).delete()
    management.queue(routed_queue).delete()
    logger.info("deleted the queues and the exchange")


def publisher_example() -> None:
    """Publish through a queue publisher, an exchange publisher and an anonymous one."""
    connection = Connection(ConnectionParameters(container_id=f"example-publisher-{uuid.uuid4().hex[:8]}"))
    suffix = uuid.uuid4().hex[:8]
    direct_queue = f"example-direct-{suffix}"
    routed_queue = f"example-routed-{suffix}"
    exchange = f"example-exchange-{suffix}"
    try:
        management = connection.management()
        declare_topology(management, direct_queue, routed_queue, exchange)

        # 1. Bound to a queue: the address is fixed at build time.
        to_queue = connection.publisher_builder().queue(direct_queue).build()
        # 2. Bound to an exchange plus a routing key.
        to_exchange = connection.publisher_builder().exchange(exchange).key(ROUTING_KEY).build()
        # 3. Anonymous: no address at all, so each message carries its own.
        anonymous = connection.publisher_builder().build()
        try:
            logger.info(
                "publisher addresses: queue=%r exchange=%r anonymous=%r",
                to_queue.address,
                to_exchange.address,
                anonymous.address,
            )

            publish_one(to_queue, "queue publisher", Message("straight to the queue"))
            publish_one(to_exchange, "exchange publisher", Message("routed by the exchange"))

            # One link, two destinations, chosen per message.
            publish_one(
                anonymous,
                "anonymous publisher -> queue",
                Message("anonymous, addressed to a queue", properties=Properties(to=queue_address(direct_queue))),
            )
            publish_one(
                anonymous,
                "anonymous publisher -> exchange",
                Message(
                    "anonymous, addressed to an exchange",
                    properties=Properties(to=exchange_address(exchange, ROUTING_KEY)),
                ),
            )

            # A routing key nothing is bound to: the broker settles it as
            # released rather than rejecting it or raising anything.
            publish_one(
                anonymous,
                "anonymous publisher -> unbound key",
                Message("nowhere to go", properties=Properties(to=exchange_address(exchange, "unbound-key"))),
            )
        finally:
            for publisher in (to_queue, to_exchange, anonymous):
                publisher.close()
            logger.info("closed every publisher")

        delete_topology(management, direct_queue, routed_queue, exchange)
    finally:
        connection.close()


if __name__ == "__main__":
    publisher_example()
