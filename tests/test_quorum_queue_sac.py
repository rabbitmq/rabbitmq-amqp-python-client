"""
Integration tests for Quorum Queue Single Active Consumer (SAC) notifications.

Requires RabbitMQ 4.3+ (the version that first introduced the
``rabbitmq:active`` FLOW link-state property).

Key timing observation
----------------------
RabbitMQ sends the initial ``rabbitmq:active`` FLOW frame as part of the
ATTACH handshake, so the SAC callback fires **during** ``conn.consumer()``
(inside the blocking ``wait()`` in ``BlockingReceiver.__init__``), before
``consumer.run()`` is ever called.

Consequences for test design:

* The initial SAC callback must **not** raise.  It just records state and
  returns so that ``conn.consumer()`` can complete and return the consumer
  object normally.

* Subsequent SAC callbacks (e.g. a standby consumer being promoted while
  its ``consumer.run()`` is live) **may** raise ``ConsumerTestException`` to
  stop the reactor loop; the exception propagates cleanly out of
  ``consumer.run()``.

Threading rules
---------------
Each consumer owns its own connection and transport.  Run each consumer's
``run()`` in a dedicated thread when you need it to stay alive to receive
a later SAC notification (e.g. promotion).

Cleanup rules
-------------
After ``ConsumerTestException`` propagates out of ``consumer.run()``, the
proton reactor may be in an inconsistent state.  Every close call is wrapped
in its own try/except.
"""

import threading
import time

import pytest

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    Environment,
    Event,
    QuorumConsumerOptions,
)
from rabbitmq_amqp_python_client.queues import (
    QuorumQueueSpecification,
)

from .conftest import ConsumerTestException

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class SimpleAcceptHandler(AMQPMessagingHandler):
    """Accepts every incoming message without stopping the consumer."""

    def on_amqp_message(self, event: Event) -> None:
        self.delivery_context.accept(event)


def _safe_close(consumer, conn) -> None:
    """Close consumer and connection, swallowing any errors."""
    try:
        consumer.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_sac_first_consumer_becomes_active(
    connection: Connection, environment: Environment
) -> None:
    """
    A single consumer on a SAC-enabled quorum queue must receive
    ``active=True`` immediately after attaching (during ``conn.consumer()``).
    """
    if not connection._is_server_version_gte("4.3.0"):
        pytest.skip("SAC notifications require RabbitMQ 4.3+")

    queue_name = "test-sac-single-consumer"
    management = connection.management()
    try:
        management.delete_queue(queue_name)
    except Exception:
        pass
    management.declare_queue(
        QuorumQueueSpecification(name=queue_name, single_active_consumer=True)
    )

    addr = AddressHelper.queue_address(queue_name)
    sac_events: list[bool] = []

    consumer_conn = environment.connection()
    consumer_conn.dial()

    # The SAC callback fires during conn.consumer() – do NOT raise here.
    consumer = consumer_conn.consumer(
        addr,
        message_handler=SimpleAcceptHandler(),
        consumer_options=QuorumConsumerOptions(
            sac_state_handler=lambda active: sac_events.append(active)
        ),
    )

    _safe_close(consumer, consumer_conn)

    management.delete_queue(queue_name)
    management.close()

    assert sac_events, "Expected at least one SAC notification"
    assert (
        sac_events[0] is True
    ), f"First SAC notification should be active=True, got {sac_events}"


def test_sac_second_consumer_starts_as_standby(
    connection: Connection, environment: Environment
) -> None:
    """
    With two consumers on a SAC-enabled queue the second one must receive
    ``active=False`` (standby) while the first receives ``active=True``.

    The active notification for consumer-1 arrives during ``conn.consumer()``.
    The standby notification for consumer-2 arrives asynchronously while the
    consumer is running, so consumer-2 must be kept alive in a thread.
    """
    if not connection._is_server_version_gte("4.3.0"):
        pytest.skip("SAC notifications require RabbitMQ 4.3+")

    queue_name = "test-sac-two-consumers"
    management = connection.management()
    try:
        management.delete_queue(queue_name)
    except Exception:
        pass
    management.declare_queue(
        QuorumQueueSpecification(name=queue_name, single_active_consumer=True)
    )

    addr = AddressHelper.queue_address(queue_name)

    events_c1: list[bool] = []
    events_c2: list[bool] = []
    errors: list[str] = []

    # Consumer-1: active notification fires during conn.consumer().
    conn1 = environment.connection()
    conn1.dial()
    consumer1 = conn1.consumer(
        addr,
        message_handler=SimpleAcceptHandler(),
        consumer_options=QuorumConsumerOptions(
            sac_state_handler=lambda active: events_c1.append(active)
        ),
    )
    # events_c1 == [True] at this point

    # Consumer-2: standby notification arrives during consumer.run().
    def sac_c2_handler(active: bool) -> None:
        events_c2.append(active)
        if not active:
            raise ConsumerTestException("c2 confirmed standby, stopping")

    conn2 = environment.connection()
    conn2.dial()
    consumer2 = conn2.consumer(
        addr,
        message_handler=SimpleAcceptHandler(),
        consumer_options=QuorumConsumerOptions(sac_state_handler=sac_c2_handler),
    )

    def run_c2() -> None:
        try:
            consumer2.run()
        except ConsumerTestException:
            pass
        except Exception as exc:
            errors.append(f"c2 error: {exc}")
        finally:
            _safe_close(consumer2, conn2)

    t2 = threading.Thread(target=run_c2, daemon=True)
    t2.start()
    t2.join(timeout=15)

    _safe_close(consumer1, conn1)
    management.delete_queue(queue_name)
    management.close()

    assert not errors, f"Thread errors: {errors}"
    assert events_c1, "consumer-1 received no SAC notifications"
    assert events_c2, "consumer-2 received no SAC notifications"
    assert (
        events_c1[0] is True
    ), f"consumer-1 first event should be active=True, got {events_c1}"
    assert (
        events_c2[0] is False
    ), f"consumer-2 first event should be active=False (standby), got {events_c2}"


def test_sac_standby_promoted_when_active_closes(
    connection: Connection, environment: Environment
) -> None:
    """
    When the active consumer closes, the standby consumer must be promoted
    and receive ``active=True``.

    The initial SAC notifications arrive during ``conn.consumer()``.
    The promotion notification arrives later – while consumer-2 is live in
    ``consumer.run()``, so the callback may raise to stop the loop.
    """
    if not connection._is_server_version_gte("4.3.0"):
        pytest.skip("SAC notifications require RabbitMQ 4.3+")

    queue_name = "test-sac-promotion"
    management = connection.management()
    try:
        management.delete_queue(queue_name)
    except Exception:
        pass
    management.declare_queue(
        QuorumQueueSpecification(name=queue_name, single_active_consumer=True)
    )

    addr = AddressHelper.queue_address(queue_name)

    events_c1: list[bool] = []
    events_c2: list[bool] = []
    errors: list[str] = []

    # Consumer-2 handler: records state; raises to stop run() once promoted.
    def sac_c2_handler(active: bool) -> None:
        events_c2.append(active)
        if active and len(events_c2) >= 2:
            raise ConsumerTestException("c2 promoted, stopping consumer")

    conn1 = environment.connection()
    conn1.dial()
    consumer1 = conn1.consumer(
        addr,
        message_handler=SimpleAcceptHandler(),
        consumer_options=QuorumConsumerOptions(
            sac_state_handler=lambda active: events_c1.append(active)
        ),
    )
    # events_c1 == [True] (active)

    conn2 = environment.connection()
    conn2.dial()
    consumer2 = conn2.consumer(
        addr,
        message_handler=SimpleAcceptHandler(),
        consumer_options=QuorumConsumerOptions(sac_state_handler=sac_c2_handler),
    )
    # events_c2 == [False] (standby)

    # Run consumer-2 in a thread so it can receive the promotion notification.
    def run_c2() -> None:
        try:
            consumer2.run()
        except ConsumerTestException:
            pass
        except Exception as exc:
            errors.append(f"c2 error: {exc}")
        finally:
            _safe_close(consumer2, conn2)

    t2 = threading.Thread(target=run_c2, daemon=True)
    t2.start()

    # Small delay to ensure consumer-2's reactor is processing events.
    time.sleep(0.5)

    # Close consumer-1 to trigger promotion of consumer-2.
    _safe_close(consumer1, conn1)

    t2.join(timeout=20)

    management.delete_queue(queue_name)
    management.close()

    assert not errors, f"Thread errors: {errors}"
    assert events_c1, "consumer-1 received no SAC notifications"
    assert events_c2, "consumer-2 received no SAC notifications"
    assert events_c1[0] is True, f"consumer-1 should start as active, got {events_c1}"
    assert events_c2[0] is False, f"consumer-2 should start as standby, got {events_c2}"
    assert (
        True in events_c2[1:]
    ), f"consumer-2 should have been promoted to active, got {events_c2}"


def test_sac_validate_requires_4_3(environment: Environment) -> None:
    """
    QuorumConsumerOptions with sac_state_handler must raise ValidationCodeException
    when the server is older than 4.3.
    """
    from rabbitmq_amqp_python_client.exceptions import (
        ValidationCodeException,
    )

    conn = environment.connection()
    conn.dial()

    if conn._is_server_version_gte("4.3.0"):
        conn.close()
        pytest.skip("Server is 4.3+ — version-gate test not applicable")

    with pytest.raises(ValidationCodeException):
        conn.consumer(
            AddressHelper.queue_address("ignored"),
            consumer_options=QuorumConsumerOptions(
                sac_state_handler=lambda active: None
            ),
        )

    conn.close()


def test_sac_validate_rejects_direct_reply_to(environment: Environment) -> None:
    """
    Combining QuorumConsumerOptions.sac_state_handler with DirectReplyTo
    settle strategy must raise ValidationCodeException.
    """
    from rabbitmq_amqp_python_client import (
        ConsumerSettleStrategy,
    )
    from rabbitmq_amqp_python_client.exceptions import (
        ValidationCodeException,
    )

    conn = environment.connection()
    conn.dial()

    if not conn._is_server_version_gte("4.3.0"):
        conn.close()
        pytest.skip("SAC notifications require RabbitMQ 4.3+")

    with pytest.raises(ValidationCodeException):
        conn.consumer(
            AddressHelper.queue_address("ignored"),
            consumer_options=QuorumConsumerOptions(
                settle_strategy=ConsumerSettleStrategy.DirectReplyTo,
                sac_state_handler=lambda active: None,
            ),
        )

    conn.close()
