from typing import Dict

from .exceptions import ArgumentOutOfRangeException
from .qpid.proton._data import PythonAMQPData
from .qpid.proton._delivery import Delivery
from .qpid.proton._events import Event
from .utils import validate_annotations

"""
DeliveryContext is a class that is used to accept, reject, requeue or requeue with annotations a message.
It is an helper to set the default values needed for manually accepting and settling messages.
"""


class DeliveryContext:
    """
    Accept the message (AMQP 1.0 <code>accepted</code> outcome).

    This means the message has been processed and the broker can delete it.
    """

    def accept(self, event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.ACCEPTED)
        dlv.settle()

    """
    Reject the message (AMQP 1.0 <code>rejected</code> outcome).
    This means the message cannot be processed because it is invalid, the broker can drop it
    or dead-letter it if it is configured.
    """

    def discard(self, event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.REJECTED)
        dlv.settle()

    """
    Discard the message with annotations to combine with the existing message annotations.
    This means the message cannot be processed because it is invalid, the broker can drop it
    or dead-letter it if it is configured.
    Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
    Annotation keys the broker understands starts with <code>x-</code>, but not with <code>x-opt-
        This maps to the AMQP 1.0
        modified{delivery-failed = true, undeliverable-here = true}</code> outcome.
         <param name="annotations"> annotations message annotations to combine with existing ones </param>
        <a
            href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
                        1.0 <code>modified</code> outcome</a>
         The annotations can be used only with Quorum queues, see https://www.rabbitmq.com/docs/amqp#modified-outcome
    """

    def discard_with_annotations(
        self, event: Event, annotations: Dict[str, "PythonAMQPData"]
    ) -> None:
        dlv = event.delivery
        dlv.local.annotations = annotations
        dlv.local.failed = True
        dlv.local.undeliverable = True

        validated = validate_annotations(annotations.keys())

        if validated is False:
            raise ArgumentOutOfRangeException(
                "Message annotation key must start with 'x-'"
            )

        dlv.update(Delivery.MODIFIED)
        dlv.settle()

    """
    Requeue the message (AMQP 1.0 <code>released</code> outcome).
    This means the message has not been processed and the broker can requeue it and deliver it
    to the same or a different consumer.
    """

    def requeue(self, event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.RELEASED)
        dlv.settle()

    """
     Requeue the message with annotations to combine with the existing message annotations.
        This means the message has not been processed and the broker can requeue it and deliver it
         to the same or a different consumer.
         Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
         Annotation keys the broker understands starts with <code>x-</code>, but not with <code>x-opt-
         </code>.
         This maps to the AMQP 1.0 <code>
         modified{delivery-failed = false, undeliverable-here = false}</code> outcome.
         <param name="annotations"> annotations message annotations to combine with existing ones </param>
        <a
             href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
             1.0 <code>modified</code> outcome</a>
        The annotations can be used only with Quorum queues, see https://www.rabbitmq.com/docs/amqp#modified-outcome
    """

    def requeue_with_annotations(
        self, event: Event, annotations: Dict[str, "PythonAMQPData"]
    ) -> None:
        dlv = event.delivery
        dlv.local.annotations = annotations
        dlv.local.failed = False
        dlv.local.undeliverable = False

        validated = validate_annotations(annotations.keys())

        if validated is False:
            raise ArgumentOutOfRangeException(
                "Message annotation key must start with 'x-'"
            )

        dlv.update(Delivery.MODIFIED)
        dlv.settle()
