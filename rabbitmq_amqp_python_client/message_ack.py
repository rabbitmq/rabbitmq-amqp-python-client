from typing import Dict

from .qpid.proton._data import PythonAMQPData
from .qpid.proton._delivery import Delivery
from .qpid.proton._events import Event


class MessageAck:

    @staticmethod
    def accept(event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.ACCEPTED)
        dlv.settle()

    @staticmethod
    def discard(event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.REJECTED)
        dlv.settle()

    @staticmethod
    def discard_with_annotations(
        event: Event, annotations: Dict[str, "PythonAMQPData"]
    ) -> None:
        dlv = event.delivery
        dlv.local.annotations = annotations
        dlv.local.failed = True
        dlv.local.undeliverable = True

        dlv.update(Delivery.MODIFIED)
        dlv.settle()

    @staticmethod
    def requeue(event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.RELEASED)
        dlv.settle()

    @staticmethod
    def requeue_with_annotations(
        event: Event, annotations: Dict[str, "PythonAMQPData"]
    ) -> None:
        dlv = event.delivery
        if dlv.local.annotations is None:
            print("is none")
        dlv.local.annotations = annotations
        dlv.local.failed = False
        dlv.local.undeliverable = False

        dlv.update(Delivery.MODIFIED)
        dlv.settle()
