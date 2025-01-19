from typing import Dict

from .qpid.proton._data import PythonAMQPData
from .qpid.proton._delivery import Delivery
from .qpid.proton._events import Event


class DeliveryContext:

    def accept(self, event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.ACCEPTED)
        dlv.settle()

    def discard(self, event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.REJECTED)
        dlv.settle()

    def discard_with_annotations(
        self, event: Event, annotations: Dict[str, "PythonAMQPData"]
    ) -> None:
        dlv = event.delivery
        dlv.local.annotations = annotations
        dlv.local.failed = True
        dlv.local.undeliverable = True

        dlv.update(Delivery.MODIFIED)
        dlv.settle()

    def requeue(self, event: Event) -> None:
        dlv = event.delivery
        dlv.update(Delivery.RELEASED)
        dlv.settle()

    def requeue_with_annotations(
        self, event: Event, annotations: Dict[str, "PythonAMQPData"]
    ) -> None:
        dlv = event.delivery
        dlv.local.annotations = annotations
        dlv.local.failed = False
        dlv.local.undeliverable = False

        dlv.update(Delivery.MODIFIED)
        dlv.settle()
