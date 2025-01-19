from .delivery_context import DeliveryContext
from .qpid.proton.handlers import MessagingHandler


class DeliveryConsumerHandler(MessagingHandler):  # type: ignore

    def __init__(self, auto_accept: bool = False, auto_settle: bool = True):
        super().__init__(auto_accept=auto_accept, auto_settle=auto_settle)
        self._count = 0
        self.delivery_context: DeliveryContext = DeliveryContext()
