from typing import Union

from .delivery_context import (
    AbcDeliveryContext,
    DeliveryContext,
)
from .qpid.proton._endpoints import Link
from .qpid.proton._events import Event
from .qpid.proton._handler import Handler
from .qpid.proton.handlers import MessagingHandler

"""
AMQPMessagingHandler extends the QPID MessagingHandler.
It is an helper to set the default values needed for manually accepting and settling messages.
self.delivery_context is an instance of IDeliveryContext (defaults to DeliveryContext), which is used to accept, reject,
requeue or requeue with annotations a message.
It is not mandatory to use this class, but it is a good practice to use it.
"""


class _DeferredFlowController(Handler):
    """
    A flow controller that intentionally omits on_link_local_open.

    The standard FlowController grants receiver credit in both on_link_local_open
    and on_link_remote_open. Granting credit during on_link_local_open causes the
    server to start delivering messages while BlockingLink.__init__() is still
    running its wait() loop (i.e. before consumer.run() is ever called), because:

      1. The FLOW frame carrying that credit is flushed to the server in the same
         selector.select() call that performs the ATTACH handshake.
      2. The server immediately sends stream messages back.
      3. Those messages arrive in the same t.push() read as the ATTACH response
         and are dispatched in the same process() iteration — inside wait() —
         so on_amqp_message fires before consumer.run() returns.

    By omitting on_link_local_open the FLOW frame is only queued (in
    on_link_remote_open) after the remote ATTACH is received, at which point the
    wait() loop has already exited. The queued FLOW is sent only when
    consumer.run() drives the first process() cycle, ensuring messages start
    arriving only after run() is called.

    Credit replenishment (on_link_flow / on_delivery) works exactly as before.
    """

    def __init__(self, window: int = 10) -> None:
        self._window = window
        self._drained = 0

    def on_link_remote_open(self, event: Event) -> None:
        self._flow(event.link)

    def on_link_flow(self, event: Event) -> None:
        self._flow(event.link)

    def on_delivery(self, event: Event) -> None:
        self._flow(event.link)

    def _flow(self, link: Union["Link", object]) -> None:
        if link.is_receiver:  # type: ignore[union-attr]
            self._drained += link.drained()  # type: ignore[union-attr]
            if self._drained == 0:
                delta = self._window - link.credit  # type: ignore[union-attr]
                link.flow(delta)  # type: ignore[union-attr]


class AMQPMessagingHandler(MessagingHandler):

    def __init__(self, auto_accept: bool = False, auto_settle: bool = True):
        """
        :param auto_accept: if True, the message is automatically accepted
        by default is false, so the user has to manually accept the message and decide with the
        different methods of the delivery_context what to do with the message
        """
        # prefetch=0 disables the built-in FlowController so it cannot grant
        # credit during link setup (which would make messages arrive before
        # consumer.run() is called).  _DeferredFlowController is added instead;
        # it behaves identically except it does not implement on_link_local_open.
        super().__init__(prefetch=0, auto_accept=auto_accept, auto_settle=auto_settle)
        self.handlers.append(_DeferredFlowController())
        self.delivery_context: AbcDeliveryContext = DeliveryContext()
        self._offset = 0

    def on_amqp_message(self, event: Event) -> None:
        pass

    def on_message(self, event: Event) -> None:
        if "x-stream-offset" in event.message.annotations:
            self._offset = int(event.message.annotations["x-stream-offset"])
        self.on_amqp_message(event)

    @property
    def offset(self) -> int:
        return self._offset
