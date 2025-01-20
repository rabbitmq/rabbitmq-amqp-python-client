from .delivery_context import DeliveryContext
from .qpid.proton.handlers import MessagingHandler

"""
AMQPMessagingHandler extends the QPID MessagingHandler.
It is an helper to set the default values needed for manually accepting and settling messages.
self.delivery_context is an instance of DeliveryContext, which is used to accept, reject,
requeue or requeue with annotations a message.
It is not mandatory to use this class, but it is a good practice to use it.
"""


class AMQPMessagingHandler(MessagingHandler):  # type: ignore

    def __init__(self, auto_accept: bool = False, auto_settle: bool = True):
        """
        :param auto_accept: if True, the message is automatically accepted
        by default is false, so the user has to manually accept the message and decide with the
        different methods of the delivery_context what to do with the message
        """
        super().__init__(auto_accept=auto_accept, auto_settle=auto_settle)
        self.delivery_context: DeliveryContext = DeliveryContext()
