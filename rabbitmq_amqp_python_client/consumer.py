import logging
from typing import Literal, Optional, Union, cast

from .amqp_consumer_handler import AMQPMessagingHandler
from .entities import (
    ConsumerOptions,
    DirectReplyToConsumerOptions,
    StreamConsumerOptions,
)
from .options import (
    ReceiverOptionUnsettled,
    ReceiverOptionUnsettledWithFilters,
)
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingReceiver,
)

logger = logging.getLogger(__name__)


class Consumer:
    """
    A consumer class for receiving messages from RabbitMQ via AMQP 1.0 protocol.

    This class handles the consumption of messages from a specified address in RabbitMQ.
    It supports both standard message consumption and stream-based consumption with
    optional filtering capabilities.

    Attributes:
        _receiver (Optional[BlockingReceiver]): The receiver for consuming messages
        _conn (BlockingConnection): The underlying connection to RabbitMQ
        _addr (str): The address to consume from
        _handler (Optional[MessagingHandler]): Optional message handling callback
        _consumer_options (Optional[StreamConsumerOptions]): Configuration for stream consumption
        _credit (Optional[int]): Flow control credit value
    """

    def __init__(
        self,
        conn: BlockingConnection,
        addr: Optional[str] = None,
        handler: Optional[AMQPMessagingHandler] = None,
        consumer_options: Optional[ConsumerOptions] = None,
        credit: Optional[int] = None,
    ):
        """
        Initialize a new Consumer instance.

        Args:
            conn: The blocking connection to use for consuming
            addr: The address to consume from
            handler: Optional message handler for processing received messages
            consumer_options: Optional configuration for stream-based consumption
            credit: Optional credit value for flow control
        """
        self._receiver: Optional[BlockingReceiver] = None
        self._conn = conn
        self._addr = addr
        self._handler = handler
        self._consumer_options = consumer_options
        self._credit = credit
        self._consumers: list[Consumer] = []
        self._open()

    def _open(self) -> None:
        if self._receiver is None:
            logger.debug("Creating Receiver")
            self._receiver = self._create_receiver(self._addr)

    def _update_connection(self, conn: BlockingConnection) -> None:
        addr = ""
        if self._addr is not None:
            addr = self._addr

        self._conn = conn
        if self._consumer_options is None:
            logger.debug("creating new receiver without stream")
            self._receiver = self._conn.create_receiver(
                addr,
                options=ReceiverOptionUnsettled(addr),
                handler=self._handler,
            )
        else:
            logger.debug("creating new stream receiver")
            self._consumer_options.offset(self._handler.offset - 1)  # type: ignore
            self._receiver = self._conn.create_receiver(
                addr,
                options=ReceiverOptionUnsettledWithFilters(
                    addr, self._consumer_options
                ),
                handler=self._handler,
            )

    def _set_consumers_list(self, consumers: []) -> None:  # type: ignore
        self._consumers = consumers

    def consume(self, timeout: Union[None, Literal[False], float] = False) -> Message:
        """
        Consume a message from the queue.

        Args:
            timeout: The time to wait for a message.
                    None: Defaults to 60s
                    float: Wait for specified number of seconds

        Returns:
            Message: The received message

        Note:
            The return type might be None if no message is available and timeout occurs,
            but this is handled by the cast to Message.
        """
        if self._receiver is not None:
            message = self._receiver.receive(timeout=timeout)
            return cast(Message, message)

    def close(self) -> None:
        """
        Close the consumer connection.

        Closes the receiver if it exists and cleans up resources.
        """
        logger.debug("Closing the receiver")
        if self._receiver is not None:
            self._receiver.close()
            if self in self._consumers:
                self._consumers.remove(self)

    def run(self) -> None:
        """
        Run the consumer in continuous mode.

        Starts the consumer's container to process messages continuously.
        """
        logger.debug("Running the consumer: starting to consume")
        if self._receiver is not None:
            self._receiver.container.run()

    def stop(self) -> None:
        """
        Stop the consumer's continuous processing.

        Stops the consumer's container, halting message processing.
        This should be called to cleanly stop a consumer that was started with run().
        """
        logger.debug("Stopping the consumer: starting to consume")
        if self._receiver is not None:
            self._receiver.container.stop_events()
            self._receiver.container.stop()

    def _create_receiver(self, addr: Optional[str] = None) -> BlockingReceiver:
        credit = 10
        if self._credit is not None:
            credit = self._credit

        if self._consumer_options is not None:
            logger.debug(
                "Creating the receiver, with options: %s",
                type(self._consumer_options).__name__,
            )
        else:
            logger.debug("Creating the receiver, without options")

        if self._consumer_options is None:
            return self._conn.create_receiver(
                addr,
                options=ReceiverOptionUnsettled(addr),
                handler=self._handler,
                credit=credit,
            )

        if isinstance(self._consumer_options, DirectReplyToConsumerOptions):
            logger.debug("Creating dynamic receiver for direct reply-to")
            dynamic_receiver = self._conn.create_dynamic_receiver(
                credit, handler=self._handler
            )
            dynamic_receiver.credit = credit
            return dynamic_receiver

        if isinstance(self._consumer_options, StreamConsumerOptions):
            return self._conn.create_receiver(
                addr,
                options=ReceiverOptionUnsettledWithFilters(
                    addr, self._consumer_options
                ),
                handler=self._handler,
            )

        raise Exception(
            "Receiver is not initialized. No valid consumer options provided."
        )

    @property
    def address(self) -> Optional[str]:
        if self._receiver is not None:
            return cast(Optional[str], self._receiver.link.remote_source.address)
        else:
            raise Exception("Receiver is not initialized")

    @property
    def handler(self) -> Optional[AMQPMessagingHandler]:
        return self._handler
