import logging
import uuid
from typing import Any, Optional, Union

from .address_helper import AddressHelper
from .common import CommonValues, QueueType
from .entities import (
    ExchangeCustomSpecification,
    ExchangeSpecification,
    ExchangeToExchangeBindingSpecification,
    ExchangeToQueueBindingSpecification,
    QueueInfo,
)
from .exceptions import ValidationCodeException
from .options import ReceiverOption, SenderOption
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)
from .queues import (
    ClassicQueueSpecification,
    QuorumQueueSpecification,
    StreamSpecification,
)

logger = logging.getLogger(__name__)


class Management:
    """
    Handles RabbitMQ management operations via AMQP 1.0 protocol.

    This class provides methods for declaring and managing exchanges, queues,
    and bindings in RabbitMQ. It uses a blocking connection to communicate
    with the RabbitMQ management interface.

    Attributes:
        _sender (Optional[BlockingSender]): The sender for management commands
        _receiver (Optional[BlockingReceiver]): The receiver for management responses
        _conn (BlockingConnection): The underlying connection to RabbitMQ
    """

    def __init__(self, conn: BlockingConnection):
        """
        Initialize a new Management instance.

        Args:
            conn: The blocking connection to use for management operations
        """
        self._sender: Optional[BlockingSender] = None
        self._receiver: Optional[BlockingReceiver] = None
        self._conn = conn

    def _update_connection(self, conn: BlockingConnection) -> None:
        self._conn = conn
        self._sender = self._create_sender(CommonValues.management_node_address.value)
        self._receiver = self._create_receiver(
            CommonValues.management_node_address.value,
        )

    def open(self) -> None:
        """
        Open the management connection by creating sender and receiver.

        Creates sender and receiver if they don't exist, using the management
        node address defined in CommonValues.
        """
        if self._sender is None:
            logger.debug("Creating Sender")
            self._sender = self._create_sender(
                CommonValues.management_node_address.value
            )
        if self._receiver is None:
            logger.debug("Creating Receiver")
            self._receiver = self._create_receiver(
                CommonValues.management_node_address.value,
            )

    def _create_sender(self, addr: str) -> BlockingSender:
        return self._conn.create_sender(addr, options=SenderOption(addr))

    def _create_receiver(self, addr: str) -> BlockingReceiver:
        return self._conn.create_receiver(addr, options=ReceiverOption(addr))

    # closes the connection to the AMQP 1.0 server.
    def close(self) -> None:
        """
        Close the management connection.

        Closes both sender and receiver if they exist.
        """
        logger.debug("Closing Sender and Receiver")
        if self._sender is not None:
            self._sender.close()
        if self._receiver is not None:
            self._receiver.close()

    def request(
        self,
        body: Any,
        path: str,
        method: str,
        expected_response_codes: list[int],
    ) -> Message:
        """
        Send a management request with a new UUID.

        Args:
            body: The request body to send
            path: The management API path
            method: The HTTP method to use
            expected_response_codes: List of acceptable response codes

        Returns:
            Message: The response message from the server

        Raises:
            ValidationCodeException: If response code is not in expected_response_codes
        """
        return self._request(
            str(uuid.uuid4()), body, path, method, expected_response_codes
        )

    def _request(
        self,
        id: str,
        body: Any,
        path: str,
        method: str,
        expected_response_codes: list[int],
    ) -> Message:
        amq_message = Message(
            id=id,
            body=body,
            reply_to="$me",
            address=path,
            subject=method,
        )

        if self._sender is not None:
            logger.debug("Sending message: " + str(amq_message))
            self._sender.send(amq_message)

        if self._receiver is not None:
            msg = self._receiver.receive()
            logger.debug("Received message: " + str(msg))

        self._validate_reponse_code(int(msg.subject), expected_response_codes)
        return msg

    def declare_exchange(
        self,
        exchange_specification: Union[
            ExchangeSpecification, ExchangeCustomSpecification
        ],
    ) -> Union[ExchangeSpecification, ExchangeCustomSpecification]:
        """
        Declare a new exchange in RabbitMQ.

        Args:
            exchange_specification: The specification for the exchange to create

        Returns:
            The same specification object that was passed in

        Raises:
            ValidationCodeException: If exchange already exists or other validation fails
        """
        logger.debug("declare_exchange operation called")
        body: dict[str, Any] = {}
        body["auto_delete"] = exchange_specification.is_auto_delete
        body["durable"] = exchange_specification.is_durable
        if isinstance(exchange_specification, ExchangeSpecification):
            body["type"] = exchange_specification.exchange_type.value
        elif isinstance(exchange_specification, ExchangeCustomSpecification):
            body["type"] = exchange_specification.exchange_type
        body["internal"] = exchange_specification.is_internal
        body["arguments"] = exchange_specification.arguments

        path = AddressHelper.exchange_address(exchange_specification.name)

        self.request(
            body,
            path,
            CommonValues.command_put.value,
            [
                CommonValues.response_code_201.value,
                CommonValues.response_code_204.value,
                CommonValues.response_code_409.value,
            ],
        )

        return exchange_specification

    def declare_queue(
        self,
        queue_specification: Union[
            ClassicQueueSpecification, QuorumQueueSpecification, StreamSpecification
        ],
    ) -> Union[
        ClassicQueueSpecification, QuorumQueueSpecification, StreamSpecification
    ]:
        """
        Declare a new queue in RabbitMQ.

        Supports declaration of classic queues, quorum queues, and streams.

        Args:
            queue_specification: The specification for the queue to create

        Returns:
            The same specification object that was passed in

        Raises:
            ValidationCodeException: If queue already exists or other validation fails
        """
        logger.debug("declare_queue operation called")

        if isinstance(queue_specification, ClassicQueueSpecification) or isinstance(
            queue_specification, QuorumQueueSpecification
        ):
            body = self._declare_queue(queue_specification)

        elif isinstance(queue_specification, StreamSpecification):
            body = self._declare_stream(queue_specification)

        path = AddressHelper.queue_address(queue_specification.name)

        self.request(
            body,
            path,
            CommonValues.command_put.value,
            [
                CommonValues.response_code_200.value,
                CommonValues.response_code_201.value,
                CommonValues.response_code_409.value,
            ],
        )

        return queue_specification

    def _declare_queue(
        self,
        queue_specification: Union[ClassicQueueSpecification, QuorumQueueSpecification],
    ) -> dict[str, Any]:

        body = {}
        args: dict[str, Any] = {}

        if queue_specification.dead_letter_exchange is not None:
            args["x-dead-letter-exchange"] = queue_specification.dead_letter_exchange
        if queue_specification.dead_letter_routing_key is not None:
            args["x-dead-letter-routing-key"] = (
                queue_specification.dead_letter_routing_key
            )
        if queue_specification.overflow_behaviour is not None:
            args["x-overflow"] = queue_specification.overflow_behaviour
        if queue_specification.max_len is not None:
            args["x-max-length"] = queue_specification.max_len
        if queue_specification.max_len_bytes is not None:
            args["x-max-length-bytes"] = queue_specification.max_len_bytes
        if queue_specification.message_ttl is not None:
            args["x-message-ttl"] = int(
                queue_specification.message_ttl.total_seconds() * 1000
            )
        if queue_specification.auto_expires is not None:
            args["x-expires"] = int(
                queue_specification.auto_expires.total_seconds() * 1000
            )
        if queue_specification.single_active_consumer is not None:
            args["x-single-active-consumer"] = (
                queue_specification.single_active_consumer
            )

        if isinstance(queue_specification, ClassicQueueSpecification):
            body["auto_delete"] = queue_specification.is_auto_delete
            body["durable"] = queue_specification.is_durable
            body["exclusive"] = queue_specification.is_exclusive

            args["x-queue-type"] = QueueType.classic.value
            if queue_specification.max_priority is not None:
                args["x-max-priority"] = queue_specification.max_priority

        if isinstance(queue_specification, QuorumQueueSpecification):
            args["x-queue-type"] = QueueType.quorum.value
            if queue_specification.deliver_limit is not None:
                args["x-deliver-limit"] = queue_specification.deliver_limit

            if queue_specification.dead_letter_strategy is not None:
                args["x-dead-letter-strategy"] = (
                    queue_specification.dead_letter_strategy
                )

            if queue_specification.quorum_initial_group_size is not None:
                args["x-quorum-initial-group-size"] = (
                    queue_specification.quorum_initial_group_size
                )

            if queue_specification.cluster_target_group_size is not None:
                args["x-quorum-target-group-size"] = (
                    queue_specification.cluster_target_group_size
                )

            if queue_specification.leader_locator is not None:
                args["x-queue-leader-locator"] = queue_specification.leader_locator

        body["arguments"] = args  # type: ignore

        return body

    def _declare_stream(
        self, stream_specification: StreamSpecification
    ) -> dict[str, Any]:

        body = {}
        args: dict[str, Any] = {}

        args["x-queue-type"] = QueueType.stream.value

        if stream_specification.max_len_bytes is not None:
            args["x-max-length-bytes"] = stream_specification.max_len_bytes

        if stream_specification.max_age is not None:
            args["x-max-age"] = (
                str(int(stream_specification.max_age.total_seconds())) + "s"
            )

        if stream_specification.stream_max_segment_size_bytes is not None:
            args["x-stream-max-segment-size-bytes"] = (
                stream_specification.stream_max_segment_size_bytes
            )

        if stream_specification.stream_filter_size_bytes is not None:
            args["x-stream-filter-size-bytes"] = (
                stream_specification.stream_filter_size_bytes
            )

        if stream_specification.initial_group_size is not None:
            args["x-initial-group-size"] = stream_specification.initial_group_size

        if stream_specification.leader_locator is not None:
            args["x-queue-leader-locator"] = stream_specification.leader_locator

        body["arguments"] = args

        return body

    def delete_exchange(self, name: str) -> None:
        """
        Delete an exchange.

        Args:
            name: The name of the exchange to delete

        Raises:
            ValidationCodeException: If exchange doesn't exist or deletion fails
        """
        logger.debug("delete_exchange operation called")
        path = AddressHelper.exchange_address(name)

        self.request(
            None,
            path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_204.value,
            ],
        )

    def delete_queue(self, name: str) -> None:
        """
        Delete a queue.

        Args:
            name: The name of the queue to delete

        Raises:
            ValidationCodeException: If queue doesn't exist or deletion fails
        """
        logger.debug("delete_queue operation called")
        path = AddressHelper.queue_address(name)

        self.request(
            None,
            path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_200.value,
            ],
        )

    def _validate_reponse_code(
        self, response_code: int, expected_response_codes: list[int]
    ) -> None:
        if response_code == CommonValues.response_code_409.value:
            raise ValidationCodeException("ErrPreconditionFailed")

        for code in expected_response_codes:
            if code == response_code:
                return None

        raise ValidationCodeException(
            "wrong response code received: " + str(response_code)
        )

    def bind(
        self,
        bind_specification: Union[
            ExchangeToQueueBindingSpecification, ExchangeToExchangeBindingSpecification
        ],
    ) -> str:
        """
        Create a binding between exchanges or between an exchange and a queue.

        Args:
            bind_specification: The specification for the binding to create

        Returns:
            str: The binding path created

        Raises:
            ValidationCodeException: If binding creation fails
        """
        logger.debug("Bind Operation called")

        body = {}
        if bind_specification.binding_key is not None:
            body["binding_key"] = bind_specification.binding_key
        else:
            body["binding_key"] = ""
        body["source"] = bind_specification.source_exchange
        if isinstance(bind_specification, ExchangeToQueueBindingSpecification):
            body["destination_queue"] = bind_specification.destination_queue
        elif isinstance(bind_specification, ExchangeToExchangeBindingSpecification):
            body["destination_exchange"] = bind_specification.destination_exchange

        body["arguments"] = {}  # type: ignore

        path = AddressHelper.path_address()

        self.request(
            body,
            path,
            CommonValues.command_post.value,
            [
                CommonValues.response_code_204.value,
            ],
        )
        binding_path = ""

        if isinstance(bind_specification, ExchangeToQueueBindingSpecification):
            binding_path = AddressHelper.binding_path_with_exchange_queue(
                bind_specification
            )
        elif isinstance(bind_specification, ExchangeToExchangeBindingSpecification):
            binding_path = AddressHelper.binding_path_with_exchange_exchange(
                bind_specification
            )

        return binding_path

    def unbind(
        self,
        bind_specification: Union[
            str,
            ExchangeToQueueBindingSpecification,
            ExchangeToExchangeBindingSpecification,
        ],
    ) -> None:
        """
        Remove a binding between exchanges or between an exchange and a queue.

        Args:
            bind_specification: Either a binding path string or a binding specification

        Raises:
            ValidationCodeException: If unbinding fails
        """
        logger.debug("UnBind Operation called")
        binding_name = ""
        if isinstance(bind_specification, str):
            binding_name = bind_specification
        else:
            if isinstance(bind_specification, ExchangeToQueueBindingSpecification):
                binding_name = AddressHelper.binding_path_with_exchange_queue(
                    bind_specification
                )
            elif isinstance(bind_specification, ExchangeToExchangeBindingSpecification):
                binding_name = AddressHelper.binding_path_with_exchange_exchange(
                    bind_specification
                )
        self.request(
            None,
            binding_name,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_204.value,
            ],
        )

    def purge_queue(self, name: str) -> int:
        """
        Purge all messages from a queue.

        Args:
            name: The name of the queue to purge

        Returns:
            int: The number of messages that were purged

        Raises:
            ValidationCodeException: If queue doesn't exist or purge fails
        """
        logger.debug("purge_queue operation called")
        path = AddressHelper.purge_queue_address(name)

        response = self.request(
            None,
            path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_200.value,
            ],
        )

        return int(response.body["message_count"])

    def queue_info(self, name: str) -> QueueInfo:
        """
        Get information about a queue.

        Args:
            name: The name of the queue to get information about

        Returns:
            QueueInfo: Object containing queue information

        Raises:
            ValidationCodeException: If queue doesn't exist or other errors occur
        """
        logger.debug("queue_info operation called")
        path = AddressHelper.queue_address(name)

        message = self.request(
            None,
            path,
            CommonValues.command_get.value,
            [
                CommonValues.response_code_200.value,
            ],
        )

        queue_info: dict[str, Any] = message.body

        if queue_info["type"] == "quorum":
            queue_type = QueueType.quorum
        elif queue_info["type"] == "stream":
            queue_type = QueueType.stream
        else:
            queue_type = QueueType.classic

        return QueueInfo(
            name=queue_info["name"],
            is_durable=queue_info["durable"],
            is_auto_delete=queue_info["auto_delete"],
            is_exclusive=queue_info["exclusive"],
            queue_type=queue_type,
            leader=queue_info["leader"],
            members=queue_info["replicas"],
            arguments=queue_info["arguments"],
            message_count=queue_info["message_count"],
            consumer_count=queue_info["consumer_count"],
        )
