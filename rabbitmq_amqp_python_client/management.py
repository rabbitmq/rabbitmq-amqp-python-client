import logging
import uuid
from typing import Any, Optional

from .address_helper import (
    binding_path_with_exchange_queue,
    exchange_address,
    path_address,
    purge_queue_address,
    queue_address,
)
from .common import CommonValues, QueueType
from .entities import (
    BindingSpecification,
    ExchangeSpecification,
    QueueInfo,
    QueueSpecification,
)
from .exceptions import ValidationCodeException
from .options import ReceiverOption, SenderOption
from .qpid.proton._message import Message
from .qpid.proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)

logger = logging.getLogger(__name__)


class Management:
    def __init__(self, conn: BlockingConnection):
        self._sender: Optional[BlockingSender] = None
        self._receiver: Optional[BlockingReceiver] = None
        self._conn = conn

    def open(self) -> None:
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
        self, exchange_specification: ExchangeSpecification
    ) -> ExchangeSpecification:
        logger.debug("delete_exchange operation called")
        body = {}
        body["auto_delete"] = exchange_specification.is_auto_delete
        body["durable"] = exchange_specification.is_durable
        body["type"] = exchange_specification.exchange_type.value  # type: ignore
        body["internal"] = exchange_specification.is_internal
        body["arguments"] = exchange_specification.arguments  # type: ignore

        path = exchange_address(exchange_specification.name)

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
        self, queue_specification: QueueSpecification
    ) -> QueueSpecification:
        logger.debug("declare_queue operation called")
        body = {}
        args: dict[str, Any] = {}

        body["auto_delete"] = queue_specification.is_auto_delete
        body["durable"] = queue_specification.is_durable
        args["x-queue-type"] = queue_specification.queue_type.value
        if queue_specification.dead_letter_exchange is not None:
            args["x-dead-letter-exchange"] = queue_specification.dead_letter_exchange
        if queue_specification.dead_letter_routing_key is not None:
            args["x-dead-letter-routing-key"] = (
                queue_specification.dead_letter_routing_key
            )
        if queue_specification.overflow is not None:
            args["x-overflow"] = queue_specification.overflow
        if queue_specification.max_len is not None:
            args["x-max-length"] = queue_specification.max_len
        if queue_specification.max_len_bytes is not None:
            args["x-max-length-bytes"] = queue_specification.max_len_bytes
        if queue_specification.message_ttl is not None:
            args["x-message-ttl"] = queue_specification.message_ttl
        if queue_specification.expires is not None:
            args["x-expires"] = queue_specification.expires
        if queue_specification.single_active_consumer is not None:
            args["x-single-active-consumer"] = (
                queue_specification.single_active_consumer
            )

        body["arguments"] = args  # type: ignore

        path = queue_address(queue_specification.name)

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

    def delete_exchange(self, exchange_name: str) -> None:
        logger.debug("delete_exchange operation called")
        path = exchange_address(exchange_name)

        self.request(
            None,
            path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_204.value,
            ],
        )

    def delete_queue(self, queue_name: str) -> None:
        logger.debug("delete_queue operation called")
        path = queue_address(queue_name)

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
        logger.debug("response_code received: " + str(response_code))
        if response_code == CommonValues.response_code_409.value:
            # TODO replace with a new defined Exception
            raise ValidationCodeException("ErrPreconditionFailed")

        for code in expected_response_codes:
            if code == response_code:
                return None

        raise ValidationCodeException(
            "wrong response code received: " + str(response_code)
        )

    def bind(self, bind_specification: BindingSpecification) -> str:
        logger.debug("Bind Operation called")
        body = {}
        body["binding_key"] = bind_specification.binding_key
        body["source"] = bind_specification.source_exchange
        body["destination_queue"] = bind_specification.destination_queue
        body["arguments"] = {}  # type: ignore

        path = path_address()

        self.request(
            body,
            path,
            CommonValues.command_post.value,
            [
                CommonValues.response_code_204.value,
            ],
        )

        binding_path_with_queue = binding_path_with_exchange_queue(bind_specification)
        return binding_path_with_queue

    def unbind(self, binding_exchange_queue_path: str) -> None:
        logger.debug("UnBind Operation called")
        self.request(
            None,
            binding_exchange_queue_path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_204.value,
            ],
        )

    def purge_queue(self, queue_name: str) -> None:
        logger.debug("purge_queue operation called")
        path = purge_queue_address(queue_name)

        self.request(
            None,
            path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_200.value,
            ],
        )

    def queue_info(self, queue_name: str) -> QueueInfo:
        logger.debug("queue_info operation called")
        path = queue_address(queue_name)

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
        )
