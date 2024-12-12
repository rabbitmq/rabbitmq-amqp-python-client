import uuid
from typing import Any, Optional

from proton import Message
from proton._data import Data
from proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)

from .address_helper import (
    binding_path_with_exchange_queue,
    exchange_address,
    path_address,
    queue_address,
)
from .common import CommonValues
from .entities import (
    BindingSpecification,
    ExchangeSpecification,
    QueueSpecification,
)
from .exceptions import ValidationCodeException
from .options import ReceiverOption, SenderOption


class Management:
    def __init__(self, conn: BlockingConnection):
        self._sender: Optional[BlockingSender] = None
        self._receiver: Optional[BlockingReceiver] = None
        self._conn = conn

    def open(self) -> None:
        if self._sender is None:
            self._sender = self._create_sender(
                CommonValues.management_node_address.value
            )
        if self._receiver is None:
            self._receiver = self._create_receiver(
                CommonValues.management_node_address.value,
            )

    def _create_sender(self, addr: str) -> BlockingSender:
        return self._conn.create_sender(addr, options=SenderOption(addr))

    def _create_receiver(self, addr: str) -> BlockingReceiver:
        return self._conn.create_receiver(addr, options=ReceiverOption(addr))

    # closes the connection to the AMQP 1.0 server.
    def close(self) -> None:
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
    ) -> None:
        self._request(str(uuid.uuid4()), body, path, method, expected_response_codes)

    def _request(
        self,
        id: str,
        body: Any,
        path: str,
        method: str,
        expected_response_codes: list[int],
    ) -> None:
        amq_message = Message(
            id=id,
            body=body,
            reply_to="$me",
            address=path,
            subject=method,
        )

        if self._sender is not None:
            self._sender.send(amq_message)

        if self._receiver is not None:
            msg = self._receiver.receive()

        self._validate_reponse_code(int(msg.subject), expected_response_codes)

    def declare_exchange(
        self, exchange_specification: ExchangeSpecification
    ) -> ExchangeSpecification:
        body = {}
        body["auto_delete"] = exchange_specification.is_auto_delete
        body["durable"] = exchange_specification.is_durable
        body["type"] = exchange_specification.exchange_type.value  # type: ignore
        body["internal"] = exchange_specification.is_internal
        body["arguments"] = {}  # type: ignore

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
        body = {}
        body["auto_delete"] = queue_specification.is_auto_delete
        body["durable"] = queue_specification.is_durable
        body["arguments"] = {  # type: ignore
            "x-queue-type": queue_specification.queue_type.value,
            "x-dead-letter-exchange": queue_specification.dead_letter_exchange,
            "x-dead-letter-routing-key": queue_specification.dead_letter_routing_key,
            "max-length-bytes": queue_specification.max_len_bytes,
        }

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
        path = exchange_address(exchange_name)

        print(path)

        self.request(
            Data.NULL,
            path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_200.value,
            ],
        )

    def delete_queue(self, queue_name: str) -> None:
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
        print("response_code received: " + str(response_code))
        if response_code == CommonValues.response_code_409.value:
            # TODO replace with a new defined Exception
            raise ValidationCodeException("ErrPreconditionFailed")

        for code in expected_response_codes:
            if code == response_code:
                return None

        raise ValidationCodeException(
            "wrong response code received: " + str(response_code)
        )

    # TODO
    def bind(self, bind_specification: BindingSpecification) -> str:
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

    # TODO
    def unbind(self, binding_exchange_queue_path: str) -> None:
        self.request(
            None,
            binding_exchange_queue_path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_200.value,
            ],
        )

    # TODO
    # def queue_info(self, queue_name:str):

    # TODO
    # def purge_queue(self, queue_name:str):