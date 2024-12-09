import uuid
from typing import Any, Optional

from proton import Message, Receiver, Sender
from proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)

from .address_helper import exchange_address, queue_address
from .common import CommonValues
from .configuration_options import (
    ReceiverOption,
    SenderOption,
)
from .entities import (
    ExchangeSpecification,
    QueueSpecification,
)


class Management:
    def __init__(self, conn: BlockingConnection):
        self._sender: Optional[Sender] = None
        self._receiver: Optional[Receiver] = None
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
        print("im in request")
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
            properties={"id": id, "to": path, "subject": method, "reply_to": "$me"},
        )

        print("message: " + str(amq_message))

        if self._sender is not None:
            print("sending: " + method)
            self._sender.send(amq_message)

        msg = self._receiver.receive()

        print("received " + str(msg))

        # TO_COMPLETE HERE

    # TODO
    # def delete_queue(self, name:str):

    def declare_exchange(self, exchange_specification: ExchangeSpecification):
        body = {}
        body["auto_delete"] = exchange_specification.is_auto_delete
        body["durable"] = exchange_specification.is_durable
        body["type"] = exchange_specification.exchange_type.value
        body["internal"] = False
        body["arguments"] = exchange_specification.arguments

        path = exchange_address(exchange_specification.name)

        print(path)

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

    def declare_queue(self, queue_specification: QueueSpecification):
        body = {}
        body["auto_delete"] = queue_specification.is_auto_delete
        body["durable"] = queue_specification.is_durable
        body["arguments"] = {
            "x-queue-type": queue_specification.queue_type.value,
            "x-dead-letter-exchange": queue_specification.dead_letter_exchange,
            "x-dead-letter-routing-key": queue_specification.dead_letter_routing_key,
            "max-length-bytes": queue_specification.max_len_bytes,
        }

        path = queue_address(queue_specification.name)

        print(path)

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

    # TODO
    # def delete_exchange(self, name:str):

    # TODO
    # def bind(self, bind_specification:BindSpecification):

    # TODO
    # def unbind(self, binding_path:str):

    # TODO
    # def queue_info(self, queue_name:str):

    # TODO
    # def purge_queue(self, queue_name:str):
