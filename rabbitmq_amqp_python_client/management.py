import uuid
from typing import Any, Optional
import json
from proton import Message, Receiver, Sender
from proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)

from proton._data import Data

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

import pickle

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
        print("path is: " + path)

        ## test exchange message
        amq_message = Message(
            id='84caea92-8e38-41d4-993f-de12b2a3d9a2',
            body=body,
            reply_to="$me",
            address=path,
            subject=method,
            #properties={"id": id, "to": path, "subject": method, "reply_to": "$me"},
        )

        ## test empty message
        amq_message = Message(
            #id='84caea92-8e38-41d4-993f-de12b2a3d9a2',
            body=Data.NULL,
            #reply_to="$me",
            #address=path,
            #subject=method,
            #properties={"id": id, "to": path, "subject": method, "reply_to": "$me"},
        )

        message_bytes= amq_message.encode()

        #print("received " + str(message_bytes.format(binary)))

        list_bytes = list(message_bytes)
        print("message: " + str(list_bytes))

        if self._sender is not None:
            self._sender.send(amq_message)

        msg = self._receiver.receive()

        #message_bytes= msg.encode()

        print("received " + str(msg))

        #self._validate_reponse_code(int(msg.properties["http:response"]), expected_response_codes)

        # TO_COMPLETE HERE

    # TODO
    # def delete_queue(self, name:str):

    def declare_exchange(self, exchange_specification: ExchangeSpecification):
        body = {}
        body["auto_delete"] = exchange_specification.is_auto_delete
        body["durable"] = exchange_specification.is_durable
        body["type"] = exchange_specification.exchange_type.value
        #body["internal"] = False
        body["arguments"] = {}

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

    def delete_exchange(self, exchange_name:str):

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


    def delete_queue(self, queue_name:str):

        path = queue_address(queue_name)

        print(path)

        self.request(
            None,
            path,
            CommonValues.command_delete.value,
            [
                CommonValues.response_code_200.value,
            ],
        )

    def _validate_reponse_code(self, response_code: int, expected_response_codes: list[int]) -> None:

        print("response code: " + str(response_code))

        if response_code == CommonValues.response_code_409:
            # TODO replace with a new defined Exception
            raise Exception("ErrPreconditionFailed")

        for code in expected_response_codes:
            if code == response_code:
                return None

        raise Exception("wrong response code received")


    # TODO
    # def bind(self, bind_specification:BindSpecification):

    # TODO
    # def unbind(self, binding_path:str):

    # TODO
    # def queue_info(self, queue_name:str):

    # TODO
    # def purge_queue(self, queue_name:str):
