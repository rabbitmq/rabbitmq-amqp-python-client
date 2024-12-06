import uuid
from typing import Any, Optional

from proton import Message, Receiver, Sender
from proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)

from .common import CommonValues
from .configuration_options import (
    ReceiverOption,
    SenderOption,
)


class Management:
    def __init__(self, conn: BlockingConnection):
        self._sender: Optional[Sender] = None
        self._receiver: Optional[Receiver] = None
        self._conn = conn

    def open(self) -> None:
        if self._sender is None:
            self._sender = self._create_sender(
                CommonValues.management_node_address.value,
                sender_option=SenderOption(CommonValues.management_node_address.value),
            )
        if self._receiver is None:
            self._receiver = self._create_receiver(
                CommonValues.management_node_address.value,
                receiver_option=ReceiverOption(
                    CommonValues.management_node_address.value
                ),
            )

    def _create_sender(self, addr: str, sender_option: SenderOption) -> BlockingSender:
        return self._conn.create_sender(addr, options=sender_option)

    def _create_receiver(
        self, addr: str, receiver_option: ReceiverOption
    ) -> BlockingReceiver:
        return self._conn.create_receiver(addr, options=receiver_option)

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

        # TO_COMPLETE HERE

    # TODO
    # def declare_queue(self, name:str):

    # TODO
    # def delete_queue(self, name:str):

    # TODO
    # def declare_exchange(self, name:str):

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
