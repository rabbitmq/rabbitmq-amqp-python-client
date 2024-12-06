from configuration_options import (
    ReceiverOption,
    SenderOption,
)
from proton.utils import (
    BlockingConnection,
    BlockingReceiver,
    BlockingSender,
)


class Connection:
    def __init__(self, addr: str):
        self._addr: str = addr
        self._conn: BlockingConnection

    def dial(self) -> None:
        self._conn = BlockingConnection(self._addr)

    # closes the connection to the AMQP 1.0 server.
    def close(self) -> None:
        self._conn.close()

    def create_sender(self, addr: str, sender_option: SenderOption) -> BlockingSender:
        return self._conn.create_sender(addr, options=sender_option)

    def create_receiver(
        self, addr: str, receiver_option: ReceiverOption
    ) -> BlockingReceiver:
        return self._conn.create_receiver(addr, options=receiver_option)

    # TODO: returns the current status of the connection.
    # def status(self) -> int:
    #    pass

    # TODO: return management object
    # def management(self) -> Management:
    #    pass
