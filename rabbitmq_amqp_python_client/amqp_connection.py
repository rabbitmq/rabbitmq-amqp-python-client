import abc
from proton.utils import BlockingConnection

class Connection(abc.ABC):

    def __self__(self, addr: str):
        self._addr = addr

    def dial(self, addr: str) -> BlockingConnection:
        return BlockingConnection(self._addr)


    # closes the connection to the AMQP 1.0 server.
    def close(self) -> None:
        pass

    # returns the current status of the connection.
    def status(self) -> int:
        pass
