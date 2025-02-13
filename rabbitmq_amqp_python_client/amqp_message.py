from typing import Union, cast
from uuid import UUID

from proton._data import Described

from .qpid.proton._message import Message


class AmqpMessage(Message):  # type: ignore

    def __init__(  # type: ignore
        self,
        body: Union[  # type: ignore
            bytes, str, dict, list, int, float, "UUID", "Described", None
        ] = None,
        **kwargs,
    ):
        super().__init__(body=body, **kwargs)
        self._addr: str = ""
        self._native_message = None

    def to_address(self, addr: str) -> None:
        self.address = addr
        self._addr = addr

    def get_address(self) -> str:
        return self._addr

    def native_message(self) -> Message:
        return cast(Message, self)
