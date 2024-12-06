from typing import Any

from address_helper import exchange_address
from common import CommonValues, ExchangeTypes
from management import Management


class Exchange:
    def __init__(
        self,
        name: str,
        management: Management,
        exchange_type: ExchangeTypes,
        arguments: dict[str, Any],
        is_auto_delete: bool = False,
    ):
        self._name = name
        self._management = management
        self.exchange_type = exchange_type
        self._arguments = arguments
        self._is_auto_delete = is_auto_delete

    def declare(self) -> None:
        body = {}
        body["auto_delete"] = False
        body["durable"] = True
        body["type"] = self.exchange_type.value
        body["internal"] = False
        body["arguments"] = self._arguments

        path = exchange_address(self._name)

        self._management.request(
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
    # def delete(self):
