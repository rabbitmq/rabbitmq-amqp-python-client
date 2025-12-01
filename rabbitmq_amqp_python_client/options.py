from typing import Optional

from .entities import ConsumerOptions
from .qpid.proton._data import (  # noqa: E402
    Data,
    PropertyDict,
    symbol,
)
from .qpid.proton._endpoints import (  # noqa: E402
    Link,
    Terminus,
)
from .qpid.proton.reactor import (  # noqa: E402
    Filter,
    LinkOption,
)


class SenderOption(LinkOption):  # type: ignore
    def __init__(self, addr: str):
        self._addr = addr

    def apply(self, link: Link) -> None:
        link.source.address = self._addr
        link.snd_settle_mode = Link.SND_SETTLED
        link.rcv_settle_mode = Link.RCV_FIRST
        link.properties = PropertyDict({symbol("paired"): True})
        link.source.dynamic = False

    def test(self, link: Link) -> bool:
        return bool(link.is_sender)


class SenderOptionUnseattle(LinkOption):  # type: ignore
    def __init__(self, addr: str):
        self._addr = addr

    def apply(self, link: Link) -> None:
        link.source.address = self._addr
        link.snd_settle_mode = Link.SND_UNSETTLED
        link.rcv_settle_mode = Link.RCV_FIRST
        link.properties = PropertyDict({symbol("paired"): True})
        link.source.dynamic = False

    def test(self, link: Link) -> bool:
        return bool(link.is_sender)


class ReceiverOption(LinkOption):  # type: ignore
    def __init__(self, addr: str):
        self._addr = addr

    def apply(self, link: Link) -> None:
        link.target.address = self._addr
        link.snd_settle_mode = Link.SND_SETTLED
        link.rcv_settle_mode = Link.RCV_FIRST
        link.properties = PropertyDict({symbol("paired"): True})
        link.source.dynamic = False


class DynamicReceiverOption(LinkOption):  # type: ignore

    def apply(self, link: Link) -> None:
        link.snd_settle_mode = Link.SND_SETTLED
        # link.rcv_settle_mode = Link.RCV_FIRST
        link.source.expiry_policy = Terminus.EXPIRE_WITH_LINK
        link.properties = PropertyDict({symbol("paired"): True})
        link.source.dynamic = True
        data = link.source.capabilities
        data.put_array(False, Data.SYMBOL)
        data.enter()
        data.put_string("rabbitmq:volatile-queue")
        data.exit()


class ReceiverOptionUnsettled(LinkOption):  # type: ignore
    def __init__(self, addr: Optional[str]):
        self._addr = addr

    def apply(self, link: Link) -> None:
        link.target.address = self._addr
        link.snd_settle_mode = Link.SND_UNSETTLED
        link.rcv_settle_mode = Link.RCV_FIRST
        link.properties = PropertyDict({symbol("paired"): True})
        link.source.dynamic = False

    def test(self, link: Link) -> bool:
        return bool(link.is_receiver)


class ReceiverOptionUnsettledWithFilters(Filter):  # type: ignore
    def __init__(self, addr: Optional[str], consumer_options: ConsumerOptions):
        super().__init__(consumer_options.filter_set())
        self._addr = addr

    def apply(self, link: Link) -> None:
        link.target.address = self._addr
        link.snd_settle_mode = Link.SND_UNSETTLED
        link.rcv_settle_mode = Link.RCV_FIRST
        link.properties = PropertyDict({symbol("paired"): True})
        link.source.dynamic = False
        link.source.filter.put_dict(self.filter_set)

    def test(self, link: Link) -> bool:
        return bool(link.is_receiver)
