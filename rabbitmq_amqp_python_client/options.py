from .entities import StreamOptions
from .qpid.proton._data import (  # noqa: E402
    PropertyDict,
    symbol,
)
from .qpid.proton._endpoints import Link  # noqa: E402
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


class ReceiverOptionUnsettled(LinkOption):  # type: ignore
    def __init__(self, addr: str):
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
    def __init__(self, addr: str, filter_options: StreamOptions):
        super().__init__(filter_options.filter_set())
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
