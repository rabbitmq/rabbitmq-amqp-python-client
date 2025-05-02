from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class PosixClientCert:
    client_cert: str
    client_key: str
    password: Optional[str] = None


@dataclass
class Unambiguous:
    """Use the only certificate in the store."""

    ...


@dataclass
class FriendlyName:
    """Use the first certificate with a matching friendly name."""

    name: str


@dataclass
class LocalMachineStore:
    name: str


@dataclass
class CurrentUserStore:
    name: str


@dataclass
class PKCS12Store:
    path: str


@dataclass
class WinClientCert:
    store: Union[LocalMachineStore, CurrentUserStore, PKCS12Store]
    disambiguation_method: Union[Unambiguous, FriendlyName]
    password: Optional[str] = None


@dataclass
class PosixSslConfigurationContext:
    ca_cert: str
    client_cert: Union[PosixClientCert, None] = None


@dataclass
class WinSslConfigurationContext:
    ca_store: Union[LocalMachineStore, CurrentUserStore, PKCS12Store]
    client_cert: Union[WinClientCert, None] = None
