from dataclasses import dataclass
from typing import Optional


@dataclass
class ClientCert:
    client_cert: str
    client_key: str
    password: Optional[str] = None


@dataclass
class SSlConfigurationContext:
    ca_cert: str
    client_cert: Optional[ClientCert] = None
