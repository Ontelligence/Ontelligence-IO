from typing import Optional

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass
from ontelligence.core.schemas.ssh import SSHPrivateKey


@dataclass
class FtpSecret(BaseDataClass):
    username: str
    password: str


@dataclass
class SftpSecret(BaseDataClass):
    username: str
    ssh: Optional[SSHPrivateKey] = None
    password: Optional[str] = None


@dataclass
class FtpConnection(BaseDataClass):
    host: str
    port: Optional[str]
    path: Optional[str] = ''
