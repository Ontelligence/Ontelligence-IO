from typing import Optional

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass
from ontelligence.core.schemas.ssh import SSHPrivateKey


@dataclass
class SnowflakeSecret(BaseDataClass):
    user: str
    ssh: Optional[SSHPrivateKey]


@dataclass
class SnowflakeConnection(BaseDataClass):
    account: str
    role: Optional[str]
    warehouse: Optional[str]
    database: Optional[str]
    db_schema: Optional[str]
