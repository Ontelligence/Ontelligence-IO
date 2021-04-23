from typing import Optional

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass


@dataclass
class SQLServerSecret(BaseDataClass):
    username: str
    password: Optional[str]


@dataclass
class SQLServerConnection(BaseDataClass):
    server: str
    database: Optional[str]
    db_schema: Optional[str]
