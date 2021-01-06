from typing import Optional, List

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass


@dataclass
class Column(BaseDataClass):
    name: str
    dtype: str


@dataclass
class Table(BaseDataClass):
    database: str
    db_schema: str
    name: str
    columns: Optional[List[Column]] = None
