from typing import Optional, Any, Dict

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass


@dataclass
class Secret(BaseDataClass):
    name: str
    type: Optional[str]
    data: Dict[str, Any]


@dataclass
class Connection(BaseDataClass):
    name: str
    type: Optional[str]
    data: Dict[str, Any]
    secret: Optional[str]
