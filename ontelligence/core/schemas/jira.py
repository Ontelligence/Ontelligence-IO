from typing import Optional

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass


@dataclass
class JiraSecret(BaseDataClass):
    email: str
    api_token: str


@dataclass
class JiraEpicProperties(BaseDataClass):
    custom_field: Optional[str] = None


@dataclass
class JiraConnection(BaseDataClass):
    server: str
    project: Optional[str] = None
    epic: Optional[JiraEpicProperties] = None
