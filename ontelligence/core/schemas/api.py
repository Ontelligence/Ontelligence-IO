from typing import Optional, List

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass


@dataclass
class Token(BaseDataClass):
    token: str
