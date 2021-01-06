import io
from typing import Optional

import paramiko
from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass


@dataclass
class SSHPrivateKey(BaseDataClass):
    private_key: str
    pass_phrase: Optional[str]

    def get_key_object(self):
        return paramiko.RSAKey.from_private_key(io.StringIO(self.private_key))
