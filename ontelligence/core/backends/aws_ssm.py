from typing import Optional

import boto3

from .base import BaseBackend, NonexistentKey


class AwsSystemsManagerBackend(BaseBackend):
    """Retrieves objects from AWS SSM"""

    def __init__(self, schema_class=None):
        super().__init__(schema_class=schema_class)

    @property
    def _get_key(self) -> boto3.client:
        return boto3.client('ssm')

    def get_key(self, key: str) -> Optional[str]:
        try:
            res = self._get_key.get_parameter(Name=key, WithDecryption=True)
            return res["Parameter"]["Value"]
        except Exception:
            raise NonexistentKey(key=key)
