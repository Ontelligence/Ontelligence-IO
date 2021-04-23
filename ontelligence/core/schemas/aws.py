import os
import re
from typing import Optional

from pydantic.dataclasses import dataclass

from ontelligence.core.schemas.base import BaseDataClass


@dataclass
class _AwsAssumeRoleConfig(BaseDataClass):
    account_id: Optional[str]
    role_name: Optional[str]
    role_arn: Optional[str]
    external_id: Optional[str]
    method: Optional[str] = 'assume_role'


@dataclass
class AwsSecret(BaseDataClass):
    access_key: Optional[str]
    secret_access_key: Optional[str]
    session_token: Optional[str]
    assume_role_config: Optional[_AwsAssumeRoleConfig]


@dataclass
class AwsS3Connection(BaseDataClass):
    bucket: str
    prefix: Optional[str]


@dataclass
class S3Bucket(BaseDataClass):
    name: str


@dataclass
class S3Key(BaseDataClass):
    bucket: str
    prefix: Optional[str] = ''
    name: Optional[str] = ''

    def __post_init__(self):
        if self.prefix:
            assert str(self.prefix).endswith('/'), 'Prefix must end with "/"'
        if self.prefix == '/':
            self.prefix = ''

    @classmethod
    def from_path(cls, path: str):
        assert path.lower().startswith('s3://'), 'Path must begin with "s3://"'
        m = re.compile("s?S?3://([-a-zA-Z0-9]+)/(.*)")
        regex = m.search(path)
        bucket = regex.group(1)
        key = regex.group(2)
        prefix = os.path.dirname(key) + '/'
        return cls(bucket=bucket, prefix=prefix, name=os.path.split(key)[1])

    @property
    def path(self):
        return f's3://{self.bucket}/{self.prefix}{self.name}'

    @property
    def key(self):
        return f'{self.prefix}{self.name}'
