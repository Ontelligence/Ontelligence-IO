from typing import Optional

from ontelligence.core.config import settings
from ontelligence.core.schemas.backend import Secret, Connection
from ontelligence.core.backends.local_filesystem import LocalFilesystemBackend
from ontelligence.core.backends.aws_ssm import AwsSystemsManagerBackend


ALL_BACKENDS = {
    'local': LocalFilesystemBackend,
    'aws_ssm': AwsSystemsManagerBackend
}


class SecretsBackend:
    schema_class = Secret

    def get_backend(self, backend_type: Optional[str] = None):
        backend_type = backend_type if backend_type else settings.DEFAULT_BACKEND_TYPE
        return ALL_BACKENDS[backend_type](schema_class=self.schema_class)


class ConnectionsBackend:
    schema_class = Connection

    def get_backend(self, backend_type: Optional[str] = None):
        backend_type = backend_type if backend_type else settings.DEFAULT_BACKEND_TYPE
        return ALL_BACKENDS[backend_type](schema_class=self.schema_class)
