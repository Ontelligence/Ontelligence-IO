from cached_property import cached_property

from ontelligence.core.schemas.api import Token
from ontelligence.providers.base import BaseProvider


class BaseAppsFlyerProvider(BaseProvider):

    account = None
    role = None
    warehouse = None
    database = None
    schema = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(secret_schema=Token, connection_schema=None)
        self.conn_id = conn_id
        self.kwargs = kwargs

    @cached_property
    def conn(self):
        secret = self._get_secret(secret_id=self.conn_id, override_data=self.kwargs)
        token = self.kwargs.get('token', secret.data.token)
        return token
