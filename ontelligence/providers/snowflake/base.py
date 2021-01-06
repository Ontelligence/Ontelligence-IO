import snowflake.connector
from cached_property import cached_property
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption

from ontelligence.core.schemas.snowflake import SnowflakeConnection, SnowflakeSecret
from ontelligence.providers.base import BaseProvider


class BaseSnowflakeProvider(BaseProvider):

    account = None
    role = None
    warehouse = None
    database = None
    schema = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(secret_schema=SnowflakeSecret, connection_schema=SnowflakeConnection)
        self.conn_id = conn_id
        self.kwargs = kwargs

    @cached_property
    def conn(self):
        conn = self._get_connection(conn_id=self.conn_id, override_data=self.kwargs)
        secret = self._get_secret(secret_id=conn.secret, override_data=self.kwargs)

        self.account = self.kwargs.get('account', conn.data.account)
        self.role = self.kwargs.get('role', conn.data.role)
        self.warehouse = self.kwargs.get('warehouse', conn.data.warehouse)
        self.database = self.kwargs.get('database', conn.data.database)
        self.schema = self.kwargs.get('schema', conn.data.db_schema)

        user = self.kwargs.get('user', secret.data.user)
        private_key = self.kwargs.get('private_key', secret.data.ssh.private_key)
        pass_phrase = self.kwargs.get('pass_phrase', secret.data.ssh.pass_phrase)

        pkb = load_pem_private_key(private_key.encode(), password=pass_phrase.encode(), backend=default_backend()) \
            .private_bytes(encoding=Encoding.DER, format=PrivateFormat.PKCS8, encryption_algorithm=NoEncryption())

        connection_config = {
            'account': self.account,
            'user': user,
            'private_key': pkb,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema,
        }

        return snowflake.connector.connect(**connection_config)
