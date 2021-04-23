import logging
# try:
import pyodbc
# except:
#     logging.warning('Python package pyodbc is not installed.')
from cached_property import cached_property

from ontelligence.core.schemas.sqlserver import SQLServerConnection, SQLServerSecret
from ontelligence.providers.base import BaseProvider


class BaseSQLServerProvider(BaseProvider):

    server = None
    database = None
    schema = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(secret_schema=SQLServerSecret, connection_schema=SQLServerConnection)
        self.conn_id = conn_id
        self.kwargs = kwargs

    @cached_property
    def conn(self):
        conn = self._get_connection(conn_id=self.conn_id, override_data=self.kwargs)
        secret = self._get_secret(secret_id=conn.secret, override_data=self.kwargs)

        self.server = self.kwargs.get('server', conn.data.server)
        self.database = self.kwargs.get('database', conn.data.database)
        self.schema = self.kwargs.get('schema', conn.data.db_schema)

        username = self.kwargs.get('username', secret.data.username)
        password = self.kwargs.get('password', secret.data.password)

        _drivers = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]
        if not _drivers:
            raise Exception('No suitable driver found. Cannot connect.')
        driver = _drivers[0]

        # if windows_auth:
        #     _conn_str = f'DRIVER={driver};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;'

        # elif port:
        #     _conn_str = f'DRIVER={driver};SERVER={self.server};PORT={port};DATABASE={self.database};UID={username};PWD={password};'

        # else:
        _conn_str = f'DRIVER={driver};SERVER={self.server};DATABASE={self.database};UID={username};PWD={password};'

        try:
            print(_conn_str)
            return pyodbc.connect(_conn_str)
        except pyodbc.Error as err:
            raise Exception(f'Could not connect to database: {err}')
