from abc import ABC, abstractmethod
from typing import Optional, Any, Dict

from cached_property import cached_property

from ontelligence.core.backends import SecretsBackend, ConnectionsBackend
from ontelligence.utils.log import LoggingMixin


class BaseProvider(LoggingMixin):

    __secret_schema = None
    __connection_schema = None

    def __init__(self, secret_schema=None, connection_schema=None):
        self.__secret_schema = secret_schema
        self.__connection_schema = connection_schema

    def _get_connection(self, conn_id, override_data: Optional[Dict[str, Any]] = None):
        if not self.__connection_schema:
            raise ValueError('Please provide connection schema')
        self.log.info(f'Retrieving connection key: "{conn_id}"')
        return ConnectionsBackend().get_backend().parse_key(data_class=self.__connection_schema, key=conn_id, override_data=override_data)

    def _get_secret(self, secret_id, override_data: Optional[Dict[str, Any]] = None):
        if not self.__secret_schema:
            raise ValueError('Please provide secret schema')
        self.log.info(f'Retrieving secret key: "{secret_id}"')
        return SecretsBackend().get_backend().parse_key(data_class=self.__secret_schema, key=secret_id, override_data=override_data)

    @cached_property
    def conn(self):
        raise NotImplementedError

    def get_conn(self):
        return self.conn
