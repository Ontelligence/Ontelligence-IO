import json
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict


class NonexistentKey(Exception):
    def __init__(self, key: str):
        super().__init__(f'Key "{key}" does not exist.')


class BaseBackend(ABC):
    """Abstract base class to retrieve secrets given a conn_id and construct a Connection object"""

    schema_class = None

    def __init__(self, schema_class):
        self.schema_class = schema_class

    @abstractmethod
    def get_key(self, key: str) -> Any:
        """Return value for key"""
        raise NotImplementedError

    def parse_key(self, data_class, key: str, override_data: Optional[Dict[str, Any]] = None) -> Any:
        """Return parsed value for key"""
        val = self.get_key(key=key)
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                pass
        if self.schema_class:
            parsed_val = self.schema_class.from_dict(data=val)
            if override_data:
                # TODO: Field name "schema" shadows Pydantic attribute; use a different field name with "alias='schema'"
                if 'schema' in override_data:
                    override_data['db_schema'] = override_data['schema']
                    override_data.pop('schema')
                parsed_val.data = data_class.from_dict(data={**parsed_val.data, **override_data})
            else:
                parsed_val.data = data_class.from_dict(data=parsed_val.data)
            return parsed_val
        return data_class.from_dict(data=val)
