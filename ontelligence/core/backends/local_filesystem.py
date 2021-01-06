import os
import json
from typing import Optional, Any, Dict

from .base import BaseBackend, NonexistentKey
from ontelligence.core.config import settings


class LocalFilesystemBackend(BaseBackend):
    """Retrieves Connection objects and Variables from local files"""

    def __init__(self, schema_class=None):
        super().__init__(schema_class=schema_class)

    @property
    def _get_key(self) -> Dict[str, str]:
        if not os.path.exists(settings.HOME_PATH):
            os.makedirs(settings.HOME_PATH, exist_ok=True)

        file_path = os.path.join(settings.HOME_PATH, 'secrets.json')
        if not os.path.exists(file_path):
            raise Exception(f'Secrets backend is missing at ~/.ontelligence/secrets.json')

        with open(file_path, 'r') as f:
            secrets = json.loads(f.read())
        return secrets

    def get_key(self, key: str) -> Optional[str]:
        return self._get_key.get(key)
