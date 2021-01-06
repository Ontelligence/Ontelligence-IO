import os
import secrets
from typing import Any, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, BaseSettings, EmailStr, HttpUrl, PostgresDsn, validator


class Settings(BaseSettings):

    HOME_PATH = os.path.expanduser('~/.ontelligence/')
    # DEFAULT_BACKEND_TYPE: str = 'local'
    DEFAULT_BACKEND_TYPE: str = 'aws_ssm'


settings = Settings()
