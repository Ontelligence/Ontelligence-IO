from typing import Union, Any
from datetime import datetime, timedelta

from jose import jwt
import ldap3


def try_ldap_login(ldap_provider_url: str, ldap_protocol_version: int, username: str, password: str):
    server = ldap3.Server(host=ldap_provider_url, get_info=ldap3.ALL)
    conn = ldap3.Connection(server=server, version=ldap_protocol_version, user=username, password=password)

    try:
        bind = conn.bind()
    except Exception as e:
        raise e
    finally:
        conn.unbind()

    return bind


def create_access_token(
        secret_key: str,
        # algorithm: str,
        subject: Union[str, Any],
        expires_in: int = None,
) -> str:
    expire = datetime.utcnow() + timedelta(seconds=expires_in)
    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm='HS256')
    return encoded_jwt




# TODO: Fernet key.
