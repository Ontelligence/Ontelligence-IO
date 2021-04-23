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


# TODO: Fernet key.
