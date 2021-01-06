import ftplib
import paramiko
from cached_property import cached_property

from ontelligence.core.schemas.ftp import FtpConnection, FtpSecret, SftpSecret
from ontelligence.providers.base import BaseProvider


class BaseFtpProvider(BaseProvider):

    path = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(secret_schema=FtpSecret, connection_schema=FtpConnection)
        self.conn_id = conn_id
        self.kwargs = kwargs

    @cached_property
    def conn(self):
        conn = self._get_connection(conn_id=self.conn_id, override_data=self.kwargs)
        secret = self._get_secret(secret_id=conn.secret, override_data=self.kwargs)

        self.path = conn.data.path

        try:
            ftp = ftplib.FTP(host=conn.data.host, timeout=600)
        except ftplib.all_errors as e:
            raise Exception('Could not find the FTP host:' + str(e))

        try:
            ftp.login(user=secret.data.username, passwd=secret.data.password)
        except ftplib.all_errors as e:
            raise Exception('Incorrect FTP credentials:' + str(e))

        try:
            ftp.cwd(self.path)
        except ftplib.all_errors as e:
            raise Exception('Missing FTP directory:' + str(e))

        return ftp

    def close(self):
        raise NotImplementedError


class BaseSftpProvider(BaseProvider):

    path = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(secret_schema=SftpSecret, connection_schema=FtpConnection)
        self.conn_id = conn_id
        self.kwargs = kwargs

    @cached_property
    def conn(self):
        conn = self._get_connection(conn_id=self.conn_id, override_data=self.kwargs)
        secret = self._get_secret(secret_id=conn.secret, override_data=self.kwargs)

        self.path = conn.data.path

        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(
                hostname=conn.data.host,
                port=conn.data.port if conn.data.port else paramiko.config.SSH_PORT,
                username=secret.data.username,
                password=secret.data.password,
                pkey=secret.data.ssh.get_key_object() if secret.data.ssh else None
            )
            sftp = ssh_client.open_sftp()
        except Exception as e:
            raise Exception('Could not connect to SFTP host:' + str(e))

        try:
            sftp.chdir(self.path)
        except ftplib.all_errors as e:
            raise Exception('Missing SFTP directory:' + str(e))

        return sftp

    def close(self):
        self.get_conn().close()
