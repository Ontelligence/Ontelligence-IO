import os
from typing import Union, Optional, BinaryIO

from jira import JIRA as _JIRA
from requests_toolbelt import MultipartEncoder
from cached_property import cached_property

from ontelligence.core.schemas.jira import JiraConnection, JiraSecret
from ontelligence.providers.base import BaseProvider


class JIRA(_JIRA):
    # The following method overrides the built-in method.
    def add_attachment(self, issue: str, attachment: Union[BinaryIO, str], filename: Optional[str] = None):
        if isinstance(attachment, str):
            attachment = open(attachment, "rb")
        if hasattr(attachment, 'read') and hasattr(attachment, 'mode') and attachment.mode != 'rb':
            raise Exception(f"{attachment.name} was not opened in 'rb' mode, attaching file may fail.")
        url = self._get_url('issue/' + str(issue) + '/attachments')
        fname = filename
        if not fname:
            fname = os.path.basename(attachment.name)

        def file_stream():
            return MultipartEncoder(fields={'file': (fname, attachment, 'application/octet-stream')})

        m = file_stream()
        headers = {'Content-Type': m.content_type, 'X-Atlassian-Token': 'no-check'}
        r = self._session.post(url, data=m, headers=headers, retry_data=file_stream)


class BaseJiraProvider(BaseProvider):

    project = None
    epic_custom_field = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(secret_schema=JiraSecret, connection_schema=JiraConnection)
        self.conn_id = conn_id
        self.kwargs = kwargs

    @cached_property
    def conn(self) -> JIRA:
        conn = self._get_connection(conn_id=self.conn_id, override_data=self.kwargs)
        secret = self._get_secret(secret_id=conn.secret, override_data=self.kwargs)

        if conn.data.project:
            self.project = conn.data.project

        if conn.data.epic and conn.data.epic.custom_field:
            self.epic_custom_field = conn.data.epic.custom_field

        try:
            jira = JIRA(server=conn.data.server, basic_auth=(secret.data.email, secret.data.api_token))
        except Exception as e:
            raise Exception('Could not connect to Jira:' + str(e))

        return jira

    def get_conn(self) -> JIRA:
        return self.conn

    def close(self):
        raise NotImplementedError

