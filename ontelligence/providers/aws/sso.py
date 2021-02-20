from typing import Any, List, Dict, Optional, Tuple, Union

from botocore.exceptions import ClientError

from ontelligence.providers.aws.base import BaseAwsProvider


class SSOProvider(BaseAwsProvider):

    account_id = None
    access_token = None

    def __init__(self, conn_id: Optional[str] = None, account_id: str = None, access_token: str = None, **kwargs):
        # NOTE: `conn_id = None` falls back to using the host's AWS config/credentials.
        kwargs['client_type'] = 'sso'
        kwargs['resource_type'] = 'sso'
        if not account_id or not access_token:
            Exception('Missing required parameters.')
        self.account_id = account_id
        self.access_token = access_token
        super().__init__(conn_id=conn_id, **kwargs)

    def list_accounts(self):
        # TODO: Add pagination.
        res = self.get_conn().list_accounts(accessToken=self.access_token)
        return res['accountList']

    def list_account_roles(self):
        # TODO: Add pagination.
        res = self.get_conn().list_account_roles(accountId=self.account_id, accessToken=self.access_token)
        return res['roleList']

    def get_role_credentials(self, role_name: str):
        res = self.get_conn().get_role_credentials(
            roleName=role_name,
            accountId=self.account_id,
            accessToken=self.access_token
        )
        return res['roleCredentials']

    def logout(self):
        self.get_conn().logout(accessToken=self.access_token)
