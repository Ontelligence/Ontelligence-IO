from typing import Any, List, Dict, Optional, Tuple, Union
import time
import datetime
import webbrowser
from dateutil.tz import tzutc

from botocore.exceptions import ClientError

from ontelligence.providers.aws.base import BaseAwsProvider


CLIENT_SECRET_ALGORITHM = 'HS384'


class SSOOpenIDConnectProvider(BaseAwsProvider):

    _DEFAULT_INTERVAL = 5
    _SLOW_DOWN_DELAY = 5
    _GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:device_code'

    def __init__(self, conn_id: Optional[str] = None, start_url: str = None, sso_region: str = None, **kwargs):
        # NOTE: `conn_id = None` falls back to using the host's AWS config/credentials.
        kwargs['client_type'] = 'sso-oidc'
        kwargs['resource_type'] = 'sso-oidc'
        if not start_url or not sso_region:
            raise Exception('Missing input parameters.')
        self.start_url = start_url
        self.sso_region = sso_region
        super().__init__(conn_id=conn_id, **kwargs)

    def register_client(self, client_name: str = None):
        client_name = client_name or f'botocore-client-id-{self.sso_region}'
        res = self.get_conn().register_client(clientName=client_name, clientType='public')  # scopes=[]
        client = {
            'client_id': res['clientId'],
            'client_id_issued_at': res['clientIdIssuedAt'],
            'client_secret': res['clientSecret'],
            'client_secret_expires_at': res['clientSecretExpiresAt']
        }
        return client

    def start_device_authorization(self, client):
        res = self.get_conn().start_device_authorization(
            clientId=client['client_id'],
            clientSecret=client['client_secret'],
            startUrl=self.start_url
        )
        authorization = {
            'device_code': res['deviceCode'],
            'user_code': res['userCode'],
            'verification_uri': res['verificationUri'],
            'verification_uri_complete': res['verificationUriComplete'],
            'expires_in': res['expiresIn'],
        }
        if 'interval' in res:
            authorization['interval'] = res['interval']
        return authorization

    def create_token(self, client, authorization, open_webbrowser=True):
        if open_webbrowser:
            try:
                webbrowser.open_new_tab(authorization['verification_uri_complete'])
            except Exception as e:
                print('Failed to open browser:', str(e))

        interval = authorization.get('interval', self._DEFAULT_INTERVAL)
        # NOTE: This loop currently relies on the service to either return
        # a valid token or a ExpiredTokenException to break the loop. If this
        # proves to be problematic it may be worth adding an additional
        # mechanism to control timing this loop out.
        while True:
            try:
                res = self.get_conn().create_token(
                    grantType=self._GRANT_TYPE,
                    clientId=client['client_id'],
                    clientSecret=client['client_secret'],
                    deviceCode=authorization['device_code'],
                    # redirectUri=''
                )
                token = {
                    'access_token': res['accessToken'],
                    'token_type': res['tokenType'],
                    'start_url': self.start_url,
                    # 'refresh_token': res.get('refreshToken'),
                    # 'id_token': res.get('idToken'),
                    'region': self.sso_region,
                    'expires_in': res['expiresIn']
                }
                return token
            except self.get_conn().exceptions.SlowDownException:
                interval += self._SLOW_DOWN_DELAY
            except self.get_conn().exceptions.AuthorizationPendingException:
                pass
            except self.get_conn().exceptions.ExpiredTokenException:
                raise Exception('The pending authorization to retrieve an SSO token has expired.')
            time.sleep(interval)
