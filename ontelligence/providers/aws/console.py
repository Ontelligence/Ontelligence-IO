import json
import requests
from urllib.parse import quote_plus
from typing import Any, List, Dict, Optional, Tuple, Union

from botocore.exceptions import ClientError

from ontelligence.providers.aws.base import BaseAwsProvider


AWS_SIGNIN_URL = 'https://signin.aws.amazon.com/federation'
AWS_CONSOLE_URL = 'https://console.aws.amazon.com/'


class ConsoleProvider(BaseAwsProvider):

    def __init__(self, conn_id: Optional[str] = None, **kwargs):
        # NOTE: `conn_id = None` falls back to using the host's AWS config/credentials.
        kwargs['client_type'] = 'none'
        kwargs['resource_type'] = 'none'
        super().__init__(conn_id=conn_id, **kwargs)

    def get_console_url(self, destination: Optional[str] = None):
        ISSUER = 'test'

        _credentials = self.get_frozen_credentials()
        credentials_json = quote_plus(json.dumps({
            'sessionId': _credentials.access_key,
            'sessionKey': _credentials.secret_key,
            'sessionToken': _credentials.token
        }))

        request_url = f'{AWS_SIGNIN_URL}?Action=getSigninToken&SessionDuration=3600&Session={credentials_json}'
        r = requests.get(request_url)
        sign_in_token = json.loads(r.text)['SigninToken']

        # Generate Signed URL
        destination = destination or quote_plus(AWS_CONSOLE_URL)
        signed_url = f'{AWS_SIGNIN_URL}?Action=login&Issuer={ISSUER}&Destination={destination}&SigninToken={sign_in_token}'
        return signed_url
