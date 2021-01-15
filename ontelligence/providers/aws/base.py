from typing import Optional, Dict, Any

import boto3
from cached_property import cached_property

from ontelligence.providers.base import BaseProvider, LoggingMixin
from ontelligence.core.schemas.aws import AwsSecret, AwsS3Connection


class _AwsSessionFactory(LoggingMixin):

    def __init__(self, secret: AwsSecret, region_name: str):
        self.secret = secret
        self.region_name = region_name

########################################################################################################################
# Session.
########################################################################################################################

    def create_session(self):
        session = self._create_basic_session()
        role_arn = self._get_role_arn()
        if not role_arn:
            return session
        return self._create_impersonated_session(role_arn=role_arn, session=session)

    def _create_basic_session(self):
        access_key, secret_access_key = self._get_credentials()
        return boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

    def _create_impersonated_session(self, role_arn: str, session: boto3.session.Session):
        sts_client = session.client('sts')
        if self.secret.assume_role_config.method == 'assume_role':
            sts_response = self._assume_role(sts_client=sts_client, role_arn=role_arn)
        # TODO: If assume_role_method == 'assume_role_with_saml': ...
        # TODO: If assume_role_method == 'assume_role_with_web_identity': ...

        access_key = sts_response['Credentials']['AccessKeyId']
        secret_access_key = sts_response['Credentials']['SecretAccessKey']
        session_token = sts_response['Credentials']['SessionToken']

        return boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,
                                     region_name=self.region_name, aws_session_token=session_token)

########################################################################################################################
# Additional helper functions.
########################################################################################################################

    def _get_credentials(self):
        access_key = None
        secret_access_key = None
        if self.secret:
            access_key = self.secret.access_key
            secret_access_key = self.secret.secret_access_key
        # TODO: Retrieve credentials from aws config file.
        return access_key, secret_access_key

    def _get_role_arn(self):
        if self.secret.assume_role_config:
            account_id = self.secret.assume_role_config.account_id
            iam_role = self.secret.assume_role_config.role_name
            role_arn = self.secret.assume_role_config.role_arn
            if not role_arn and account_id and iam_role:
                role_arn = f'arn:aws:iam::{account_id}:role/{iam_role}'
            return role_arn
        return None

    def _assume_role(self, sts_client: boto3.client, role_arn: str):
        self.log.info(f'Assuming role: {role_arn}')
        role_session_name = 'S3Provider_GeneratedSession'
        external_id = self.secret.assume_role_config.external_id
        if external_id:
            return sts_client.assume_role(RoleArn=role_arn, RoleSessionName=role_session_name, DurationSeconds=3600, ExternalId=external_id)
        return sts_client.assume_role(RoleArn=role_arn, RoleSessionName=role_session_name, DurationSeconds=3600)


class BaseAwsProvider(BaseProvider):

    __assume_role_configs = []

    def __init__(self, conn_id: str, client_type: Optional[str] = None, resource_type: Optional[str] = None, **kwargs):

        if not (client_type or resource_type):
            raise ValueError('Either "client_type" or "resource_type" must be specified')

        connection_schema = kwargs.get('connection_schema', AwsS3Connection)
        super().__init__(secret_schema=AwsSecret, connection_schema=connection_schema)
        self.conn_id = conn_id
        self.client_type = client_type
        self.resource_type = resource_type
        self.region_name = kwargs.get('region_name')
        self.kwargs = kwargs

    def _get_credentials(self):
        if self.conn_id:
            try:
                conn = self._get_connection(conn_id=self.conn_id, override_data=self.kwargs)
                secret = self._get_secret(secret_id=conn.secret, override_data=self.kwargs)
                endpoint_url = None
                session = _AwsSessionFactory(secret=secret.data, region_name=self.region_name).create_session()
                return session, endpoint_url

            except Exception:
                raise Exception('Failed to create boto3 session. Fallback to boto3 credential strategy')

        session = boto3.session.Session(region_name=self.region_name, profile_name=self.kwargs.get('profile', None))

        if 'assume_role_config' in self.kwargs:
            _secret = AwsSecret.from_dict({'assume_role_config': self.kwargs['assume_role_config']})
            _session_factory = _AwsSessionFactory(secret=_secret, region_name=self.region_name)
            _role_arn = _session_factory._get_role_arn()
            session = _session_factory._create_impersonated_session(role_arn=_role_arn, session=session)

        for each_config in self.__assume_role_configs:
            _secret = AwsSecret.from_dict({'assume_role_config': each_config})
            _session_factory = _AwsSessionFactory(secret=_secret, region_name=self.region_name)
            _role_arn = _session_factory._get_role_arn()
            session = _session_factory._create_impersonated_session(role_arn=_role_arn, session=session)

        return session, None

    def get_client(self, client_type: Optional[str] = None):
        """Get the underlying boto3 client using boto3 session"""
        session, endpoint_url = self._get_credentials()
        client_type = client_type if client_type else self.client_type
        return session.client(client_type)

    def get_resource(self, resource_type: Optional[str] = None):
        """Get the underlying boto3 resource using boto3 session"""
        session, endpoint_url = self._get_credentials()
        resource_type = resource_type if resource_type else self.resource_type
        return session.resource(resource_type)

    @cached_property
    def conn(self):
        """Get the underlying boto3 client/resource (cached)"""
        if self.client_type:
            return self.get_client()
        elif self.resource_type:
            return self.get_resource()

    def get_conn(self):
        return self.conn

########################################################################################################################
# Additional helper functions.
########################################################################################################################

    def get_session(self):
        """Get the underlying boto3.session object"""
        session, _ = self._get_credentials()
        return session

    def get_frozen_credentials(self):
        session = self.get_session()
        return session.get_credentials().get_frozen_credentials()

########################################################################################################################
# Refactor the below function. Should it be here?
########################################################################################################################

    def assume_role(self, assume_role_config: Dict[str, Any]):
        self.__assume_role_configs.extend(assume_role_config)
