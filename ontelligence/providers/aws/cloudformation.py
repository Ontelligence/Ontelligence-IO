from typing import Any, List, Dict, Optional, Tuple, Union

from botocore.exceptions import ClientError

from ontelligence.providers.aws.base import BaseAwsProvider


class CloudFormation(BaseAwsProvider):

    def __init__(self, conn_id: Optional[str] = None, **kwargs):
        # NOTE: `conn_id = None` falls back to using the host's AWS config/credentials.
        kwargs['client_type'] = 'cloudformation'
        kwargs['resource_type'] = 'cloudformation'
        super().__init__(conn_id=conn_id, **kwargs)

########################################################################################################################
# Stacks.
########################################################################################################################

    def create_stack(self, name: str, template_url: str, parameters: List[Dict] = None, capabilities: Optional[str] = None) -> str:
        if capabilities and capabilities not in ['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM', 'CAPABILITY_AUTO_EXPAND']:
            raise Exception('Stack "capabilities" is not valid')
        params = {
            'StackName': name,
            'TemplateURL': template_url,
            'Parameters': parameters,
            'Capabilities': capabilities
        }
        res = self.get_conn().create_stack(**params)
        return res['StackId']

    def delete_stack(self, name: str) -> None:
        self.get_conn().delete_stack(StackName=name)
