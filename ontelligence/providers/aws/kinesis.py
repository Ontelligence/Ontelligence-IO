from typing import Any, List, Dict, Optional, Tuple, Union

from botocore.exceptions import ClientError

from ontelligence.providers.aws.base import BaseAwsProvider


class KinesisProvider(BaseAwsProvider):

    def __init__(self, conn_id: Optional[str] = None, **kwargs):
        # NOTE: `conn_id = None` falls back to using the host's AWS config/credentials.
        kwargs['client_type'] = 'kinesis'
        kwargs['resource_type'] = 'kinesis'
        super().__init__(conn_id=conn_id, **kwargs)
