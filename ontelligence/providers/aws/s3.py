import os
import fnmatch
import gzip as gz
import io
import re
import json
import shutil
from io import BytesIO
from typing import Any, List, Dict, Optional, Tuple, Union
from urllib.parse import urlparse

from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.exceptions import ClientError

from ontelligence.providers.aws.base import BaseAwsProvider
from ontelligence.core.schemas.aws import S3Bucket, S3Key
from ontelligence.utils.decorators.function_factory import provide_if_missing
from ontelligence.utils.file import chunks


provide_bucket = provide_if_missing('bucket')


class S3(BaseAwsProvider):

    def __init__(self, conn_id: Optional[str] = None, bucket: Optional[str] = None, **kwargs):
        # NOTE: `conn_id = None` falls back to using the host's AWS config/credentials.
        kwargs['client_type'] = 's3'
        kwargs['resource_type'] = 's3'

        if bucket:
            kwargs['bucket'] = bucket

        super().__init__(conn_id=conn_id, **kwargs)

        # TODO: Populate self.bucket and self.prefix from connection when not provided to S3 class.
        self.bucket = bucket
        self.prefix = kwargs['prefix'] if 'prefix' in kwargs else ''

    @staticmethod
    def parse_s3_url(url: str) -> Tuple[str, str]:
        parsed_url = urlparse(url)
        if not parsed_url.netloc:
            raise Exception(f'Please provide a bucket instead of "{url}"')
        bucket = parsed_url.netloc
        key = parsed_url.path.strip('/')
        return bucket, key

########################################################################################################################
# Bucket.
########################################################################################################################

    @provide_bucket
    def bucket_exists(self, bucket: Optional[str] = None) -> bool:
        try:
            self.get_conn().head_bucket(Bucket=bucket)
            return True
        except ClientError:
            return False

    def list_buckets(self) -> List[S3Bucket]:
        response = self.get_conn().list_buckets()
        return [S3Bucket(name=x['Name']) for x in response['Buckets']]

    def create_bucket(self):
        raise NotImplementedError

    def delete_bucket(self, bucket: str, force_delete: bool = False) -> None:
        """To delete s3 bucket, delete all s3 bucket objects and then delete the bucket"""
        # if force_delete:
        #     bucket_keys = self.list_keys(bucket=bucket)
        #     if bucket_keys:
        #         # TODO: Delete multiple keys.
        #         pass
        # self.get_conn().delete_bucket(Bucket=bucket)
        raise NotImplementedError

########################################################################################################################
# Bucket metadata.
#   ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
########################################################################################################################

    @provide_bucket
    def _get_bucket_acl(self, bucket: Optional[str] = None):
        acl = self.get_resource().BucketAcl(bucket)
        return acl  # return acl.grants

    @provide_bucket
    def get_bucket_cors(self, bucket: Optional[str] = None):
        cors = self.get_resource().BucketCors(bucket)
        return cors

    @provide_bucket
    def get_bucket_lifecycle(self, bucket: Optional[str] = None):
        lifecycle = self.get_resource().BucketLifecycle(bucket)
        try:
            return lifecycle.rules
        except ClientError:
            pass
        return None

    @provide_bucket
    def get_bucket_policy(self, bucket: Optional[str] = None):
        policy = self.get_resource().BucketPolicy(bucket)
        return json.loads(policy.policy)

    @provide_bucket
    def get_bucket_encryption(self, bucket: Optional[str] = None):
        try:
            res = self.get_conn().get_bucket_encryption(Bucket=bucket)
            rules = res['ServerSideEncryptionConfiguration']['Rules']
            if rules:
                if 'ApplyServerSideEncryptionByDefault' in rules[0]:
                    encryption = rules[0]['ApplyServerSideEncryptionByDefault']
                    return encryption
        except ClientError:
            pass
        return None

########################################################################################################################
# Prefix.
########################################################################################################################

    @provide_bucket
    def prefix_exists(self, prefix: str, delimiter: str, bucket: Optional[str] = None) -> bool:
        """Checks that a prefix exists in a bucket"""
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(r'(\w+[{d}])$'.format(d=delimiter), prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(bucket, previous_level, delimiter)
        return prefix in plist

    @provide_bucket
    def list_prefixes(self, bucket: Optional[str] = None, prefix: Optional[str] = None, delimiter: Optional[str] = None, page_size: Optional[int] = None, max_items: Optional[int] = None) -> list:
        """Lists prefixes in a bucket under prefix"""
        prefix = prefix or self.prefix
        delimiter = delimiter or '/'
        config = {'PageSize': page_size, 'MaxItems': max_items}

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config)

        prefixes = []
        for page in response:
            if 'CommonPrefixes' in page:
                for common_prefix in page['CommonPrefixes']:
                    prefixes.append(common_prefix['Prefix'])

        return prefixes

    def delete_prefix(self):
        raise NotImplementedError

########################################################################################################################
# Key.
########################################################################################################################

    @provide_bucket
    def key_exists(self, key: str, bucket: Optional[str] = None) -> bool:
        try:
            self.get_conn().head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            else:
                raise e

    @provide_bucket
    def get_key(self, key: str, bucket: Optional[str] = None) -> S3Transfer:
        """Returns a boto3.s3.Object"""
        obj = self.get_resource().Object(bucket, key)
        obj.load()
        return obj

    @provide_bucket
    def list_keys(self, bucket: Optional[str] = None, prefix: Optional[str] = None, delimiter: Optional[str] = None,
                  page_size: Optional[int] = None, max_items: Optional[int] = None) -> list:
        """Lists keys in a bucket under prefix and not containing delimiter"""
        prefix = prefix or self.prefix
        delimiter = delimiter or ''
        config = {'PageSize': page_size, 'MaxItems': max_items}

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config)

        keys = []
        for page in response:
            if 'Contents' in page:
                for k in [x for x in page['Contents'] if x['Key'] != prefix]:
                    keys.append(k['Key'])

        return keys

    @provide_bucket
    def delete_keys(self, keys: Union[str, List[str]], bucket: Optional[str] = None):
        if isinstance(keys, str):
            keys = [keys]
        for each_chunk in chunks(keys, chunk_size=1000):  # boto3 max keys per request = 100
            _keys_to_be_deleted = {"Objects": [{"Key": k} for k in each_chunk]}
            response = self.get_conn().delete_objects(Bucket=bucket, Delete=_keys_to_be_deleted)
            deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
            print("Deleted:", deleted_keys)
            if "Errors" in response:
                errors_keys = [x['Key'] for x in response.get("Errors", [])]
                raise Exception(f"Errors when deleting: {errors_keys}")
        # return self.get_conn().delete_object(Bucket=bucket, Key=key)

########################################################################################################################
# Read Key.
########################################################################################################################

    @provide_bucket
    def download_file(self, key: str, bucket: Optional[str] = None, local_path: Optional[str] = None) -> str:
        """Downloads a file from the S3 location to the local file system"""
        # self.log.info('Downloading source S3 file from Bucket %s with path %s', bucket_name, key)
        if not self.key_exists(key, bucket):
            raise Exception(f'The source file in Bucket {bucket} with path {key} does not exist')

        _output_folder = local_path if local_path else ''
        local_path = os.path.join(_output_folder, os.path.split(key)[1])
        self.get_conn().download_file(bucket, key, local_path)
        return local_path

    @provide_bucket
    def open(self, key: str, bucket: Optional[str] = None) -> str:
        """Reads a key from S3"""
        obj = self.get_key(key, bucket)
        return obj.get()['Body']

    @provide_bucket
    def read_key(self, key: str, bucket: Optional[str] = None) -> str:
        """Reads a key from S3"""
        return self.open(key=key, bucket=bucket).read().decode('utf-8')

    @provide_bucket
    def select_key(self, key: str, bucket: Optional[str] = None, expression: Optional[str] = None, expression_type: Optional[str] = None,
                   input_serialization: Optional[Dict[str, Any]] = None, output_serialization: Optional[Dict[str, Any]] = None) -> str:
        """
        Reads a key with S3 Select
        ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.select_object_content
        """
        expression = expression or 'SELECT * FROM S3Object'
        expression_type = expression_type or 'SQL'
        if input_serialization is None:
            input_serialization = {'CSV': {'FileHeaderInfo': 'Use'}}  # {'CSV': {}}
        if output_serialization is None:
            output_serialization = {'CSV': {}}
        response = self.get_conn().select_object_content(
            Bucket=bucket,
            Key=key,
            Expression=expression,
            ExpressionType=expression_type,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization,
        )
        return ''.join(event['Records']['Payload'].decode('utf-8') for event in response['Payload'] if 'Records' in event)

########################################################################################################################
# Write Key.
########################################################################################################################

    @provide_bucket
    def upload_file(self, filename: str, key: Optional[str] = None, bucket: Optional[str] = None, replace: bool = False,
                    encrypt: bool = False, acl_policy: Optional[str] = None) -> None:
        """Loads a local file to S3"""
        key = key or self.prefix + os.path.split(filename)[1]
        if not replace and self.key_exists(key, bucket):
            raise ValueError("The key {key} already exists.".format(key=key))
        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"
        if acl_policy:
            extra_args['ACL'] = acl_policy

        # TODO: Is TransferConfig needed? upload_file() is managed by the S3TransferManager and therefore automatically handles multipart uploads.
        # config = TransferConfig(multipart_threshold=1024 * 25, multipart_chunksize=1024 * 25, max_concurrency=10, use_threads=True)

        # TODO: upload_file() allows you to track the upload using a callback function.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#the-callback-parameter
        self.get_conn().upload_file(filename, bucket, key, ExtraArgs=extra_args)

    @provide_bucket
    def upload_string(self, string_data: str, key: str, bucket: Optional[str] = None, replace: bool = False,
                      encrypt: bool = False, encoding: Optional[str] = None, acl_policy: Optional[str] = None, ) -> None:
        """Loads a string to S3"""
        encoding = encoding or 'utf-8'
        bytes_data = string_data.encode(encoding)
        file_obj = io.BytesIO(bytes_data)
        self._upload_file_obj(file_obj, key, bucket, replace, encrypt, acl_policy)
        file_obj.close()

    @provide_bucket
    def upload_bytes(self, bytes_data: bytes, key: str, bucket: Optional[str] = None, replace: bool = False,
                     encrypt: bool = False, acl_policy: Optional[str] = None) -> None:
        """Loads bytes to S3"""
        file_obj = io.BytesIO(bytes_data)
        self._upload_file_obj(file_obj, key, bucket, replace, encrypt, acl_policy)
        file_obj.close()

    @provide_bucket
    def upload_file_obj(self, file_obj: BytesIO, key: str, bucket: Optional[str] = None, replace: bool = False,
                        encrypt: bool = False, acl_policy: Optional[str] = None) -> None:
        """Loads a file object to S3"""
        self._upload_file_obj(file_obj, key, bucket, replace, encrypt, acl_policy)

    @provide_bucket
    def _upload_file_obj(self, file_obj: BytesIO, key: str, bucket: Optional[str] = None, replace: bool = False,
                         encrypt: bool = False, acl_policy: Optional[str] = None) -> None:
        if not replace and self.key_exists(key, bucket):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"
        if acl_policy:
            extra_args['ACL'] = acl_policy

        self.get_conn().upload_fileobj(file_obj, bucket, key, ExtraArgs=extra_args)

    @provide_bucket
    def copy_key(self, key: str, bucket: Optional[str] = None, destination_bucket: Optional[str] = None, destination_prefix: Optional[str] = None) -> S3Key:
        """Copy a file from one S3 location to another"""
        if not self.key_exists(key, bucket):
            raise Exception(f'The source file in Bucket {bucket} with path {key} does not exist')
        copy_source = {'Bucket': bucket, 'Key': key}
        destination_bucket = destination_bucket or bucket
        destination_prefix = destination_prefix or self.prefix
        destination_key = destination_prefix + os.path.split(key)[1]

        # print('copy_source:', copy_source)
        # print('destination_bucket:', destination_bucket)
        # print('destination_prefix:', destination_key)
        self.get_conn().copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        return S3Key(bucket=destination_bucket, prefix=os.path.split(destination_key)[0] + '/', name=os.path.split(destination_key)[1])

    def move_key(self):
        raise NotImplementedError

    def generate_presigned_url(self, client_method: str, params: Optional[dict] = None, expires_in: int = 3600, http_method: Optional[str] = None) -> Optional[str]:
        """Generate a pre-signed URL given a client, its method, and arguments"""
        try:
            return self.get_conn().generate_presigned_url(ClientMethod=client_method, Params=params, ExpiresIn=expires_in, HttpMethod=http_method)
        except ClientError as e:
            # self.log.error(e.response["Error"]["Message"])
            return None

    def create_manifest(self):
        raise NotImplementedError

########################################################################################################################
# Key metadata.
########################################################################################################################

    def update_object_acl(self):
        raise NotImplementedError

    def get_object_tags(self):
        raise NotImplementedError

    def update_object_tags(self):
        raise NotImplementedError

########################################################################################################################
# Event notifications.
########################################################################################################################

    @provide_bucket
    def get_lambda_configurations(self, bucket: Optional[str] = None) -> List[Optional[Dict[str, Any]]]:
        """Get Amazon Lambda event notifications"""
        notifications = self.get_resource().BucketNotification(bucket)
        configurations = notifications.lambda_function_configurations
        return configurations or []

    @provide_bucket
    def get_queue_configurations(self, bucket: Optional[str] = None) -> List[Optional[Dict[str, Any]]]:
        """Get Amazon SQS event notifications"""
        notifications = self.get_resource().BucketNotification(bucket)
        configurations = notifications.queue_configurations
        return configurations or []

    @provide_bucket
    def get_topic_configurations(self, bucket: Optional[str] = None) -> List[Optional[Dict[str, Any]]]:
        """Get Amazon SNS event notifications"""
        notifications = self.get_resource().BucketNotification(bucket)
        configurations = notifications.topic_configurations
        return configurations or []

    @provide_bucket
    def create_lambda_configuration(self, lambda_arn: str, on_events: Union[str, List[str]], event_name: str = None,
                                    bucket: Optional[str] = None, prefix: str = None, suffix: str = None, ):
        """Create Amazon Lambda event notification"""
        _valid_events = [
            's3:ReducedRedundancyLostObject',
            's3:ObjectCreated:*',
            's3:ObjectCreated:Put',
            's3:ObjectCreated:Post',
            's3:ObjectCreated:Copy',
            's3:ObjectCreated:CompleteMultipartUpload',
            's3:ObjectRemoved:*',
            's3:ObjectRemoved:Delete',
            's3:ObjectRemoved:DeleteMarkerCreated',
            's3:ObjectRestore:*',
            's3:ObjectRestore:Post',
            's3:ObjectRestore:Completed',
            's3:Replication:*',
            's3:Replication:OperationFailedReplication',
            's3:Replication:OperationNotTracked',
            's3:Replication:OperationMissedThreshold',
            's3:Replication:OperationReplicatedAfterThreshold',
        ]
        if isinstance(on_events, str):
            on_events = [on_events]

        if not on_events or not all(x in _valid_events for x in on_events):
            raise Exception('Invalid S3 events.')

        filter_rules = []
        if prefix or suffix:
            if prefix:
                filter_rules.append({'Name': 'prefix', 'Value': prefix})
            if suffix:
                filter_rules.append({'Name': 'suffix', 'Value': suffix})

        config = {
            'LambdaFunctionConfigurations': [
                {
                    # 'Id': event_name,  # If not provided, an ID will be assigned.
                    'LambdaFunctionArn': lambda_arn,
                    'Events': on_events,
                    'Filter': {'Key': {'FilterRules': filter_rules}}
                }
            ]
        }

        self.get_conn().put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=config)
