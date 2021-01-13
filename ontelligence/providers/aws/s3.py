import os
import fnmatch
import gzip as gz
import io
import re
import shutil
from functools import wraps
from inspect import signature
from io import BytesIO
from typing import Any, Callable, List, Dict, Optional, Tuple, TypeVar, Union, cast
from urllib.parse import urlparse

from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.exceptions import ClientError

from ontelligence.providers.aws.base import BaseAwsProvider
from ontelligence.core.schemas.aws import S3Bucket, S3Key
from ontelligence.utils.file import chunks


T = TypeVar('T', bound=Callable)


def provide_bucket(func: T) -> T:
    """
    Function decorator that provides bucket used during instantiation if no bucket has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)
        if 'bucket' not in bound_args.arguments:
            self = args[0]
            bound_args.arguments['bucket'] = self.bucket
        return func(*bound_args.args, **bound_args.kwargs)
    return cast(T, wrapper)


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
    def _get_bucket_cors(self, bucket: Optional[str] = None):
        cors = self.get_resource().BucketCors(bucket)
        return cors

    @provide_bucket
    def _get_bucket_lifecycle(self, bucket: Optional[str] = None):
        lifecycle = self.get_resource().BucketLifecycle(bucket)
        return lifecycle  # return lifecycle.rules

    @provide_bucket
    def _get_bucket_policy(self, bucket: Optional[str] = None):
        policy = self.get_resource().BucketPolicy(bucket)
        return policy  # return policy.policy

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
    def read_key(self, key: str, bucket: Optional[str] = None) -> str:
        """Reads a key from S3"""
        obj = self.get_key(key, bucket)
        return obj.get()['Body'].read().decode('utf-8')

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
    def copy_key(self, key: str, bucket: Optional[str] = None, destination_bucket: Optional[str] = None, destination_prefix: Optional[str] = None) -> str:
        """Copy a file from one S3 location to another"""
        if not self.key_exists(key, bucket):
            raise Exception(f'The source file in Bucket {bucket} with path {key} does not exist')
        copy_source = {'Bucket': bucket, 'Key': key}
        destination_bucket = destination_bucket or bucket
        destination_prefix = destination_prefix or self.prefix
        destination_key = destination_prefix + os.path.split(key)[1]

        print('copy_source:', copy_source)
        print('destination_bucket:', destination_bucket)
        print('destination_prefix:', destination_key)
        # self.get_conn().copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)

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
