import os
from typing import Any, List, Optional

import boto3
import smart_open
from pydantic.dataclasses import dataclass

from ontelligence.providers.snowflake import Snowflake
from ontelligence.providers.aws.s3 import S3
from ontelligence.core.schemas.base import BaseDataClass
from ontelligence.core.schemas.data import Table


@dataclass
class S3ToSnowflakeParams(BaseDataClass):
    # Input.
    s3_path: str
    table: Table

    # File config.
    file_profile: Any

    # Load config.
    dependency_on_file: bool

    # Merge config.
    extract_script: Optional[str]
    truncate_table: bool
    replace_table: bool
    overlap_columns: Optional[List[str]]


def s3_to_snowflake(sf: Snowflake, s3: S3, params: S3ToSnowflakeParams, **kwargs):

    table_exists = sf.table_exists(
        database=params.table.database,
        schema=params.table.db_schema,
        table=params.table.name
    )

    # Define staging table.
    staging_table = Table(
        database=params.table.database,
        db_schema=params.table.db_schema,
        name=f'STG_{params.table.name}'
    )

########################################################################################################################
# Load S3 object into staging table.
########################################################################################################################

    if table_exists and not params.replace_table and not params.dependency_on_file:
        # Create staging table like final table.
        sf.create_table_like(
            table=staging_table.name,
            schema=staging_table.db_schema,
            parent_table=params.table.name,
            parent_schema=params.table.db_schema,
            replace_if_exists=True
        )

        # Copy data into staging table.
        # Create temporary file format and stage.
        _file_format = f'tmp_{params.table.database}_{params.table.db_schema}_{params.table.name}'
        _stage = _file_format
        _s3_path_for_stage = os.path.split(params.s3_path)[0]
        _s3_file_name = os.path.split(params.s3_path)[1]
        sf.create_file_format(file_format=_file_format, file_format_type='CSV', replace_if_exists=True, skip_header=True)
        sf.create_stage(stage_name=_stage, storage_integration='INSCAPE_STORAGE_INTEGRATION', s3_path=_s3_path_for_stage, file_format=_file_format)
        # Copy data into staging table.
        if 'data_schema' not in kwargs:
            raise NotImplementedError('Cannot infer a file directly from S3 yet. Pass in "data_schema": List[Column]')
        columns = kwargs['data_schema']

        sf.copy_into_from_stage_expanded(table_name=staging_table.name, stage_name=f'{_stage}/{_s3_file_name}', file_format=_file_format, pattern='*', columns=columns)
    else:
        # Analyze file profile and schema.
        if 'data_schema' not in kwargs:
            raise NotImplementedError('Cannot infer a file directly from S3 yet. Pass in "data_schema": List[Column]')
        columns = kwargs['data_schema']

        # Create staging table using analyzed schema.
        sf.create_table(table=staging_table.name, columns=columns, replace_if_exists=True)

        # Copy data into staging table.
        # Create temporary file format and stage.
        _file_format = f'tmp_{params.table.database}_{params.table.db_schema}_{params.table.name}'
        _stage = _file_format
        _s3_path_for_stage = os.path.split(params.s3_path)[0]
        _s3_file_name = os.path.split(params.s3_path)[1]
        sf.create_file_format(file_format=_file_format, file_format_type='CSV', replace_if_exists=True, skip_header=True)
        sf.create_stage(stage_name=_stage, storage_integration='INSCAPE_STORAGE_INTEGRATION', s3_path=_s3_path_for_stage, file_format=_file_format)
        # Copy data into staging table.
        sf.copy_into_from_stage_expanded(table_name=staging_table.name, stage_name=f'{_stage}/{_s3_file_name}', file_format=_file_format, pattern='*', columns=columns)

########################################################################################################################
# Run intermediate transformations.
########################################################################################################################

    if params.extract_script:
        # TODO: Run extract script.
        raise NotImplementedError

########################################################################################################################
# Run high-level QA to verify if staging table can be inserted into final table.
########################################################################################################################

    staging_table.columns = sf.get_columns(
        database=staging_table.database,
        schema=staging_table.db_schema,
        table=staging_table.name
    )

    params.table.columns = sf.get_columns(
        database=params.table.database,
        schema=params.table.db_schema,
        table=params.table.name
    )

    if table_exists and not params.replace_table and (params.dependency_on_file or params.extract_script):
        # Column count QA.
        if len(staging_table.columns) != len(params.table.columns):
            # print('STAGING COLUMNS:', staging_table.columns)
            # print('FINAL COLUMNS:', params.table.columns)
            raise Exception('The columns in the staging table do not match the target table.')

        # Column names and ordinal position QA.

        # Column dtypes compatibility QA.

        # Check for duplicates.

    else:
        dtypes_are_same = True

########################################################################################################################
# Prepare final table for new data.
########################################################################################################################

    if params.overlap_columns and table_exists and not params.replace_table and not params.truncate_table:
        # Delete overlapping data between staging table and final table.
        if all(isinstance(x, str) for x in params.overlap_columns):
            _match_keys = params.overlap_columns
        else:
            _match_keys = [x.name for x in params.overlap_columns]
        sf.delete_overlapping_data(
            table=params.table.name,
            schema=params.table.db_schema,
            match_keys=_match_keys,
            match_table=staging_table.name,
            match_schema=staging_table.db_schema,
            delete_overlapping=True
        )

    if table_exists and not params.replace_table:
        if params.truncate_table:
            # Truncate final table.
            sf.truncate_table(database=params.table.database, schema=params.table.db_schema, table=params.table.name)

        # Insert staging table into final table.
        sf.insert_into(
            table=params.table.name,
            schema=params.table.db_schema,
            from_table=staging_table.name,
            from_schema=staging_table.db_schema,
            from_columns=[x.name for x in staging_table.columns],
            columns=[x.name for x in params.table.columns],
            ignore_identity_cols=None
        )

        #  Drop staging table.
        sf.drop_table(
            database=staging_table.database,
            schema=staging_table.db_schema,
            table=staging_table.name
        )
    else:
        # Rename staging table to final table.
        sf.rename_table(
            table=staging_table.name,
            rename_to=params.table.name,
            schema=params.table.db_schema,
            drop_if_exists=params.replace_table
        )


# def s3_to_s3():
#
#     s3_next = S3()
#     s3_spire = S3()
#     s3_next.get_session()
#
#     # AWS Credentials
#     session = boto3.Session(
#         aws_access_key_id='<YOUR S3 KEY>',
#         aws_secret_access_key='<YOUR S3 SECRET>',
#     )
#
#     def read_in_chunks(file_object, chunk_size=1024):
#         """Lazy function (generator) to read a file piece by piece.
#         Default chunk size: 1k."""
#         while True:
#             data = file_object.read(chunk_size)
#             if not data:
#                 break
#             yield data
#
#     CHUNK_SIZE = 256 * 1024 * 1024  # 256MB
#     PART_SIZE = 256 * 1024 * 1024  # 256MB
#
#     source_s3_url = 's3://path/to/s3/file.gz'
#     destination_gcp_url = 'gs://path/to/gcs/file.gz'
#
#     chunk_index = 0
#
#     with open(destination_gcp_url, 'wb', transport_params={'min_part_size': PART_SIZE}) as gcp_sink:
#         with open(source_s3_url, 'rb', transport_params={'session': session}, ignore_ext=True) as s3_source:
#             for piece in read_in_chunks(s3_source, CHUNK_SIZE):
#                 print('Read: ' + size(chunk_index * CHUNK_SIZE) + " (" + str(chunk_index) + ")")
#                 gcp_sink.write(piece)
#
#                 chunk_index = chunk_index + 1
