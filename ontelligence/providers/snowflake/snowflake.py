import os
import logging
from functools import wraps
from inspect import signature
from typing import Optional, List, Dict, TypeVar, Callable, cast

import pandas as pd

from ontelligence.core.schemas.data import Table, Column
from ontelligence.providers.snowflake.base import BaseSnowflakeProvider
from ontelligence.utils.file import get_clean_headers


T = TypeVar('T', bound=Callable)


def provide_database_and_schema(func: T) -> T:
    """
    Function decorator that provides database and schema used during instantiation if not passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)
        if 'database' not in bound_args.arguments:
            self = args[0]
            bound_args.arguments['database'] = self.database
        if 'schema' not in bound_args.arguments:
            self = args[0]
            bound_args.arguments['schema'] = self.schema
        return func(*bound_args.args, **bound_args.kwargs)
    return cast(T, wrapper)


class Snowflake(BaseSnowflakeProvider):

    chunk_size = 1024 ** 2 * 25

    role = None
    warehouse = None
    database = None
    schema = None

    def __init__(self, conn_id, **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)
        self.__cursor = self.get_conn().cursor()
        self.use_role(role=self.role)

    def close(self):
        self.__cursor.close()
        self.get_conn().commit()
        self.get_conn().close()

########################################################################################################################
# Query execution.
########################################################################################################################

    def execute(self, query, commit=True, return_cursor=False):
        self.__cursor.execute(query)
        row_count = self.__cursor.rowcount
        if row_count != -1:
            self.log.info(f'Number of rows affected by query: {row_count}')
        if commit:
            self.get_conn().commit()
        if return_cursor:
            return self.__cursor

    def query(self, query: str, chunk_size: Optional[int] = None, return_chunks: bool = False) -> pd.DataFrame:
        chunk_size = chunk_size if chunk_size else self.chunk_size
        cur = self.execute(query=query, commit=False, return_cursor=True)
        columns = [desc[0] for desc in cur.description]
        del cur
        try:
            chunks = pd.read_sql(sql=query, con=self.get_conn(), chunksize=chunk_size)
            if return_chunks:
                return chunks
            else:
                try:
                    df = pd.concat(chunks, ignore_index=True)
                    return df
                except:
                    return pd.DataFrame(columns=columns)
        except Exception as e:
            raise Exception('Could not execute query:' + str(e))

########################################################################################################################
# Session.
########################################################################################################################

    def use_role(self, role: str) -> None:
        self.execute(query=f'USE ROLE {role};')
        self.role = role

    def use_database(self, database: str) -> None:
        self.execute(query=f'USE DATABASE {database};')
        self.database = database

    def use_schema(self, schema: str) -> None:
        self.execute(query=f'USE SCHEMA {schema};')
        self.schema = schema

########################################################################################################################
# Fetch.
########################################################################################################################

    def get_all_databases(self) -> List[str]:
        return self.query('SHOW DATABASES;')['name'].to_list()

    def get_all_schemas(self) -> List[str]:
        return self.query('SHOW SCHEMAS;')['name'].to_list()

    def get_all_tables(self) -> List[str]:
        return self.query('SHOW TABLES;')['name'].to_list()

    def get_all_views(self) -> List[str]:
        return self.query('SHOW VIEWS;')['name'].to_list()

    @provide_database_and_schema
    def table_exists(self, table: str, database: Optional[str] = None, schema: Optional[str] = None) -> bool:
        query = f'''SELECT 1 FROM {database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_CATALOG = '{database}'
                      AND TABLE_SCHEMA = '{schema}'
                      AND TABLE_NAME = '{table}';'''
        cursor = self.execute(query=query, return_cursor=True)
        return True if cursor.rowcount > 0 else False

    @provide_database_and_schema
    def view_exists(self, view: str, database: Optional[str] = None, schema: Optional[str] = None) -> bool:
        query = f'''SELECT 1 FROM {database}.INFORMATION_SCHEMA.VIEWS
                    WHERE TABLE_CATALOG = '{database}'
                      AND TABLE_SCHEMA = '{schema}'
                      AND TABLE_NAME = '{view}';'''
        cursor = self.execute(query=query, return_cursor=True)
        return True if cursor.rowcount > 0 else False

    @provide_database_and_schema
    def get_columns(self, table: str, database: Optional[str] = None, schema: Optional[str] = None) -> List[Column]:
        # TODO: Also get data type size.
        query = f'''SELECT COLUMN_NAME AS "name",
                           DATA_TYPE AS "dtype"
                    FROM {database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = '{database}'
                      AND TABLE_SCHEMA = '{schema}'
                      AND TABLE_NAME = '{table}'
                    ORDER BY ORDINAL_POSITION;'''
        return [Column.from_dict(data=x) for x in self.query(query).to_dict(orient='records')]

    def get_columns_of_query(self, query: str) -> List[str]:
        cur = self.execute(query=query, commit=False, return_cursor=True)
        return [desc[0] for desc in cur.description]

########################################################################################################################
# Tables.
########################################################################################################################

    @provide_database_and_schema
    def create_table(self, table: str, columns: List[Column], database: Optional[str] = None, schema: Optional[str] = None, replace_if_exists: bool = False, table_type: str = None, create_if_not_exists: bool = False):
        or_replace = ' OR REPLACE' if replace_if_exists else ''
        table_type = f' {table_type}' if table_type else ''
        if_not_exists = ' IF NOT EXISTS' if create_if_not_exists else ''
        columns_and_data_types = ',\n    '.join([f'"{x.name}" {x.dtype}' for x in columns])
        query = f'''CREATE{or_replace}{table_type} TABLE{if_not_exists} {database}.{schema}.{table}\n   ({columns_and_data_types});'''
        self.log_sql(query)
        self.execute(query)

    def create_external_table(self):
        raise NotImplementedError

    def create_table_like(self, table: str, parent_table: str, schema: str, parent_schema: str, replace_if_exists: Optional[bool] = False):
        or_replace = ' OR REPLACE' if replace_if_exists else ''
        query = f'CREATE{or_replace} TABLE {self.database}.{schema}.{table} LIKE {self.database}.{parent_schema}.{parent_table};'
        self.log_sql(query)
        self.execute(query)

    @provide_database_and_schema
    def create_table_as(self, table: str, query: str, database: Optional[str] = None, schema: Optional[str] = None):
        query = f'CREATE OR REPLACE TABLE {database}.{schema}.{table}\nAS\n({query})'
        self.log_sql(query)
        self.execute(query)

    @provide_database_and_schema
    def drop_table(self, table: str, database: Optional[str] = None, schema: Optional[str] = None):
        query = f'DROP TABLE IF EXISTS {database}.{schema}.{table};'
        self.log_sql(query)
        self.execute(query)

    @provide_database_and_schema
    def truncate_table(self, table: str, database: Optional[str] = None, schema: Optional[str] = None):
        query = f'TRUNCATE TABLE {database}.{schema}.{table};'
        self.log_sql(query)
        self.execute(query)

########################################################################################################################
# Views.
########################################################################################################################

    @provide_database_and_schema
    def create_view(self, view: str, query: str, database: Optional[str] = None, schema: Optional[str] = None):
        query = f'''CREATE VIEW {database}.{schema}.{view} IF NOT EXISTS AS ({query.replace(";", "")});'''
        self.log_sql(query)
        self.execute(query)

    @provide_database_and_schema
    def drop_view(self, view: str, database: Optional[str] = None, schema: Optional[str] = None):
        query = f'DROP VIEW IF EXISTS {database}.{schema}.{view};'
        self.log_sql(query)
        self.execute(query)

    def get_view_ddl(self):
        raise NotImplementedError

########################################################################################################################
# File formats.
########################################################################################################################

    @provide_database_and_schema
    def get_file_formats(self, like: Optional[str] = None, database: Optional[str] = None, schema: Optional[str] = None):
        like = f" LIKE '{like}'" if like else ''
        database = f" DATABASE {database}" if database else ''
        schema = f" SCHEMA {schema}" if schema else ''
        in_account = ' IN' if database or schema else ''
        return self.query(f"SHOW FILE FORMATS{like}{in_account}{database}{schema};")

    def create_file_format(self, file_format: str, file_format_type: str, replace_if_exists: Optional[bool] = False,
                           create_if_not_exists: Optional[bool] = False, **kwargs):
        or_replace = ' OR REPLACE' if replace_if_exists else ''
        if_not_exists = ' IF NOT EXISTS' if create_if_not_exists else ''
        skip_header = f'\nSKIP_HEADER = 1' if 'skip_header' in kwargs and kwargs.get('skip_header') else ''
        query = f'''CREATE{or_replace} FILE FORMAT{if_not_exists} {file_format}
                    TYPE = {file_format_type}
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF = ('NULL', 'null', 'N/A'){skip_header};'''
        self.log_sql(query)
        self.execute(query)

    def drop_file_format(self, file_format: str) -> None:
        self.execute(f'DROP FILE FORMAT IF EXISTS {file_format};')

########################################################################################################################
# Stages.
########################################################################################################################

    def create_stage(self, stage_name: str,
                     # replace_if_exists: Optional[bool] = False, temporary: Optional[bool] = False, create_if_not_exists: Optional[bool] = False, file_format: Optional[str] = None, **kwargs
                     storage_integration: str, s3_path: str, file_format: str):
        query = f'''CREATE OR REPLACE STAGE {stage_name}
                    STORAGE_INTEGRATION = {storage_integration}
                    URL = '{s3_path}'
                    FILE_FORMAT = {file_format};'''
        self.log_sql(query)
        self.execute(query)

    def drop_stage(self, stage: str) -> None:
        self.execute(f'DROP STAGE IF EXISTS {stage};')

########################################################################################################################
# Loading data into Snowflake
#   ref: https://docs.snowflake.com/en/user-guide-data-load.html
########################################################################################################################

########################################################################################################################
# Copy into.
########################################################################################################################

    def copy_into_from_stage(self, table_name: str, stage_name: str, file_format: str, pattern: str):
        query = f'''COPY INTO {self.database}.{self.schema}.{table_name}
                    FROM @{stage_name}
                    FILE_FORMAT = {file_format}
                    PATTERN = '{pattern}'
                    ON_ERROR = 'ABORT_STATEMENT'
                    PURGE = FALSE;'''
        self.log_sql(query)
        # self.execute(query=query)

    def copy_into_from_stage_expanded(self, table_name: str, stage_name: str, file_format: str, pattern: str, columns: List[Column]):
        columns_as = ',\n        '.join(
            ['${} AS "{}"'.format(i + 1, x.name) for i, x in enumerate(columns)])
        query = f'''COPY INTO {self.database}.{self.schema}.{table_name}
                    FROM (SELECT {columns_as}
                          FROM @{stage_name})
                    FILE_FORMAT = {file_format}
                    -- PATTERN = '{pattern}'
                    ON_ERROR = 'ABORT_STATEMENT'
                    PURGE = FALSE;'''

        self.log_sql(query)
        print('\n' + query)
        self.execute(query=query)

########################################################################################################################
# Snow pipe.
#   ref: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro.html
########################################################################################################################

    def get_all_pipes(self, like: Optional[str] = None, database: Optional[str] = None, schema: Optional[str] = None):
        like = f" LIKE '{like}'" if like else ''
        database = f" DATABASE {database}" if database else ''
        schema = f" SCHEMA {schema}" if schema else ''
        in_account = ' IN' if database or schema else ''
        return self.query(f"SHOW PIPES{like}{in_account}{database}{schema};")

    def create_pipe(self, pipe: str, copy_statement: str, replace_if_exists: bool = False, create_if_not_exists: bool = False, auto_ingest: bool = False, integration: Optional[str] = None):
        or_replace = ' OR REPLACE' if replace_if_exists else ''
        if_not_exists = ' IF NOT EXISTS' if create_if_not_exists else ''
        auto_ingest = f'\nAUTO_INGEST = {auto_ingest}' if auto_ingest else ''
        integration = f"\nINTEGRATION = '{integration}'" if integration else ''
        query = f'''CREATE{or_replace} PIPE{if_not_exists} {pipe}{auto_ingest}{integration}\nAS\n{copy_statement};'''
        self.execute(query)

    def drop_pipe(self, pipe: str) -> None:
        self.execute(f'DROP PIPE IF EXISTS {pipe};')

    def run_pipe(self):
        raise NotImplementedError

########################################################################################################################
# Insert into.
########################################################################################################################

    # TODO: Refactor this again.
    def insert_into(self, table, schema, query=None, from_table=None, from_schema=None, columns=None, from_columns=None,
                    override_dtypes=None, ignore_identity_cols=None, debug=False):

        from_schema = from_schema if from_schema else schema
        override_dtypes = override_dtypes if override_dtypes else dict()
        if not self.table_exists(database=self.database, table=table, schema=schema):
            raise Exception('Cannot insert into non-existent table {}.{}.'.format(schema, table))

        insert_cols = columns if columns else self.get_columns(database=self.database, table=table, schema=schema)
        if ignore_identity_cols:
            insert_cols = [x for x in insert_cols if x not in ignore_identity_cols]
        insert_cols = get_clean_headers(headers=insert_cols, clean_headers=False, for_query=True)
        insert_query = '''INSERT INTO {}.{}.{}\n   ({})'''.format(self.database, schema, table, ',\n    '.join(insert_cols))

        if query:
            from_columns = self.get_columns_of_query(query=query)
            insert_query = '''{}\n{};'''.format(insert_query, query.rstrip(' ').rstrip(';'))
        elif from_table:
            from_columns = from_columns if from_columns else self.get_columns(database=self.database, table=from_table, schema=from_schema)
            if ignore_identity_cols:
                from_columns = [x for x in from_columns if x not in ignore_identity_cols]
            from_columns = get_clean_headers(headers=from_columns, clean_headers=False, for_query=True)
            from_columns = ['{0}::{1} {0}'.format(x, override_dtypes[x]) if x in override_dtypes else x for x in from_columns]
            insert_query = '''{}\nSELECT {}\nFROM {}.{}.{};'''.format(insert_query, ',\n       '.join(from_columns), self.database, from_schema, from_table)
        else:
            raise Exception('Specify either a query or from_table.')

        self.log_sql(insert_query)

        len_i = len(insert_cols)
        len_f = len(from_columns)
        if len_i != len_f:
            raise Exception('\n{} has {} columns than {}.{}.' .format('Insert query' if query else '{}.{}'.format(from_schema, from_table), '{} {}'.format(abs(len_i - len_f), 'less' if len_i > len_f else 'more'), schema, table))
        self.execute(query=insert_query)
        return

########################################################################################################################
# Alter table.
########################################################################################################################

    def rename_table(self, table: str, rename_to: str, schema: str, drop_if_exists=False):

        if drop_if_exists:
            self.drop_table(database=self.database, table=rename_to, schema=schema)
        else:
            if self.table_exists(database=self.database, schema=schema, table=rename_to):
                raise Exception('Cannot rename {0}.{1} to {0}.{2} because the table already exists.'.format(schema, table, rename_to))
        query = 'ALTER TABLE {}.{}.{} RENAME TO {}.{}.{};'.format(self.database, schema, table, self.database, schema, rename_to)
        self.log_sql(query)
        self.execute(query=query)

########################################################################################################################
# Exports data.
########################################################################################################################

    def export_query(self):
        raise NotImplementedError

########################################################################################################################
# Stored procedures.
########################################################################################################################

    def run_stored_procedure(self, procedure, **kwargs):
        # TODO: Pass in additional input parameters.
        query = f'CALL {procedure}();'
        self.log_sql(query)
        self.execute(query)

########################################################################################################################
# Query plan.
########################################################################################################################

    def explain(self, statement: str, using: Optional[str] = None):
        using = f' USING {using}' if using else ''
        return self.query(f"EXPLAIN{using} {statement.rstrip(';')};")

########################################################################################################################
# Misc.
########################################################################################################################

    def delete_overlapping_data(self, table, schema, match_table, match_schema, match_keys, debug=False, delete_overlapping=True):
        # use delete_overlapping=False to remove duplicate data for large datasets
        all_match_keys = []
        if delete_overlapping:
            for key in match_keys:
                if key.lower().startswith('trunc('):
                    key = key.lower().strip().replace('trunc(', '').replace(')', '')
                    # join_pattern = 'TRUNC({0}.{1}.{2}."{3}") = TRUNC({4}.{5}.{6}."{7}")'
                    join_pattern = 'DATE_TRUNC("day", {0}.{1}.{2}."{3}") = DATE_TRUNC("day", {4}.{5}.{6}."{7}")'
                elif key.lower().startswith('nvl('):
                    key = key.lower().strip().replace('nvl(', '').replace(')', '')
                    join_pattern = 'NVL({0}.{1}.{2}."{3}") = NVL({4}.{5}.{6}."{7}")'
                else:
                    join_pattern = '{0}.{1}.{2}."{3}" = {4}.{5}.{6}."{7}"'
                all_match_keys.extend([join_pattern.format(self.database, schema, table, str(key).strip('"').strip("'"),
                                                           self.database, match_schema, match_table, str(key).strip('"').strip("'"))])
            join_statement = '\n                AND '.join(all_match_keys)
            query = f'''DELETE FROM {self.database}.{schema}.{table}
                        WHERE EXISTS (SELECT 1 FROM {self.database}.{match_schema}.{match_table}
                                      WHERE {join_statement});'''
            self.log_sql(query)
            self.execute(query)
        # else:
        #     for key in match_keys:
        #         join_pattern = f'{key} IN (SELECT DISTINCT {key} FROM {match_schema}.{match_table})'
        #         all_match_keys.extend([join_pattern])
        #     join_statement = '\n                AND '.join(all_match_keys)
        #     temp_table = table + '_overlap_temp'
        #
        #     retry = True
        #     while retry:
        #         retry = False
        #         # self.remove_schema_binding(table=f'{schema}.{table}')
        #         self.create_table_like(table=temp_table, parent_table=table, schema=schema, parent_schema=schema, drop_if_exists=True)
        #         insert_query = f'''INSERT INTO {schema}.{temp_table}
        #         (
        #             SELECT * FROM {schema}.{table} WHERE NOT
        #             (
        #                 {join_statement}
        #             )
        #         );'''
        #         print('\n' + insert_query + '\n')
        #         self.execute(query=insert_query)
        #         renamed_prod = table + '_overlap_old'
        #         if self.table_exists(schema=schema, table=renamed_prod):
        #             # if table_old exists, it means that a previous run of this function failed
        #             # code below restores "_old" table
        #             self.rename_table(table=renamed_prod, rename_to=table, drop_if_exists=True)
        #             print(f'''Found table {schema}.{renamed_prod} which means there was a fatal error with a previous execution of this function.
        #             Table has been renamed back to {table}. Attempting to re-run...''')
        #             retry = True
        #
        #     self.rename_table(table=table, rename_to=renamed_prod, schema=schema)
        #     self.rename_table(table=temp_table, rename_to=table, schema=schema)
        #     self.drop_table(table=renamed_prod)
