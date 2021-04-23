import os
import csv
import logging
try:
    import pyodbc
except:
    logging.warning('Python package pyodbc is not installed.')
import pandas as pd
from pandas.io.sql import DatabaseError

from ontelligence.core.schemas.data import Table, Column
from ontelligence.providers.microsoft.base import BaseSQLServerProvider


class SQLServerConnection(BaseSQLServerProvider):

    chunk_size = 10000

    def __init__(self, conn_id, **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)

    def query(self, query, return_chunks=False):
        try:
            chunks = pd.read_sql(sql=query, con=self.get_conn(), chunksize=self.chunk_size)
            return chunks if return_chunks else pd.concat(chunks, ignore_index=True)
        except DatabaseError as err:
            raise DatabaseError('Could not execute query: {}'.format(str(err)))

    def list_of_tables(self):
        cursor = self.get_conn().cursor()
        tables = [x.table_name for x in cursor.tables()]
        cursor.close()
        return tables

    def table_exists(self, table):
        cursor = self.get_conn().cursor()
        table = table.replace('\'', '\'\'').replace('[', '', 1).replace(']', '', 1)
        cursor.execute(f"SELECT COUNT(*) FROM sys.tables WHERE [Name] = '{table}'")
        if cursor.fetchone()[0] == 1:
            cursor.close()
            return True
        cursor.close()
        return False

    def export_query(self, query, output_file, delimiter=','):
        # DEPRECATED ON 7/20/2018 BY HAMZA AHMAD
        # cursor = self.get_conn().cursor()
        # cursor.execute(query)
        # with open(output_file, 'w', newline='', encoding='utf-8') as f:
        #     writer = csv.writer(f, delimiter=delimiter)
        #     writer.writerow([x[0] for x in cursor.description])  # Write the column headers
        #     data = cursor.fetchmany(self.chunk_size)
        #     while data:
        #         for row in data:
        #             writer.writerow(row)
        #         data = cursor.fetchmany(self.chunk_size)
        # cursor.close()
        chunks = self.query(query=query, return_chunks=True)
        try:
            with open(output_file, 'w', newline='') as f:
                next(chunks).to_csv(f, index=False, sep=delimiter, quoting=csv.QUOTE_ALL)  # First fetch contains headers
                for chunk in chunks:
                    chunk.to_csv(f, index=False, header=False, sep=delimiter, quoting=csv.QUOTE_ALL)
        except Exception as e:
            print(type(chunks))
            print(chunks)
            print('#############################################')
            raise e
        return

    def export_table(self, table, output_file, delimiter=','):
        query = 'select * from {}'.format(table)
        self.export_query(query=query, output_file=output_file, delimiter=delimiter)
        return
