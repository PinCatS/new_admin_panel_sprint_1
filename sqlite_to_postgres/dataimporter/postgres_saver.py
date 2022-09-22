import re
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Dict

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import execute_batch


@contextmanager
def pg_conn_context(dsn: Dict, cursor_factory):
    conn = psycopg2.connect(**dsn, cursor_factory=cursor_factory)
    with conn:
        yield conn
    conn.close()


class PostgresSaver:
    _camel_2_snake_case = re.compile(r'(?<!^)(?=[A-Z])')
    _schema_info = None

    def __init__(self, conn: _connection):
        self.conn = conn
        self.curs = conn.cursor()

    @contextmanager
    def prepare_insert_context(self, model):
        """Ensure that PREPARE statement is executed before insertion.
        It also deallocates prepare at completion.
        """
        self.prepare(model)
        yield
        self.deallocate_prepare()

    def get_column_name_and_type(self):
        """Get column names and their types for all content schemas.

        Returns dict where keys are table names and values are
        dicts with 'column_names' and 'column_types' keys that
        contain lists of column names and types, accordingly.
        """
        if self._schema_info:  # already requested before
            return self._schema_info

        result = defaultdict(lambda: defaultdict(list))
        self.curs.execute(
            (
                "SELECT table_name, column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_schema IN ('content') "
                "ORDER BY ordinal_position;"
            )
        )
        for row in self.curs.fetchall():
            table, column, type = row
            result[table]['column_names'].append(column)
            result[table]['column_types'].append(type)

        return result

    def build_prepare_query(self, table: str) -> str:
        """Build SQL PREPARE statement to optimize inserts."""
        self._schema_info = self.get_column_name_and_type()
        types = ', '.join(self._schema_info[table]['column_types'])
        values_count = len(self._schema_info[table]['column_types'])
        values_placeholders = ', '.join(
            ['$' + str(i + 1) for i in range(values_count)]
        )
        return (
            'PREPARE table_insert '
            f'({types}) AS INSERT INTO content.{table} '
            f'VALUES({values_placeholders}) '
            'ON CONFLICT (id) DO NOTHING'
        )

    def build_insert_query(self, table: str) -> str:
        """Build SQL INSERT statement."""
        self._schema_info = self.get_column_name_and_type()
        values_placeholders = ', '.join(
            [
                '%(' + name + ')s'
                for name in self._schema_info[table]['column_names']
            ]
        )
        return f'EXECUTE table_insert ({values_placeholders})'

    def prepare(self, model):
        """Execute PREPARE statement for the model."""
        table_name = self.model_2_table_name(model)
        query = self.build_prepare_query(table_name)
        self.curs.execute(query)

    def save(self, data, model, batch_size=100):
        """Insert a batch of rows to the table."""
        table_name = self.model_2_table_name(model)
        query = self.build_insert_query(table_name)
        args = [asdict(row) for row in data]
        execute_batch(self.curs, query, args, page_size=batch_size)

    def deallocate_prepare(self):
        self.curs.execute('DEALLOCATE table_insert')

    def model_2_table_name(self, model):
        return self._camel_2_snake_case.sub('_', model.__name__).lower()
