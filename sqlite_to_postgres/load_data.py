import sqlite3

import config
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from dataimporter.models import (
    FilmWork,
    Genre,
    GenreFilmWork,
    Person,
    PersonFilmWork,
)
from dataimporter.postgres_saver import PostgresSaver
from dataimporter.sqlite_extractor import SQLiteExtractor, sqlite_conn_context


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    tables = (Genre, Person, FilmWork, PersonFilmWork, GenreFilmWork)

    def load_and_save(table):
        with postgres_saver.prepare_insert_context(table):
            for batch_rows in sqlite_extractor.extract(table):
                postgres_saver.save(batch_rows, table)

    for table in tables:
        load_and_save(table)


if __name__ == '__main__':
    with sqlite_conn_context(
        config.SQLITE_DB, read_only=True
    ) as sqlite_conn, psycopg2.connect(
        **config.DATABASE, cursor_factory=DictCursor
    ) as pg_conn:
        try:
            load_from_sqlite(sqlite_conn, pg_conn)
        except psycopg2.Error as err:
            print('Ошибка PostgreSQL во время импортирования данных.')
            print(err.pgerror)
        except sqlite3.Error as err:
            print('Ошибка SQLite во время выгрузки данных.')
            print(err)
        except Exception as err:
            print('Ошибка работы скрипта.')
            print(err)
