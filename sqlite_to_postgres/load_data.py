import logging
import sqlite3
import sys

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
from dataimporter.postgres_saver import PostgresSaver, pg_conn_context
from dataimporter.sqlite_extractor import SQLiteExtractor, sqlite_conn_context


def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s, [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


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
    logger = setup_logger()

    with sqlite_conn_context(
        config.SQLITE_DB, read_only=True
    ) as sqlite_conn, pg_conn_context(
        config.DATABASE, cursor_factory=DictCursor
    ) as pg_conn:
        try:
            load_from_sqlite(sqlite_conn, pg_conn)
        except psycopg2.Error:
            logger.exception(
                'Ошибка PostgreSQL во время импортирования данных.'
            )
        except sqlite3.Error:
            logger.exception('Ошибка SQLite во время выгрузки данных.')
        except Exception:
            logger.exception('Ошибка работы скрипта.')
