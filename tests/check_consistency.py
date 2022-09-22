import os
import sqlite3
import sys
from string import Template

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from sqlite_to_postgres import config
from sqlite_to_postgres.dataimporter.postgres_saver import pg_conn_context
from sqlite_to_postgres.dataimporter.sqlite_extractor import (
    SQLiteExtractor,
    sqlite_conn_context,
)


def test_integrity(sqlite_curs, pg_curs):
    """Check number of returned rows between each pair of tables."""
    tables = (
        "genre",
        "person",
        "film_work",
        "person_film_work",
        "genre_film_work",
    )

    query = Template("SELECT COUNT(*) AS count FROM $table")
    for table in tables:
        sqlite_curs.execute(query.substitute(table=table))
        pg_curs.execute(query.substitute(table=table))
        sqlite_count = sqlite_curs.fetchone()[0]
        pg_count = pg_curs.fetchone()[0]
        assert sqlite_count == pg_count, (
            f"Кол-во строк в SQLite {sqlite_count} не равно "
            f"кол-ву в PostgreSQL {pg_count} для таблицы {table}"
        )


def test_tables_values(sqlite_curs, pg_curs):
    """Check table rows values from SQLite are equal ones in PostgreSQL."""
    tables = (
        "genre",
        "person",
        "film_work",
        "person_film_work",
        "genre_film_work",
    )

    queries = {
        "genre": ("SELECT id, name, description FROM genre;"),
        "person": "SELECT id, full_name FROM person",
        "film_work": (
            "SELECT id, title, description, creation_date, file_path, "
            "rating, type FROM film_work;"
        ),
        "person_film_work": (
            "SELECT id, film_work_id, person_id, role FROM person_film_work"
        ),
        "genre_film_work": (
            "SELECT id, film_work_id, genre_id FROM genre_film_work"
        ),
    }

    for table in tables:
        sqlite_curs.execute(queries[table])
        pg_curs.execute(queries[table])
        sqlite_rows = map(SQLiteExtractor.transform, sqlite_curs.fetchall())
        pg_rows = map(SQLiteExtractor.transform, pg_curs.fetchall())
        for sqlite_row, pg_row in zip(sqlite_rows, pg_rows):
            for sqlite_item, pg_item in zip(
                sqlite_row.items(), pg_row.items()
            ):
                assert sqlite_item[1] == pg_item[1], (
                    f"В таблице {table} значение колонки {sqlite_item[0]} "
                    f"в SQLite {sqlite_item[1]} не равно значению "
                    f"в PostgreSQL {pg_item[1]}"
                )


def check_consistency(connection: sqlite3.Connection, pg_conn: _connection):
    sqlite_curs = connection.cursor()
    pg_curs = pg_conn.cursor()

    test_integrity(sqlite_curs, pg_curs)
    test_tables_values(sqlite_curs, pg_curs)

    sqlite_curs.close()
    pg_curs.close()


if __name__ == "__main__":
    with sqlite_conn_context(
        config.SQLITE_DB, read_only=True
    ) as sqlite_conn, pg_conn_context(
        config.DATABASE, cursor_factory=DictCursor
    ) as pg_conn:
        check_consistency(sqlite_conn, pg_conn)
