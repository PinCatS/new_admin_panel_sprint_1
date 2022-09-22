from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Dict

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch

from .models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


@contextmanager
def pg_conn_context(dsn: Dict, cursor_factory):
    conn = psycopg2.connect(**dsn, cursor_factory=DictCursor)
    with conn:
        yield conn
    conn.close()


class PostgresSaver:
    @dataclass(frozen=True)
    class Queries:
        prepare: str
        insert: str

    QUERIES = {
        Genre.__name__: Queries(
            prepare=(
                'PREPARE table_insert '
                '(timestamp with time zone, timestamp with time zone, '
                'uuid, text, text) AS '
                'INSERT INTO content.genre VALUES($1, $2, $3, $4, $5) '
                'ON CONFLICT (name) DO NOTHING'
            ),
            insert=(
                (
                    'EXECUTE table_insert '
                    '(%(created)s, %(modified)s, %(id)s, %(name)s, %(description)s)'
                )
            ),
        ),
        Person.__name__: Queries(
            prepare=(
                'PREPARE table_insert '
                '(timestamp with time zone, timestamp with time zone, '
                'uuid, text) AS '
                'INSERT INTO content.person VALUES($1, $2, $3, $4) '
                'ON CONFLICT (full_name) DO NOTHING'
            ),
            insert=(
                'EXECUTE table_insert '
                '(%(created)s, %(modified)s, %(id)s, %(full_name)s)'
            ),
        ),
        FilmWork.__name__: Queries(
            prepare=(
                'PREPARE table_insert '
                '(timestamp with time zone, timestamp with time zone, '
                'uuid, text, text, date, double precision, text, text) AS '
                'INSERT INTO content.film_work '
                'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) '
                'ON CONFLICT (id) DO NOTHING'
            ),
            insert=(
                'EXECUTE table_insert '
                '(%(created)s, %(modified)s, %(id)s, %(title)s, %(description)s, '
                '%(creation_date)s, %(rating)s, %(type)s, %(file_path)s)'
            ),
        ),
        PersonFilmWork.__name__: Queries(
            prepare=(
                'PREPARE table_insert '
                '(uuid, text, timestamp with time zone, uuid, uuid) AS '
                'INSERT INTO content.person_film_work '
                'VALUES($1, $2, $3, $4, $5) '
                'ON CONFLICT (film_work_id, person_id, role) DO NOTHING'
            ),
            insert=(
                'EXECUTE table_insert '
                '(%(id)s, %(role)s, %(created)s, %(film_work_id)s, %(person_id)s)'
            ),
        ),
        GenreFilmWork.__name__: Queries(
            prepare=(
                'PREPARE table_insert '
                '(uuid, timestamp with time zone, uuid, uuid) AS '
                'INSERT INTO content.genre_film_work VALUES($1, $2, $3, $4) '
                'ON CONFLICT (film_work_id, genre_id) DO NOTHING'
            ),
            insert=(
                'EXECUTE table_insert '
                '(%(id)s, %(created)s, %(film_work_id)s, %(genre_id)s)'
            ),
        ),
    }

    @contextmanager
    def prepare_insert_context(self, model):
        self.prepare(model)
        yield
        self.deallocate_prepare()

    def __init__(self, conn: _connection):
        self.conn = conn
        self.curs = conn.cursor()

    def prepare(self, model):
        self.curs.execute(PostgresSaver.QUERIES[model.__name__].prepare)

    def save(self, data, model, batch_size=100):
        args = [asdict(row) for row in data]
        execute_batch(
            self.curs,
            PostgresSaver.QUERIES[model.__name__].insert,
            args,
            page_size=batch_size,
        )

    def deallocate_prepare(self):
        self.curs.execute('DEALLOCATE table_insert')
