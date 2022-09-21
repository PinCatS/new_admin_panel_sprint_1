import pprint
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from email.policy import default
from string import Template
from typing import List

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch


@contextmanager
def sqlite_conn_context(db_path: str, read_only: bool = False):
    db_path_template = (
        Template('file:$db_path?mode=ro')
        if read_only
        else Template('file:$db_path')
    )

    conn = sqlite3.connect(
        db_path_template.substitute(db_path=db_path), uri=True
    )
    conn.row_factory = sqlite3.Row
    with conn:
        yield conn
    conn.close()


@dataclass(frozen=True)
class Genre:
    name: str
    description: str = field(default='')
    id: uuid.UUID = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True)
class Person:
    full_name: str
    id: uuid.UUID = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True)
class FilmWork:
    title: str
    file_path: str
    type: str
    creation_date: str
    description: str = field(default='')
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True)
class GenreFilmWork:
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    id: uuid.UUID = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True)
class PersonFilmWork:
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    id: uuid.UUID = field(default_factory=lambda: str(uuid.uuid4()))


class SQLiteExtractor:
    queries = {
        Genre.__name__: (
            'SELECT id, name, '
            'CASE WHEN description IS NULL THEN "" '
            'ELSE description END description FROM genre;'
        ),
        Person.__name__: 'SELECT id, full_name FROM person',
        FilmWork.__name__: (
            'SELECT id, title, CASE WHEN description IS NULL THEN "" '
            'ELSE description END description, '
            'creation_date, file_path, '
            'CASE WHEN rating IS NULL THEN 0.0 ELSE rating END rating, '
            'type FROM film_work;'
        ),
        PersonFilmWork.__name__: (
            'SELECT id, film_work_id, person_id, role FROM person_film_work'
        ),
        GenreFilmWork.__name__: (
            'SELECT id, film_work_id, genre_id FROM genre_film_work'
        ),
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def extract(self, factory, batch_size=100):
        curs = self.conn.cursor()
        curs.execute(SQLiteExtractor.queries[factory.__name__])
        data = curs.fetchmany(size=batch_size)
        objs = map(lambda row: factory(**row), data)
        while data:
            if data:
                yield objs
            data = curs.fetchmany(size=batch_size)
            objs = map(lambda row: factory(**row), data)
        curs.close()
        return

    def extract_movies(self):
        return {
            'genres': list(self.extract_genres),
        }


class PostgresSaver:
    prepare_queries = {
        Genre.__name__: (
            'PREPARE table_insert '
            '(timestamp with time zone, timestamp with time zone, '
            'uuid, text, text) AS '
            'INSERT INTO content.genre VALUES($1, $2, $3, $4, $5) '
            'ON CONFLICT (name) DO NOTHING'
        ),
        Person.__name__: (
            'PREPARE table_insert '
            '(timestamp with time zone, timestamp with time zone, '
            'uuid, text) AS '
            'INSERT INTO content.person VALUES($1, $2, $3, $4) '
            'ON CONFLICT (full_name) DO NOTHING'
        ),
        FilmWork.__name__: (
            'PREPARE table_insert '
            '(timestamp with time zone, timestamp with time zone, '
            'uuid, text, text, date, double precision, text, text) AS '
            'INSERT INTO content.film_work '
            'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) '
            'ON CONFLICT (id) DO NOTHING'
        ),
        PersonFilmWork.__name__: (
            'PREPARE table_insert '
            '(uuid, text, timestamp with time zone, uuid, uuid) AS '
            'INSERT INTO content.person_film_work VALUES($1, $2, $3, $4, $5) '
            'ON CONFLICT (film_work_id, person_id, role) DO NOTHING'
        ),
        GenreFilmWork.__name__: (
            'PREPARE table_insert '
            '(uuid, timestamp with time zone, uuid, uuid) AS '
            'INSERT INTO content.genre_film_work VALUES($1, $2, $3, $4) '
            'ON CONFLICT (film_work_id, genre_id) DO NOTHING'
        ),
    }

    def __init__(self, conn: _connection):
        self.conn = conn
        self.curs = conn.cursor()

    def prepare(self, factory):
        self.curs.execute(PostgresSaver.prepare_queries[factory.__name__])

    def save_genres(self, data: List[Genre], batch_size=100):
        args = [asdict(genre) for genre in data]
        execute_batch(
            self.curs,
            "EXECUTE table_insert (NOW(), NOW(), %(id)s, %(name)s, %(description)s)",
            args,
            page_size=batch_size,
        )

    def save_person(self, data: List[Person], batch_size=100):
        args = [asdict(person) for person in data]
        execute_batch(
            self.curs,
            "EXECUTE table_insert (NOW(), NOW(), %(id)s, %(full_name)s)",
            args,
            page_size=batch_size,
        )

    def save_film_work(self, data: List[FilmWork], batch_size=100):
        args = [asdict(film_work) for film_work in data]
        execute_batch(
            self.curs,
            "EXECUTE table_insert (NOW(), NOW(), %(id)s, %(title)s, %(description)s, %(creation_date)s, %(rating)s, %(type)s, %(file_path)s)",
            args,
            page_size=batch_size,
        )

    def save_person_film_work(
        self, data: List[PersonFilmWork], batch_size=100
    ):
        args = [asdict(person_film_work) for person_film_work in data]
        execute_batch(
            self.curs,
            "EXECUTE table_insert (%(id)s, %(role)s, NOW(), %(film_work_id)s, %(person_id)s)",
            args,
            page_size=batch_size,
        )

    def save_genre_film_work(self, data: List[GenreFilmWork], batch_size=100):
        args = [asdict(genre_film_work) for genre_film_work in data]
        execute_batch(
            self.curs,
            "EXECUTE table_insert (%(id)s, NOW(), %(film_work_id)s, %(genre_id)s)",
            args,
            page_size=batch_size,
        )

    def deallocate_prepare(self):
        self.curs.execute('DEALLOCATE table_insert')


@contextmanager
def prepare_insert_context(saver: PostgresSaver, factory):
    saver.prepare(factory)
    yield
    saver.deallocate_prepare()


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    with prepare_insert_context(postgres_saver, Genre):
        for genres in sqlite_extractor.extract(Genre):
            postgres_saver.save_genres(genres)

    with prepare_insert_context(postgres_saver, Person):
        for person in sqlite_extractor.extract(Person):
            postgres_saver.save_person(person)

    with prepare_insert_context(postgres_saver, FilmWork):
        for film_work in sqlite_extractor.extract(FilmWork):
            postgres_saver.save_film_work(film_work)

    with prepare_insert_context(postgres_saver, PersonFilmWork):
        for person_film_work in sqlite_extractor.extract(PersonFilmWork):
            postgres_saver.save_person_film_work(person_film_work)

    with prepare_insert_context(postgres_saver, GenreFilmWork):
        for genre_film_work in sqlite_extractor.extract(GenreFilmWork):
            postgres_saver.save_genre_film_work(genre_film_work)


if __name__ == '__main__':
    dsl = {
        'dbname': 'movies_database',
        'user': 'app',
        'password': 'aquanox123',
        'host': '127.0.0.1',
        'port': 5432,
    }
    with sqlite_conn_context(
        'db.sqlite', read_only=True
    ) as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
