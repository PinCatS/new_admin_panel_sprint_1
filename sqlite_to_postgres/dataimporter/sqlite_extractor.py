import sqlite3
from contextlib import contextmanager
from string import Template
from typing import Dict

from .models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


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


class SQLiteExtractor:
    QUERIES = {
        Genre.__name__: 'SELECT id, name, description FROM genre;',
        Person.__name__: 'SELECT id, full_name FROM person',
        FilmWork.__name__: (
            'SELECT id, title, description, creation_date, file_path, '
            'rating, type FROM film_work;'
        ),
        PersonFilmWork.__name__: (
            'SELECT id, film_work_id, person_id, role FROM person_film_work'
        ),
        GenreFilmWork.__name__: (
            'SELECT id, film_work_id, genre_id FROM genre_film_work'
        ),
    }

    def transform(row: sqlite3.Row) -> Dict:
        """Transform Row object to Dict.
        Substitutes description and rating NULL values with their default.
        """
        result = {}
        for key in row.keys():
            if key == 'description':
                result[key] = row[key] if row[key] else ''
            elif key == 'rating':
                result[key] = row[key] if row[key] else 0.0
            else:
                result[key] = row[key]
        return result

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def extract(self, model, batch_size=100):
        curs = self.conn.cursor()
        curs.execute(SQLiteExtractor.QUERIES[model.__name__])
        while data := curs.fetchmany(size=batch_size):
            objs = map(
                lambda row: model(**SQLiteExtractor.transform(row)), data
            )
            if data:
                yield objs
        curs.close()
        return
