import sqlite3
from contextlib import contextmanager
from string import Template

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

    def extract(self, model, batch_size=100):
        curs = self.conn.cursor()
        curs.execute(SQLiteExtractor.QUERIES[model.__name__])
        data = curs.fetchmany(size=batch_size)
        objs = map(lambda row: model(**row), data)
        while data:
            if data:
                yield objs
            data = curs.fetchmany(size=batch_size)
            objs = map(lambda row: model(**row), data)
        curs.close()
        return

    def extract_movies(self):
        return {
            'genres': list(self.extract_genres),
        }
