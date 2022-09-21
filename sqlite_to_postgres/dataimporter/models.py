import uuid
from dataclasses import dataclass, field


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
