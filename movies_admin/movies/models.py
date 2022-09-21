from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

from .mixins import TimeStampedMixin, UUIDMixin


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('person'), max_length=64, unique=True)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('people')


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=64, unique=True)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')

    def __str__(self):
        return self.name


class FilmType(models.TextChoices):
    MALE = 'movie', _('movie')
    FEMALE = 'tv_show', _('tv show')


class Filmwork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_('name'), max_length=128)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation date'), null=True)
    file_path = models.FileField(
        _('file'), blank=True, null=True, upload_to='movies/'
    )
    rating = models.FloatField(
        _('rating'),
        default=0.0,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
    )
    type = models.CharField('тип', choices=FilmType.choices, max_length=64)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    person = models.ManyToManyField(Person, through='PersonFilmwork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('film work')
        verbose_name_plural = _('film works')
        constraints = [
            models.UniqueConstraint(
                fields=['title', 'creation_date'], name='unique_filmwork'
            )
        ]

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        constraints = [
            models.UniqueConstraint(
                fields=['film_work', 'genre'], name='unique_filmwork_genre'
            )
        ]


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField('role', null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        constraints = [
            models.UniqueConstraint(
                fields=['film_work', 'person', 'role'],
                name='unique_filmwork_person',
            )
        ]
