from django.contrib import admin

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'created', 'modified')
    search_fields = ('full_name', 'id')


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'created', 'modified')
    list_filter = ('name',)
    search_fields = ('id',)
    empty_value_display = '-пусто-'


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (
        GenreFilmworkInline,
        PersonFilmworkInline,
    )
    list_display = (
        'title',
        'description',
        'creation_date',
        'rating',
        'type',
        'created',
        'modified',
    )
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')
    empty_value_display = '-пусто-'
