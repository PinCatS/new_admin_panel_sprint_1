from split_settings.tools import include

settings = [
    'components/base.py',  # standard django settings
    'components/database.py',  # postgres
]

include(*settings)
