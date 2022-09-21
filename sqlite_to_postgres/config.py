import os

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_CONFIG_PATH = os.path.join(BASE_DIR, 'movies_admin/config/.env')

# SQLite source db from where data is imported to main PostgreSQL
SQLITE_DB = os.path.join(BASE_DIR, 'sqlite_to_postgres/db.sqlite')

load_dotenv(DB_CONFIG_PATH)

DATABASE = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', default='127.0.0.1'),
    'port': os.getenv('DB_PORT', default=5432),
}
