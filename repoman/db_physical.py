from playhouse.sqlite_ext import SqliteExtDatabase

import constants as c


database = SqliteExtDatabase(
    c.DB_PATH,
    autoconnect=False,
    rank_functions=True,
    pragmas=(
        ('cache_size', -1024 * 64),  # 64MB page-cache.
        ('journal_mode', 'wal'),     # Use WAL-mode (you should always use this!).
        ('foreign_keys', 1))         # Enforce foreign-key constraints.
)

def get_db_connection():
    if not database.is_closed():
        database.close()
    return database.connect()


def drop():
    if not database.is_closed():
        database.close()
    if c.DB_PATH.exists():
        c.DB_PATH.unlink()
