# All methods for interfacing with and managing the SQLite database.
import sqlite3
from pathlib import Path

from rich import print


CONFIG_PATH = Path.home() / Path(".config")
REPOMAN_PATH = CONFIG_PATH / Path("repoman")
REPOMAN_DB = "repoman.db"
DB_PATH = REPOMAN_PATH / Path(REPOMAN_DB)

def _confirm_db_dir():
    if not CONFIG_PATH.exists():
        raise RuntimeError("Sorry, we expect a general '~/.config' directory!")
    if not REPOMAN_PATH.exists():
        REPOMAN_PATH.mkdir()

def get_db_conn():
    return sqlite3.connect(DB_PATH)

def query_db(str_):
    query = """SELECT path, suffix FROM docs WHERE docs MATCH '{0}'""".format(str_)
    csr = get_db_conn().cursor()
    csr.execute(query)

    results = list()
    for idx, row in enumerate(csr.fetchall()):
        full_path, suffix = Path(row[0]), row[1]
        inner_path = full_path.relative_to(*full_path.parts[:3])
        results.append((str(inner_path), suffix))

    return results
    #     print(inner_path)

    # if 'idx' not in locals():
    # else:
    #     print()

def dropdb():
    if DB_PATH.exists():
        DB_PATH.unlink()

def createdb():
    """Create our database (in the user general config area)"""
    _confirm_db_dir()
    conn = get_db_conn()
    csr = conn.cursor()
    schema = ("""
    CREATE VIRTUAL TABLE docs USING fts5(
	path   UNINDEXED,     -- eg. ~/Repository/1.Projects/lapswim_timemap
	suffix UNINDEXED,     -- eg. "org", or pdf, txt, py etc.
        last_mod UNINDEXED,   -- eg. 2021-11-29 or 2021-11-29T0929
        body,
        tokenize='porter ascii'
    );
    ""","""
    CREATE TABLE IF NOT EXISTS tags (
	tag TEXT PRIMARY KEY
    );
    ""","""
    CREATE TABLE IF NOT EXISTS tags_docs (
	tag_id INTEGER NOT NULL,
        doc_id INTEGER NOT NULL
    );
    ""","""
    CREATE TABLE IF NOT EXISTS links (
	doc_id INTEGER NOT NULL,
        link TEXT NOT NULL,
        desc TEXT
    );
    """
    )
    csr.executescript(schema)
    conn.commit()
