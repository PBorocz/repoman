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

class SO:
    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def query_db(str_):

    # The snippet() function is similar to highlight(), except that instead of returning entire
    # column values, it automatically selects and extracts a short fragment of document text to
    # process and return. The snippet() function must be passed five parameters following the
    # table name argument:
    # 1. An integer indicating the index of the FTS table column to select the returned text
    #    from. Columns are numbered from left to right starting at zero. A negative value
    #    indicates that the column should be automatically selected.
    # 2. The text to insert before each phrase match within the returned text.
    # 3. The text to insert after each phrase match within the returned text.
    # 4. The text to add to the start or end of the selected text to indicate that the returned
    #    text does not occur at the start or end of its column, respectively.
    # 5. The maximum number of tokens in the returned text. This must be greater than zero and
    #    equal to or less than 64.
    query = """SELECT snippet(docs, 3, '[bold italic]', '/[bold italic]', '...', 5),
                      path,
                      suffix,
                      last_mod,
                      rank
                 FROM docs
                WHERE docs MATCH '{0}'
             ORDER BY rank""".format(str_)

    csr = get_db_conn().cursor()
    csr.execute(query)

    results = list()
    for idx, row in enumerate(csr.fetchall()):
        snippet, s_path, suffix, last_mod, rank = row
        path = Path(s_path)
        results.append(SO(
            rank     = f"{rank:.2f}",
            path     = str(path.relative_to(*path.parts[:3])),
            suffix   = suffix,
            last_mod = last_mod,
            snippet  = snippet,
            ))
    return results

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
	path     UNINDEXED,   -- eg. ~/Repository/1.Projects/lapswim_timemap
	suffix   UNINDEXED,   -- eg. "org", or pdf, txt, py etc.
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
