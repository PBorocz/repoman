# All methods for interfacing with and managing the SQLite database.
import sqlite3
from pathlib import Path
from typing import List

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

    def get_docs_from_fts(csr, str_:str) -> List[SO]:
        sql = """SELECT snippet(docs, 3, '[green bold]', '/[green bold]', '...', 5),
                        path,
                        suffix,
                        last_mod,
                        rank
                   FROM docs
                  WHERE docs MATCH ?
               ORDER BY rank"""
        csr.execute(sql, (str_,))

        docs = list()
        for idx, row in enumerate(csr.fetchall()):
            snippet, s_path, suffix, last_mod, rank = row
            path = Path(s_path)
            docs.append(SO(
                rank     = f"{rank:.2f}",
                path     = str(path.relative_to(*path.parts[:3])),
                suffix   = suffix,
                last_mod = last_mod,
                snippet  = snippet,
                ))
        return docs

    def get_docs_from_tags(csr, tag: str) -> List[SO]:
        # Do we have a tag for this?
        sql = "SELECT rowid FROM tags WHERE tag = ?"
        row = csr.execute(sql, (tag,)).fetchone()
        if not row:
            return []  # Nope, we're done..
        # We have a matching tag for this, do we have any documents associated with it?
        tag_id = row[0]

        sql = "SELECT doc_id FROM tags_docs WHERE tag_id=?"
        rows = csr.execute(sql, (tag_id,)).fetchall()
        if not rows:
            return []

        docs = list()
        for (doc_id,) in rows:
            query = """SELECT path, suffix, last_mod FROM docs WHERE rowid = ?"""
            s_path, suffix, last_mod = csr.execute(query, (doc_id,)).fetchone()
            path = Path(s_path)
            docs.append(SO(
                rank     = f" 0.00",
                path     = str(path.relative_to(*path.parts[:3])),
                suffix   = suffix,
                last_mod = last_mod,
                snippet  = f"Matched Tag: '[blue bold]{tag}[/blue bold]'",
            ))
        return docs

    csr = get_db_conn().cursor()
    return get_docs_from_tags(csr, str_) + get_docs_from_fts(csr, str_)


def upsert_doc(
        conn,
        path_: Path,
        suffix: str,
        body: str,
        lmod: str,
        tags: List[str],
        links: List[str]) -> int:

    csr = conn.cursor()

    def check_delete_existing(csr, path_: Path) -> None:
        """Does a row exist already for this path? If so, nuke it."""
        sql = "SELECT rowid FROM docs WHERE path = ?"
        row = csr.execute(sql, (str(path_),)).fetchone()
        if not row:
            return
        doc_id = row[0]
        sql = "DELETE FROM docs WHERE rowid = ?"
        csr.execute(sql, (doc_id,))

        sql = "DELETE FROM tags_docs WHERE doc_id = ?"
        csr.execute(sql, (doc_id,))
        conn.commit()

    # Clean out any existing row!
    check_delete_existing(csr, path_)

    # Do the insert..(note that body could be essentially empty, ie. '')
    cleansed = body.replace("'", '"')
    sql = "INSERT INTO docs(path, suffix, last_mod, body) VALUES (?, ?, ?, ?)"
    try:
        csr.execute(sql, (str(path_), suffix, lmod, cleansed))
    except sqlite3.OperationalError as err:
        print(err)
    conn.commit()
    doc_id = csr.lastrowid

    # Do we have any tags to handle?
    if tags:
        for tag in tags:
            query = "SELECT tag FROM tags WHERE tag = ?"
            if not csr.execute(query, (tag,)).fetchone():
                sql = "INSERT INTO tags(tag) VALUES(?)"
                csr.execute(sql, (tag,))
                tag_id = csr.lastrowid
                conn.commit()
            else:
                sql = "SELECT rowid FROM tags WHERE tag = ?"
                tag_id = csr.execute(sql, (tag,)).fetchone()[0]

        # Update that we have this tag assigned to this document (unless it exists already)
        sql = "SELECT rowid FROM tags_docs WHERE tag_id=? AND doc_id=?"
        if not csr.execute(sql, (tag_id, doc_id)).fetchone():
            sql = "INSERT INTO tags_docs(tag_id, doc_id) VALUES(?, ?)"
            csr.execute(sql, (tag_id, doc_id))
            conn.commit()

    return doc_id

################################################################################
# Database Maintenance!
################################################################################
def cleardb():
    conn = get_db_conn()
    csr = conn.cursor()
    for table_ in ('tags_docs', 'docs', 'tags'):
        csr.execute(f"DELETE FROM {table_}")
    conn.commit()

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
	path,                 -- eg. ~/Repository/1.Projects/lapswim_timemap
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
