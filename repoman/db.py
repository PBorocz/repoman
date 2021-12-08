# All methods for interfacing with and managing the SQLite database.
import sys
from sqlite3 import connect, OperationalError
from sqlite3 import Connection  # Typing only
from pathlib import Path
from typing import List, Dict, Tuple

from pdfminer.pdfparser import PDFSyntaxError
from rich import print

import constants as c
from utils import AnonymousObj


def get_db_conn():
    return connect(c.DB_PATH)

################################################################################
# Core database operations
################################################################################
def query(con: Connection, query_string: str):

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

    def get_docs_from_fts(con: Connection, query_string:str) -> List[AnonymousObj]:
        sql = """SELECT rowid,
                        snippet(document, 3, '[green bold]', '/[green bold]', '...', 5),
                        path,
                        suffix,
                        last_mod,
                        rank
                   FROM document
                  WHERE document MATCH ?
               ORDER BY rank"""
        rows = con.execute(sql, (query_string,))

        docs = list()
        for idx, row in enumerate(rows):
            doc_id, snippet, s_path, suffix, last_mod, rank = row
            path = Path(s_path)
            docs.append(AnonymousObj(
                doc_id   = doc_id,
                rank     = f"{rank:.2f}",
                path     = str(path.relative_to(*path.parts[:3])),
                suffix   = suffix,
                last_mod = last_mod,
                snippet  = snippet,
                ))
        return docs

    def get_docs_from_tags(con: Connection, docs: list, tag: str) -> List[AnonymousObj]:
        # Do we have a tag for this?
        sql = "SELECT rowid FROM tag WHERE tag = ?"
        row = con.execute(sql, (tag,)).fetchone()
        if not row:
            return []  # Nope, we're done..
        # We have a matching tag for this, do we have any documents associated with it?
        tag_id = row[0]

        sql = "SELECT doc_id FROM document_tag WHERE tag_id=?"
        rows = con.execute(sql, (tag_id,)).fetchall()
        if not rows:
            return []

        # Get the doc_id's from those already queried (so we don't list any docs twice that
        # match on text *and* tags!)
        doc_ids = {doc.doc_id: True for doc in docs}

        docs = list()
        for (doc_id,) in rows:
            if doc_id in doc_ids:
                continue        # Dedup..
            query = """SELECT path, suffix, last_mod FROM document WHERE rowid = ?"""
            s_path, suffix, last_mod = con.execute(query, (doc_id,)).fetchone()
            path = Path(s_path)
            docs.append(AnonymousObj(
                rank     = f" 0.00",
                path     = str(path.relative_to(*path.parts[:3])),
                suffix   = suffix,
                last_mod = last_mod,
                snippet  = f"Matched Tag: '[blue bold]{tag}[/blue bold]'",
            ))
        return docs

    docs_from_fts  = get_docs_from_fts (con, query_string)
    docs_from_tags = get_docs_from_tags(con, docs_from_fts, query_string)

    return docs_from_tags + docs_from_fts


def upsert_doc(con: Connection, doc: AnonymousObj) -> int:

    def check_delete_existing(con: Connection, path_: Path) -> bool:
        """Does a row exist already for this path?
        - If so, nuke it and return True
        - If not, do nothing and return False.
        """
        sql = "SELECT rowid FROM document WHERE path = ?"
        row = con.execute(sql, (str(path_),)).fetchone()
        if not row:
            return False

        doc_id = row[0]
        sql = "DELETE FROM document WHERE rowid = ?"
        con.execute(sql, (doc_id,))

        sql = "DELETE FROM document_tag WHERE doc_id = ?"
        con.execute(sql, (doc_id,))
        return True

        sql = "DELETE FROM document_link WHERE doc_id = ?"
        con.execute(sql, (doc_id,))
        return True

    # Clean out any existing row!
    check_delete_existing(con, doc.path_)

    # Do any final cleansing of the text to make sure it's "insertable"!
    cleansed = doc.body.replace("'", '"')

    # Do the insert..(note that body could be essentially empty, ie. '')
    csr = con.cursor()  # Use a cursor here to get access to the lastrowid aka doc_id
    sql = "INSERT INTO document(path, suffix, last_mod, body) VALUES (?, ?, ?, ?)"
    try:
        csr.execute(sql, (str(doc.path_), doc.suffix, doc.lmod, cleansed))
    except OperationalError as err:
        sys.stderr.write(f"{err}\n")
        return None
    doc_id = csr.lastrowid

    # Do we have any tags to handle?
    if doc.tags:
        for tag in doc.tags:
            tag_id = upsert_tag(con, tag)

        # Update that we have this tag assigned to this document (unless it exists already)
        upsert_document_tag(con, doc_id, tag_id)

    # Do we have any links to handle?
    if doc.links:
        for link in doc.links:
            upsert_document_link(con, doc_id, link)

    return doc_id

def upsert_tag(con: Connection, tag: str) -> int:
    """Upsert on the specified tag, return the tag_id associated with it."""
    # Essentially and upsert on the "tag" value itself.
    query = "SELECT tag FROM tag WHERE tag = ?"
    csr = con.cursor()  # Use a cursor here to get access to the lastrowid
    if not csr.execute(query, (tag,)).fetchone():
        sql = "INSERT INTO tag(tag) VALUES(?)"
        con.execute(sql, (tag,))
        return csr.lastrowid
    sql = "SELECT rowid FROM tag WHERE tag = ?"
    return con.execute(sql, (tag,)).fetchone()[0]


def upsert_document_tag(con: Connection, doc_id: int, tag_id:int) -> None:
    """Upsert on the document<->tag relationship"""
    sql = "SELECT rowid FROM document_tag WHERE tag_id=? AND doc_id=?"
    if not con.execute(sql, (tag_id, doc_id)).fetchone():
        sql = "INSERT INTO document_tag(tag_id, doc_id) VALUES(?, ?)"
        con.execute(sql, (tag_id, doc_id))


def upsert_document_link(con: Connection, doc_id: int, link: Tuple[str]) -> None:
    """Upsert on the document->link relationship."""
    sql = "SELECT rowid FROM document_link WHERE url=? AND doc_id=?"
    (url, desc) = link
    if not con.execute(sql, (url, doc_id)).fetchone():
        sql = "INSERT INTO document_link(doc_id, url, desc) VALUES(?, ?, ?)"
        con.execute(sql, (doc_id, url, desc))


def get_paths_already_indexed(con: Connection) -> Dict[Path, str]:
    """Return the paths and last-mod time of docs already indexed."""
    query = """SELECT path, last_mod FROM document"""
    return {Path(row[0]) : row[1] for row in con.execute(query).fetchall()}


################################################################################
# Database Status
################################################################################
def status(con: Connection):
    return_ = AnonymousObj()

    # Documents...
    try:
        query = "SELECT count(*) FROM document"
        return_.total_docs = con.execute(query).fetchone()[0]
    except OperationalError:
        print("Sorry, database hasn't been created yet. Please use .createdb to create a new database.")
        return None

    query = """SELECT suffix, count(*)
                 FROM document
             GROUP BY suffix
             ORDER BY count(*) DESC"""
    return_.suffix_counts = con.execute(query).fetchall()

    # Tags...
    query = "SELECT count(*) FROM tag"
    return_.total_tags = con.execute(query).fetchone()[0]

    query = """SELECT tag, count(*)
                 FROM document_tag
           INNER JOIN tag on tag.rowid = document_tag.tag_id
             GROUP BY tag_id
             ORDER BY count(*) DESC"""
    return_.tag_counts = con.execute(query).fetchall()

    # Links...
    query = "SELECT count(*) FROM document_link"
    return_.total_links = con.execute(query).fetchone()[0]

    return return_


################################################################################
# Database Maintenance
################################################################################
def clear(con: Connection):
    for table_ in ('document_link', 'document_tag', 'document', 'tag'):
        con.execute(f"DELETE FROM {table_}")
    con.commit()

def drop(con: Connection):
    if DB_PATH.exists():
        DB_PATH.unlink()


def create(con: Connection):
    """Create our database (in the user general config area)"""
    schema = ("""
    CREATE VIRTUAL TABLE IF NOT EXISTS document USING fts5(
	path,                 -- eg. ~/Repository/1.Projects/lapswim_timemap
	suffix   UNINDEXED,   -- eg. "org", or pdf, txt, py etc.
        last_mod UNINDEXED,   -- eg. 2021-11-29 or 2021-11-29T0929
        body,
        tokenize='porter ascii'
    );

    CREATE TABLE IF NOT EXISTS tag (
        tag TEXT
    );

    CREATE TABLE IF NOT EXISTS document_tag (
        tag_id INTEGER NOT NULL,
        doc_id INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS document_link (
        doc_id INTEGER NOT NULL,
        url    TEXT NOT NULL,
        desc   TEXT
    );
    """
    )
    con.executescript(schema)
