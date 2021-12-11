# All methods for interfacing with and managing the SQLite database.
import sys
from collections import defaultdict
from sqlite3 import connect, OperationalError
from sqlite3 import Connection  # typing...
from pathlib import Path
from typing import List, Dict, Tuple

from pdfminer.pdfparser import PDFSyntaxError
from peewee import fn
from rich import print

import constants as c
from db_logical import Document, DocumentTag, DocumentLink, DocumentFTS
from db_physical import database
from utils import AnonymousObj, retry

################################################################################
# Core database operations
################################################################################
def query(query_string: str):

    def get_docs_from_fts(query_string:str) -> List[AnonymousObj]:
        fts = (DocumentFTS
               .search_bm25(query_string)
               .select(
                   DocumentFTS,
                   DocumentFTS.bm25().alias('bm25'),
                   DocumentFTS.body.snippet('[green bold]', '[/green bold]', max_tokens=5).alias('snippet'))
               .order_by(DocumentFTS.bm25().desc())
               )
        doc_ids = [doc.rowid for doc in fts]

        # Now, get the matching document definitions..
        docs = Document.select().where(Document.id.in_(doc_ids))
        doc_objs = {doc.id : doc for doc in docs}

        # And laminate them both together..
        return_ = list()
        for fts in fts:
            doc = doc_objs.get(fts.rowid)
            doc_path = Path(doc.path)
            ao_doc = AnonymousObj(
                doc_id   = doc.id,
                rank     = f"{fts.bm25:.2f}",
                path     = str(doc_path.relative_to(*doc_path.parts[:3])),
                suffix   = doc.suffix,
                last_mod = doc.last_mod,
                snippet  = fts.snippet,
            )
            return_.append(ao_doc)

        return return_, doc_ids

    def get_docs_from_tags(doc_ids: list[int], tag: str) -> List[AnonymousObj]:
        query = DocumentTag.select(DocumentTag.doc_id).where(DocumentTag.tag == tag)
        if not query:
            return []

        tag_doc_ids = {dt_.doc_id for dt_ in query if dt_.doc_id not in doc_ids}

        docs = Document.select().where(Document.id.in_(tag_doc_ids))

        return_ = list()
        for doc in docs:
            doc_path = Path(doc.path)
            ao_doc = AnonymousObj(
                rank     = f" 0.00",
                path     = str(doc_path.relative_to(*doc_path.parts[:3])),
                suffix   = doc.suffix,
                last_mod = doc.last_mod,
                snippet  = f"Matched Tag: '[blue bold]{tag}[/blue bold]'",
            )
            return_.append(ao_doc)
        return return_

    docs_from_fts, doc_ids  = get_docs_from_fts (query_string)
    docs_from_tags = get_docs_from_tags(doc_ids, query_string)

    return docs_from_tags + docs_from_fts


################################################################################
# Core method to insert a new document!
################################################################################
def upsert_doc(a_doc: AnonymousObj) -> int:

    # Clean out any existing row!
    check_delete_existing(a_doc.path_)

    # Do any final cleansing of the text to make sure it's "insertable"!
    # If we didn't get any text, we still want to make an entry so we know
    # that we've considered the path already.
    cleansed = a_doc.body.replace("'", '"') if a_doc.body else ''

    # Do the insert..(note that body could be essentially empty, ie. '')
    doc = Document(
        path     = str(a_doc.path_),
        suffix   = a_doc.suffix,
        last_mod = a_doc.lmod,
        last_idx = a_doc.lmod,    # FIXME!
    )
    doc.save()
    doc_id = doc.id

    DocumentFTS.insert({
        DocumentFTS.rowid : doc_id,
        DocumentFTS.path  : str(a_doc.path_),
        DocumentFTS.body  : cleansed,
    }).execute()

    # Do we have any tags to handle?
    if doc.tags:
        for tag in doc.tags:
            DocumentTag.insert({
                DocumentTag.doc_id : doc_id,
                DocumentTag.tag    : tag,
            }).execute()

    # Do we have any links to handle?
    if doc.links:
        for link in doc.links:
            (url, desc) = link
            DocumentLink.insert({
                DocumentLink.doc_id : doc_id,
                DocumentLink.url    : url,
                DocumentLink.desc   : desc,
            }).execute()
        print(f"Inserted {len(doc.links)} links!")

    return doc_id


@retry(OperationalError, tries=5, delay=1.0)
def check_delete_existing(path_: Path) -> bool:
    """Does a row exist already for this path?
    - If so, nuke it and return True
    - If not, do nothing and return False.
    """
    docs = Document.select().where(Document.path == str(path_))
    if not docs:
        return False

    for doc in docs:
        DocumentTag.delete().where(DocumentTag.doc_id==doc.id).execute()
        DocumentLink.delete().where(DocumentLink.doc_id==doc.id).execute()
        DocumentFTS.delete().where(DocumentFTS.path==doc.path).execute()
        doc.delete_instance()
    return True


def get_paths_already_indexed() -> Dict[Path, str]:
    """Return the paths and last-mod time of docs already indexed."""
    return {doc.path : doc for doc in Document.select()}


################################################################################
# Database Status
################################################################################
def status() -> AnonymousObj:

    return_ = AnonymousObj()

    # Documents...
    return_.total_docs = Document.select().count()

    # FIXME: How to do this in peewee?
    #     query = """SELECT suffix, count(*)
    #                  FROM document
    #              GROUP BY suffix
    #              ORDER BY count(*) DESC"""
    return_.suffix_counts = defaultdict(int)
    for doc in Document.select(Document.suffix):
        return_.suffix_counts[doc.suffix] += 1

    # Tags...
    return_.total_tags = DocumentTag.select().count()

    # Links...
    return_.total_links = DocumentLink.select().count()

    return return_

def tag_count():
    """Count of documents per tag, order by count desc"""
    return DocumentTag.select(DocumentTag.tag, fn.COUNT()).group_by(DocumentTag.tag)
