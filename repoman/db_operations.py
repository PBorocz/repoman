# All methods for interfacing with and managing the SQLite database.
import sys
from collections import defaultdict
from pathlib import Path
from sqlite3 import Connection  # typing...
from sqlite3 import connect, OperationalError
from typing import List, Dict, Tuple
from urllib.parse import urlparse

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
                   DocumentFTS.body.snippet('>>>', '<<<', max_tokens=5).alias('snippet'))
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
                doc_id    = doc.id,
                rank      = f"{fts.bm25:.2f}",
                name      = doc_path.name,
                path_full = doc_path,
                path_rel  = str(doc_path.relative_to(*doc_path.parts[:3])), # FIXME! Won't always be 3!!
                suffix    = doc.suffix,
                last_mod  = doc.last_mod,
                snippet   = fts.snippet,
            )
            return_.append(ao_doc)

        return return_, doc_ids

    def get_docs_from_tags(doc_ids: list[int], tag: str) -> List[AnonymousObj]:
        query = DocumentTag.select(DocumentTag.doc_id).where(DocumentTag.tag == tag)
        if not query:
            return []

        # Depup against what we've already queried.
        tag_doc_ids = {dt_.doc_id for dt_ in query if dt_.doc_id not in doc_ids}

        docs = Document.select().where(Document.id.in_(tag_doc_ids))

        return_ = list()
        for doc in docs:
            doc_path = Path(doc.path)
            ao_doc = AnonymousObj(
                rank      = f" 0.00",
                name      = doc_path.name,
                path_full = doc_path,
                path_rel  = str(doc_path.relative_to(*doc_path.parts[:3])),
                suffix    = doc.suffix,
                last_mod  = doc.last_mod,
                snippet   = f"Tag: >>>{tag}<<<",
            )
            return_.append(ao_doc)
        return return_

    ################################################################################
    # Main: Query docs based on content *and* tag match:
    ################################################################################
    docs_from_fts, doc_ids = get_docs_from_fts (query_string)
    docs_from_tags         = get_docs_from_tags(doc_ids, query_string)

    return docs_from_tags + docs_from_fts


################################################################################
# Core method to "index" a new document.
################################################################################
def upsert_doc(a_doc: AnonymousObj) -> int:

    # Clean out any existing row!
    check_delete_existing(a_doc.path_)

    # Do any final cleansing of the text to make sure it's "insertable"!
    # If we didn't get any text, we still want to make an entry so we know
    # that we've considered the path already.

    # Do the into the "master" table:
    doc = Document(
        path     = str(a_doc.path_),
        suffix   = a_doc.suffix,
        last_mod = a_doc.lmod,
        last_idx = a_doc.now,
    )
    doc.save()

    # Now, insert the body content of the file into the text search table
    # (note that body could be essentially empty, ie. '')
    cleansed = a_doc.body.replace("'", '"').replace("\n", "") if a_doc.body else ""
    DocumentFTS.insert({
        DocumentFTS.rowid : doc.id,
        DocumentFTS.path  : str(a_doc.path_),
        DocumentFTS.body  : cleansed,
    }).execute()

    # Do we have any tags to handle?
    if a_doc.tags:
        for tag in a_doc.tags:
            DocumentTag.insert({
                DocumentTag.doc_id : doc.id,
                DocumentTag.tag    : tag,
            }).execute()

    # Do we have any links to handle?
    if a_doc.links:
        for link in a_doc.links:
            (url, desc) = link
            DocumentLink.insert({
                DocumentLink.doc_id : doc.id,
                DocumentLink.url    : url,
                DocumentLink.desc   : desc,
            }).execute()

    return doc.id


@retry(OperationalError, tries=10, delay=1.0)
def check_delete_existing(path_: Path) -> bool:
    """Does a row exist already for this path?
    - If so, nuke it and return True
    - If not, do nothing and return False.

    If db is locked, keep trying..
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


def link_summary() -> list[AnonymousObj]:
    """Summarise most popular link domains"""
    domain_counts = defaultdict(int)
    for link in DocumentLink.select():
        parsed = urlparse(link.url)
        domain_counts[parsed.netloc] += 1

    # Sort by count descending before returning
    return [
        AnonymousObj(url=url, count=count)
        for url, count in
        sorted(domain_counts.items(), key=lambda count: count[1])
    ]
