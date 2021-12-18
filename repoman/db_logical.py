import datetime

from peewee import *
from playhouse.sqlite_ext import SqliteExtDatabase, FTS5Model, RowIDField, SearchField
from sqlite3 import Connection  # typing

import constants as c
from db_physical import database, get_db_connection, drop


class Document(Model):
    path     = CharField    (index=True) # Here as primary key, also stored below for search
    suffix   = CharField    (index=False, null=True)
    last_mod = DateTimeField(index=False, formats=(c.DB_DATETIME_FORMAT,))
    last_idx = DateTimeField(index=False, formats=(c.DB_DATETIME_FORMAT,), default=datetime.datetime.now)

    class Meta:
        database = database
        table_name = "document"


class DocumentFTS(FTS5Model):
    rowid = RowIDField()
    path  = SearchField()  # Yes, we store path twice..here again so we can search on it..
    body  = SearchField()

    class Meta:
        database   = database
        table_name = "document_fts"
        options    = {'tokenize': 'porter'}


class DocumentTag(Model):
    doc_id = ForeignKeyField(Document, backref="tags")
    tag = CharField()

    class Meta:
        database   = database
        table_name = "document_tag"


class DocumentLink(Model):
    doc_id = ForeignKeyField(Document, backref="links")
    url    = CharField()
    desc   = CharField(null=True)

    class Meta:
        database   = database
        table_name = "document_link"

MODELS = [Document, DocumentFTS, DocumentLink, DocumentTag]

def create_schema():
    database.connect(reuse_if_open=True)
    database.create_tables(MODELS)


def clear():
    # Since models are sorted in "definition" order, we need to delete in reverse order:
    database.connect(reuse_if_open=True)
    for entity in MODELS[::-1]:
        entity.delete().execute()


def test():

    if False:
        clear()

    if True:
        create_schema()

    path = "aPath/anotherFilename.org"
    doc = Document.create(
        path     = path,
        suffix   = "org",
        last_mod = "2021-12-09 10:59",
        last_idx = "2021-12-09 10:59"
    )
    doc_id = doc

    DocumentFTS.insert({
        DocumentFTS.rowid : doc_id,
        DocumentFTS.path  : path,
        DocumentFTS.body  : "another org entry",
    }).execute()

    DocumentLink.insert({
        DocumentLink.doc_id : doc_id,
        DocumentLink.url : "aUrl",
        DocumentLink.desc : "A Description"
    }).execute()

    DocumentTag.insert({
        DocumentTag.doc_id : doc_id,
        DocumentTag.tag : "aTag",
    }).execute()

    res = (DocumentFTS
           .search_bm25(
               "org OR foobar",
               with_score=True,
               score_alias="rank"
           )
           .select(
               DocumentFTS,
               DocumentFTS.bm25().alias('bm25'),
               DocumentFTS.body.highlight('>>>', '<<<').alias('snippet'))
           .order_by(DocumentFTS.bm25().desc())
    )

if __name__ == '__main__':
    test()
