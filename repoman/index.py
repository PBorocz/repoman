from collections import defaultdict
from pathlib import Path
from typing import Tuple, Iterable, List, Set

import pdfplumber
import sqlite3
from rich import print
from rich.progress import track

SKIP_DIRS = (
    ".git",
    ".venv",
    "venv",
    "_vm",
    ".vm",
    "__pycache__",
    "node_modules",
)


def walker(path: Path, skip_dirs: list) -> Iterable[Path]:
    """Return all files recursively from the specified path on down, skipping any directories
    specified.
    """
    def _walk(path):
        for p in Path(path).iterdir():
            if p.is_dir() and p.name not in skip_dirs:
                yield from _walk(p)
                continue
            yield p.resolve()
    for path_ in _walk(path):
        yield path_


def filter_by_suffix(path_, suffix):
    if not suffix:
        # If we're not filtering by suffix, noop.
        return True
    if not path_.suffix:
        # We are filtering but this file has no suffix, noop
        return False
    if suffix.lower() == path_.suffix.lower():
        # We are filtering and the suffix of this path matches the suffix requested.
        return True
    return False


class Index:
    def __init__(self, conn):
        self.conn = conn

    def get_paths_already_indexed(self) -> Set[Path]:
        csr = self.conn.cursor()
        query = """SELECT path FROM docs"""
        csr.execute(query)
        return {Path(row[0]) for row in csr.fetchall()}

    def get_paths_to_index(self, debug: bool, arg_dir: str, arg_suffix: str, arg_force: bool) -> Iterable:
        """Encapsulate all the logic regarding what files to be indexed,
        taking into account:
        - If we're filtering by suffix
        - If we're supposed to "reindex" files already indexed.
        - Directories to skip outright.
        """
        path_dir  = Path(arg_dir).expanduser().resolve()

        all_paths = set(walker(path_dir, SKIP_DIRS))
        if debug:
            print(f"{len(all_paths):4d} files potentially indexable.")

        files_to_index = {path_ for path_ in all_paths if filter_by_suffix(path_, arg_suffix)}
        if debug:
            print(f"{len(files_to_index):4d} files that match suffix: {arg_suffix}.")

        if not arg_force:
            # Unless we're forcing, only index those doc's that haven't already been processed.
            paths_already_indexed = self.get_paths_already_indexed()
            if debug:
                print(f"{len(paths_already_indexed):4d} documents already indexed")

            files_to_index -= paths_already_indexed
            if debug:
                print(f"{len(files_to_index):4d} documents to be indexed")

        if not files_to_index:
            return None

        if debug:
            return files_to_index

        # Non-debug mode, use a progress bar..
        return track(files_to_index, description="Indexing...")


    def index(self, debug: bool, arg_dir: str, arg_suffix: str, arg_force: bool) -> int:
        """
        CORE METHOD: Get an iterator of files to be indexed and return the number that worked.
        """
        iterator = self.get_paths_to_index(debug, arg_dir, arg_suffix, arg_force)
        if not iterator:
            return None

        count_indexed = 0
        for path_ in iterator:
            if self._index(path_):
                count_indexed += 1
        return count_indexed

    def _index(self, path_):
        suffix = path_.suffix.lower()[1:]

        # FIXME: Good case for new "match" semantic?
        if suffix == "txt":
            body_method = self.get_body_txt

        elif suffix == "org":
            body_method = self.get_body_org

        elif suffix == "pdf":
            body_method = self.get_body_pdf

        else:
            return None

        body = body_method(path_)
        if not body:
            return None
        return self._upsert_doc(path_, suffix, body)

    def _upsert_doc(self, path_, suffix, body):
        csr = self.conn.cursor()

        # Does this row exist already? If so, nuke it.
        query = """SELECT path FROM docs WHERE path = ?"""
        if csr.execute(query, (str(path_),)).fetchall():
            delete = """DELETE FROM docs WHERE path = ?"""
            csr.execute(delete, (str(path_),))
            self.conn.commit()

        # Do the insert..
        cleansed = body.replace("'", '"')
        insert = f"""
           INSERT INTO docs(path, suffix, body) VALUES(
           '{path_}',
           '{suffix}',
           '{cleansed}'
        )
        """
        try:
            csr.execute(insert)
        except sqlite3.OperationalError as err:
            print(err)
        self.conn.commit()
        return csr.lastrowid


    def get_body_txt(self, path_, suffix="txt"):
        """Insert an index for a text file"""
        return ''.join(path_.read_text())


    def get_body_org(self, path_, suffix="org"):
        """Insert an index for an org file"""
        return ''.join(path_.read_text())


    def get_body_pdf(self, path_, suffix="pdf"):
        """Insert an index for an pdf file"""
        with pdfplumber.open(path_) as pdf:
            text = []
            for ith, page in enumerate(pdf.pages, 1):
                page_text = page.extract_text()
                if page_text:
                    text.append(page_text)
        return ' '.join(text)
