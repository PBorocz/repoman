import datetime
from collections import defaultdict
from pathlib import Path
from typing import Tuple, Iterable, List, Set, Dict

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


def get_last_mod(path_: Path) -> str:
    """Utility method to return an ISO-8601 formatted last mod date of path specified."""
    mtime = path_.stat().st_mtime
    return datetime.datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')


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

    def get_paths_already_indexed(self) -> Dict[Path, str]:
        """Return the paths and last-mod time of docs already indexed."""
        csr = self.conn.cursor()
        query = """SELECT path, last_mod FROM docs"""
        return {Path(row[0]) : row[1] for row in csr.execute(query).fetchall()}

    def get_paths_to_index(self, debug: bool, arg_dir: str, arg_suffix: str, arg_force: bool) -> Iterable:
        """Encapsulate all the logic regarding what files to be indexed,
        taking into account:
        - If we're filtering by suffix
        - If we're supposed to "reindex" files already indexed.
        - If the file hasn't been modified from the last time we indexed it.
        - Directories to skip outright.
        """
        path_dir = Path(arg_dir).expanduser().resolve()

        all_paths = set(walker(path_dir, SKIP_DIRS))
        if debug:
            print(f"{len(all_paths):6,d} files potentially indexable.")

        files_to_index = {path_ for path_ in all_paths if filter_by_suffix(path_, arg_suffix)}
        if debug:
            print(f"{len(files_to_index):6,d} files that match suffix: {arg_suffix}.")

        if arg_force:
            # Easy, index every file that passes our file "filters"!
            paths_to_index = files_to_index
        else:
            # We're not "force" index, consider only those docs that
            # a) Haven't been already indexed and
            # b) Those that *have* been already indexed by have changed since we index them originally.
            paths_already_indexed = self.get_paths_already_indexed()
            if debug:
                print(f"{len(paths_already_indexed):6,d} documents already indexed.")

            # a) Have we not indexed this file before?
            paths_missing = files_to_index - set(list(paths_already_indexed.keys()))
            if debug:
                print(f"{len(paths_missing):6,d} documents to be indexed since we haven't indexed them before.")

            # b) Has file has been modified more recently than our stored indexed version?
            paths_updated = set()
            for path_, lmod_doc in paths_already_indexed.items():
                if get_last_mod(path_) > lmod_doc:
                    paths_updated.add(path_)
            if debug:
                print(f"{len(paths_updated):6,d} documents have been updated since they were last indexed.")

            paths_to_index = paths_missing.union(paths_updated)
            if debug:
                print(f"{len(paths_to_index):6,d} documents to be indexed.")

        # Depending on the running, either decorate or not the list of paths
        # to be indexed.
        if debug:
            return paths_to_index
        return track(paths_to_index, description="Indexing...")


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
        lmod = get_last_mod(path_)
        if not body:
            return None
        return self._upsert_doc(path_, suffix, body, lmod)




    def _upsert_doc(self, path_: Path, suffix: str, body: str, lmod: str) -> int:
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
           INSERT INTO docs(
              path,
              suffix,
              last_mod,
              body
           ) VALUES (
              '{path_}',
              '{suffix}',
              '{lmod}',
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
