import chardet
import datetime
import os
import re
import sys
from collections import defaultdict
from copy import copy
from pathlib import Path
from typing import Tuple, Iterable, List, Set, Dict

import pdfplumber
import sqlite3
from rich import print
from rich.progress import track

from db import upsert_doc


SKIP_DIRS = (
    ".git",
    ".venv",
    "venv",
    "_vm",
    ".vm",
    "__pycache__",
    "node_modules",
)
FILENAME_TAG_SEPARATOR = " -- "
FILE_WITH_TAGS_REGEX   = re.compile(r'(.+?)' + FILENAME_TAG_SEPARATOR + r'(.+?)(\.(\w+))??$')
YYYY_MM_DD_PATTERN     = re.compile(r'^(\d{4,4})-([01]\d)-([0123]\d)[- _T]')


class Index:
    def __init__(self, conn):
        self.conn = conn
        self.suffixes_skipped = defaultdict(int)

    def get_paths_already_indexed(self) -> Dict[Path, str]:
        """Return the paths and last-mod time of docs already indexed."""
        csr = self.conn.cursor()
        query = """SELECT path, last_mod FROM docs"""
        return {Path(row[0]) : row[1] for row in csr.execute(query).fetchall()}

    def get_paths_to_index(self, verbose: bool, arg_dir: str, arg_suffix: str, arg_force: bool) -> Iterable:
        """Encapsulate all the logic regarding what files to be indexed,
        taking into account:
        - If we're filtering by suffix
        - If we're supposed to "reindex" files already indexed.
        - If the file hasn't been modified from the last time we indexed it.
        - Directories to skip outright.
        """
        path_dir = Path(arg_dir).expanduser().resolve()

        all_paths = set(walker(path_dir, SKIP_DIRS))
        if verbose:
            print(f"{len(all_paths):6,d} files potentially indexable.")

        files_to_index = {path_ for path_ in all_paths if filter_by_suffix(path_, arg_suffix)}
        if verbose:
            print(f"{len(files_to_index):6,d} files that match suffix: {arg_suffix}.")

        if arg_force:
            # Easy, index every file that passes our file "filters"!
            paths_to_index = files_to_index
        else:
            # We're not "force" index, consider only those docs that
            # a) Haven't been already indexed and
            # b) Those that *have* been already indexed by have changed since we index them originally.
            paths_already_indexed = self.get_paths_already_indexed()
            if verbose:
                print(f"{len(paths_already_indexed):6,d} documents already indexed.")

            # a) Have we not indexed this file before?
            paths_missing = files_to_index - set(list(paths_already_indexed.keys()))
            if verbose:
                print(f"{len(paths_missing):6,d} documents to be indexed since we haven't indexed them before.")

            # b) Has file has been modified more recently than our stored indexed version?
            paths_updated = set()
            for path_, lmod_doc in paths_already_indexed.items():
                if get_last_mod(path_) > lmod_doc:
                    paths_updated.add(path_)
            if verbose:
                print(f"{len(paths_updated):6,d} documents have been updated since they were last indexed.")

            paths_to_index = paths_missing.union(paths_updated)
            if verbose:
                print(f"{len(paths_to_index):6,d} documents to be indexed.")

        # Depending on the running, either decorate or not the list of paths
        # to be indexed.
        if verbose:
            return paths_to_index
        return track(paths_to_index, description="Indexing...")


    def index(self, verbose: bool, arg_dir: str, arg_suffix: str, arg_force: bool) -> int:
        """
        CORE METHOD: Get an iterator of files to be indexed and return the number that worked.
        """
        iterator = self.get_paths_to_index(verbose, arg_dir, arg_suffix, arg_force)
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
        body_method = None
        if suffix == "txt":
            body_method = self.get_body_txt

        elif suffix == "org":
            body_method = self.get_body_org

        elif suffix == "pdf":
            body_method = self.get_body_pdf

        else:
            # Keep track of the suffixes we're not dealing with right now..
            self.suffixes_skipped[suffix] += 1
            return None

        if body_method:
            body, links = body_method(path_)
        else:
            # Even if we can't read the content, the file may still have tags and
            # we want to the filename to know we *tried* indexing it at least.
            body = ''

        lmod = get_last_mod(path_)
        tags = get_tags(path_)

        return upsert_doc(self.conn, path_, suffix, body, lmod, tags, links)


    def get_body_txt(self, path_, suffix="txt"):
        """Insert an index for a text file"""
        # Find out what encoding to use first...
        with open(path_, 'rb') as fh_:
            rawdata = fh_.read()
            result = chardet.detect(rawdata)
            charenc = result['encoding']

        # ...then we can open the file with an encoding that should work.
        with open(path_, encoding=charenc) as fh_:
            return fh_.read(), None


    def get_body_org(self, path_, suffix="org"):
        """Insert an index for an org file"""
        text = []
        links = []
        with open(path_, encoding=charenc) as fh_:
            for line in fh_.readlines():

                text.append(copy(line))  # We're going to modify line below..

                # The only extra thing we look for from org files are links
                while '[[' in line:
                    # 'asdasdfsd [[https:/www.google.com][Google search]] asdf asdf asdf
                    # 'asdasdfsd [[https:/www.google.com]] asdf asdf asdf
                    _, rest = line.split('[[')
                    url_desc, rest = rest.split(']]')
                    if '][' in url_desc:
                        # [[https:/www.google.com][Google search]]
                        url, desc = url_desc.split('][')
                    else:
                        # [[https:/www.google.com]]
                        url, desc = url_desc, None
                    links.append((url, desc))

                    _, line = line.split(']]')  # Any more on the line?

        return ' '.join(text), links


    def get_body_pdf(self, path_, suffix="pdf"):
        """Insert an index for an pdf file"""
        with pdfplumber.open(path_) as pdf:
            text = []
            for ith, page in enumerate(pdf.pages, 1):
                page_text = page.extract_text()
                if page_text:
                    text.append(page_text)
        return ' '.join(text), None


################################################################################
# Utility methods
################################################################################


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


def get_last_mod(path_: Path)-> str:
    """Utility method to return last mod date:

    - If the filepath conforms to the 'Novoid' filetagging syntax, we use any
      explicitly-set last modification date.

     -If not, we use the underlying OS' definition of last-mode datetime.
    """
    components = re.match(YYYY_MM_DD_PATTERN, path_.name)
    if components:
        yyyy, mm, dd = [components.group(1), components.group(2), components.group(3)]
        return f"{yyyy}-{int(mm):02d}-{int(dd):02d} 00:00:00"

    f_mtime = path_.stat().st_mtime
    return datetime.datetime.fromtimestamp(f_mtime).strftime('%Y-%m-%d %H:%M:%S')


def get_tags(path_: Path) -> List[str]:
    """Utility method to return any tags embedded in the file path if the filepath conforms to
    the 'Novoid' filetagging syntax, we can extract "tags".

    RegEx's cribbed from https://github.com/novoid/filetags/blob/master/filetags/__init__.py
    """
    components = re.match(FILE_WITH_TAGS_REGEX, path_.name)
    if components:
        return components.group(2).split(' ')
    return []


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
