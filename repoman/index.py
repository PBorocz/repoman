import chardet
import datetime
import os
import re
import sys
from collections import defaultdict
from copy import copy
from pathlib import Path
from typing import Tuple, Iterable, List, Set, Dict

import sqlite3
from pdfminer.high_level import extract_text
from rich import print
from rich.progress import track

from db import upsert_doc, get_paths_already_indexed
from utils import AnonymousObj, progressIndicator


SKIP_DIRS = (
    ".git",
    ".hg",
    ".venv",
    "venv",
    "_vm",
    ".vm",
    "__pycache__",
    "node_modules",
)

INCLUDE_EXTENSIONS = (
    ".txt",
    ".org",
    ".gif",
    ".pdf",
    ".mp4", ".mov",
    ".jpg", ".jepg", ".jpg_large",
    ".png",
)

FILENAME_TAG_SEPARATOR = " -- "
FILE_WITH_TAGS_REGEX   = re.compile(r'(.+?)' + FILENAME_TAG_SEPARATOR + r'(.+?)(\.(\w+))??$')
YYYY_MM_DD_PATTERN     = re.compile(r'^(\d{4,4})-([01]\d)-([0123]\d)[- _T]')


class Index:
    def __init__(self, conn):
        self.conn = conn

    def index(self, verbose: bool, index_command: AnonymousObj) -> int:
        """
        CORE METHOD: Get an iterator of files to be indexed and return the number that worked.
        """
        # convert index_force to boolean
        b_force = False if not index_command or not index_command.force.lower().startswith('y') else True

        iterator = self.iter_paths_to_index(
            verbose,
            Path(index_command.dir).expanduser().resolve(),
            index_command.suffix,
            b_force)

        pi = progressIndicator(level="low")
        for path_ in iterator:
            self._index(path_)
            self.conn.commit()
            pi.update()
        pi.final()

        return pi.get_count()


    def iter_paths_to_index(self, verbose: bool, path_dir: Path, arg_suffix: str, arg_force: bool) -> Iterable:
        """Encapsulate all the logic regarding what files to be indexed,
        taking into account:
        - If we're filtering by suffix (here specified *without* the leading '.', e.g. pdf, org, text ...)
        - If we're supposed to "reindex" files already indexed.
        - If the file hasn't been modified from the last time we indexed it.
        - Directories to skip outright.
        """
        # Before we start, what files have already been index?
        paths_already_indexed = get_paths_already_indexed(self.conn)

        for path_ in walker(path_dir, arg_suffix, SKIP_DIRS, INCLUDE_EXTENSIONS):
            if arg_force:
                yield path_  # Easy, index every file that passes our file "filters"!

            # Have we not indexed this file before?
            is_path_not_indexed = path_ not in paths_already_indexed

            # Has file has been modified more recently than our stored indexed version?
            lmod_doc = paths_already_indexed.get(path_, None)
            has_doc_been_updated = get_last_mod(path_) > lmod_doc if lmod_doc else None

            # Decide to take/not take the path under consideration
            if is_path_not_indexed:
                yield path_  # Easy case, we haven't seen it yet!
            else:
                # We've already seen it...but...
                # has it been updated since we last indexed it?
                if has_doc_been_updated:
                    yield path_


    def _index(self, path_):
        """Index the file on the specified path!"""
        so_doc = AnonymousObj(
            path_  = path_,
            suffix = path_.suffix.lower()[1:],
            body   = '',                  # Assume empty until we can pull anything out...
            links  = [],                  # "
            lmod   = get_last_mod(path_), # When was the file tagged or last modified?
            tags   = get_tags(path_),     # What are any file-specific (ie. Novoid) tags?
        )

        # For specific extensions that we *can* get meaningful text from,
        # find the specific method to do so and get it!
        get_text_method = dict(
            txt = self.get_text_from_txt,
            org = self.get_text_from_org,
            pdf = self.get_text_from_pdf,
        ).get(so_doc.suffix)

        if get_text_method:
            so_doc.body, so_doc.links = get_text_method(path_)

        # Update/insert the doc into our database
        return upsert_doc(self.conn, so_doc)


    def get_text_from_txt(self, path_, suffix="txt"):
        """Get text from a txt file"""
        with open(path_, encoding=get_file_encoding(path_)) as fh_:
            return fh_.read(), None


    def get_text_from_org(self, path_, suffix="org"):
        """Get text and links from an org file"""
        text  = []
        links = []
        encoding = get_file_encoding(path_)
        try:
            with open(path_, encoding=encoding, errors='ignore') as fh_:
                for lineno, line in enumerate(fh_, 1):
                    clean = line.strip()

                    # Core text capture for filtering..
                    if clean:
                        text.append(clean)

                    # Additionally, look for links..
                    links = get_org_links(path_, lineno, line.strip())

        except UnicodeDecodeError as err:
            print(path_)
            print(encoding)
            print(err)
            breakpoint()

        return ' '.join(text), links


    def get_text_from_pdf(self, path_, suffix="pdf"):
        """Get text from a pdf file"""
        try:
            return extract_text(path_), None
        except PDFSyntaxError as err:
            print(f"\nSorry, {path_} may not be a valid PDF?")
            return None


################################################################################
# Utility methods
################################################################################
def get_file_encoding(path_: Path) -> str:
    with open(path_, 'rb') as fh_:
        rawdata = fh_.read()
        result = chardet.detect(rawdata)
        return result['encoding']
    return None


def walker(arg_path: Path, arg_suffix: str, skip_dirs: list[str], include_suffixes: list[str]) -> Iterable[Path]:
    """Return all files recursively from the specified path on down, skipping any directories
    specified and filtering by suffix explicitly if provided or implicitly based on built-in
    list of suffixes we're supporting indexing on behalf of.
    """
    def _walk(walk_path):
        for path_ in Path(walk_path).iterdir():
            if path_.is_dir() and path_.name not in skip_dirs:
                yield from _walk(path_)
                continue
            yield path_

    for path_ in _walk(arg_path):
        if arg_suffix:
            # We're filtering by suffix (inclusive!)
            if arg_suffix.lower() == path_.suffix.lower()[1:]:
                yield path_
        else:
            # No suffix preference...still, only include files that we're explicitly indexing
            if path_.suffix.lower() in include_suffixes:
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


def get_org_links(path_: Path, lineno: int, line: str) -> List[str]:
    """The only extra thing we look for from org files are links.
    A set of primary cases here:
    - asdasdfsd [[https:/www.google.com][Google search]] asdf asdf asdf
    - asdasdfsd [[https:/www.google.com]] asdf asdf asdf
    - asdasdfsd [[https:/www.google.com]] asdasdfsd [[https:/www.foo.org][another site]] asdf asdf asdf
    """
    return_ = list()

    while '[[' in line:
        _, rest = line.split('[[', 1)
        try:
            url_desc, rest = rest.split(']]', 1)
        except ValueError as err:
            print(path_)
            print(f"{lineno:,d}")
            print(line)
            print(err)
            return None

        if '][' in url_desc:
            # [[https:/www.google.com][Google search]]
            try:
                url, desc = url_desc.split('][')
            except ValueError as err:
                print(path_)
                print(f"{lineno:,d}")
                print(line)
                print(err)
                return None
        else:
            # [[https:/www.google.com]]
            url, desc = url_desc, None
        return_.append((url, desc))
        try:
            _, line = line.split(']]', 1)  # Any more on the line?
        except ValueError as err:
            print(path_)
            print(f"{lineno:,d}")
            print(line)
            print(err)
            return None

    return return_
