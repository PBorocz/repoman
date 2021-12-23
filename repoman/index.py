import chardet
import datetime
import multiprocessing
import os
import re
import sys
import time
from collections import defaultdict
from contextlib import suppress
from copy import copy
from functools import partial
from pathlib import Path
from typing import Iterable, Optional

from pdfminer.high_level import extract_text
from pdfminer.pdftypes import PDFException

from rich import print
from rich.progress import track

import constants as c
import db_logical as dbl
import db_physical as dbp
import db_operations as dbo
from utils import AnonymousObj, progressIndicator, timer
from adts import IndexCommandParameters

SKIP_DIRS = (
    ".git",
    ".hg",
    ".venv",
    "venv",
    "_vm",
    ".vm",
    "__pycache__",
    "node_modules",
    "zzArchive",
)

INCLUDE_EXTENSIONS = (
    ".md",
    ".txt",
    ".org",
    ".gif",
    ".pdf",
    ".mp4", ".mov",
    ".jpg", ".jepg", ".jpg_large",
    ".png",
    ".py",
)

FILENAME_TAG_SEPARATOR = " -- "
FILE_WITH_TAGS_REGEX   = re.compile(r'(.+?)' + FILENAME_TAG_SEPARATOR + r'(.+?)(\.(\w+))??$')
YYYY_MM_DD_PATTERN     = re.compile(r'^(\d{4,4})-([01]\d)-([0123]\d)[- _T]')


################################################################################
# CORE METHOD: Get an iterator of files to be indexed and return the number that worked.
################################################################################
def index(index_parms: IndexCommandParameters) -> tuple[int, float]:

    b_force   = False if not index_parms.force   or not index_parms.force.lower().startswith('y') else True
    b_verbose = False if not index_parms.verbose or not index_parms.verbose.lower().startswith('y') else True

    paths_to_index = _paths_to_index(
        Path(index_parms.root).expanduser().resolve(),
        index_parms.suffix,
        b_force)

    # Set up our processing pool based on:
    # 1 - the number of documents to index and
    # 2 - one less than the number of cores we think are available.
    pool_size = (multiprocessing.cpu_count() * 2) - 1 if len(paths_to_index) > 100 else 1
    pool = multiprocessing.Pool(processes=pool_size)

    # ..Let 'em loose!
    start = time.time()
    pool_outputs = pool.map(partial(_index, b_verbose), paths_to_index)

    # ..and wait for all to finish.
    pool.close()
    pool.join()

    num_cleansed = cleanup()

    # Return the number of file paths we indexed *and* how long it took!
    return len(pool_outputs), num_cleansed, time.time() - start


def _paths_to_index(
        path_dir: Path,
        arg_suffix: Optional[str],
        arg_force: bool) -> Iterable[Path]:
    """Encapsulate all the logic regarding what files to be indexed,
    taking into account:
    - If we're filtering by suffix (here specified *without* the leading '.', e.g. pdf, org, text ...)
    - If we're supposed to "reindex" files already indexed.
    - If the file hasn't been modified from the last time we indexed it.
    - Directories to skip outright.
    """
    return_ = list()

    # Before we start, what files have already been index?
    with dbp.database.connection_context() as ctx:
        paths_already_indexed = dbo.get_paths_already_indexed()

    for path_ in walker(path_dir, arg_suffix, SKIP_DIRS, INCLUDE_EXTENSIONS):
        if arg_force:
            return_.append(path_)  # Easy, index every file that passes our file "filters"!
            continue

        # Have we not indexed this file before?
        is_path_not_indexed = path_ not in paths_already_indexed

        # Has file has been modified more recently than our stored indexed version?
        lmod_doc = paths_already_indexed.get(path_, None)
        has_doc_been_updated = get_last_mod(path_) > lmod_doc if lmod_doc else None

        # Decide to take/not take the path under consideration
        if is_path_not_indexed:
            return_.append(path_)  # Easy case, we haven't seen it yet!
            continue
        else:
            # We've already seen it...but...
            # has it been updated since we last indexed it?
            if has_doc_been_updated:
                return_.append(path_)

    return return_

def _index(verbose: bool, path_: Path) -> Optional[int]:
    """Index the file on the specified path on a thread-safe basis"""

    if verbose:
        print(f"[{os.getpid():6d}] {path_}...")
        sys.stdout.flush()

    so_doc = AnonymousObj(
        path_  = path_,
        suffix = path_.suffix.lower()[1:],
        body   = '',                  # Assume empty until we can pull anything out...
        links  = [],                  # "
        lmod   = get_last_mod(path_), # When was the file tagged or last modified?
        tags   = get_tags(path_),     # What are any file-specific (ie. Novoid) tags?
        now    = datetime.datetime.now().strftime(c.DB_DATETIME_FORMAT)
    )

    # For specific extensions that we *can* get meaningful text from,
    # find the specific method to do so and get it!
    get_text_method = dict(
        txt = get_text_from_txt,
        py  = get_text_from_txt, # For now, treat Python as simple text..
        md  = get_text_from_txt, # For now, treat Markdown as simple text..
        org = get_text_from_org,
        pdf = get_text_from_pdf,
    ).get(so_doc.suffix)

    if not get_text_method:
        return None

    # Get the text of the document (along with any links encountered)...
    so_doc.body, so_doc.links = get_text_method(path_)

    # ..and update/insert a new document entry into our database.
    # (get a database connection *here* as we might be in
    #  a separate thread from the main index method)
    with dbp.database.connection_context() as ctx:
        return dbo.upsert_doc(verbose, so_doc)


def get_text_from_txt(path_, suffix="txt") -> tuple[str, Optional[list[str]]]:
    """Get text from a txt file"""
    with open(path_, encoding=get_file_encoding(path_)) as fh_:
        return fh_.read(), None


def get_text_from_org(path_, suffix="org") -> tuple[str, Optional[list[str]]]:
    """Get text and links from an org file"""

    def iter_lines(path_):
        """Open the file on 'path_' with the right encoding and yield lines.."""
        encoding = get_file_encoding(path_)
        with open(path_, encoding=encoding, errors='ignore') as fh_:
            for line in fh_:
                yield line

    def filter_source_blocks(lines: Iterable) -> Iterable:
        """Use a mini-state machine to filter out org source code blocks.."""
        in_block = False
        for line in lines:
            lsl = line.strip().lower()
            if lsl.startswith("#+begin_src"):
                in_block = True
                continue
            if lsl.startswith("#+end_src"):
                in_block = False
                continue
            if not in_block:
                yield line

    text_in_file  = []
    links_in_file = []

    for line in filter_source_blocks(iter_lines(path_)):
        clean = line.strip()
        if clean:
            text_in_file.append(clean)
            if line_links := get_org_links(path_, clean):
                for link in line_links:
                    links_in_file.append(link)

    return ' '.join(text_in_file), links_in_file


def get_text_from_pdf(path_, suffix="pdf") -> tuple[str, Optional[list[str]]]:
    """Get text from a pdf file"""
    try:
        return extract_text(path_), None
    except Exception as err:
        print(f"\nSorry, {path_} may be a invalid PDF ({err})")
        return None, None


################################################################################
# Utility methods
################################################################################
def get_file_encoding(path_: Path) -> str:
    """Detect the most appropriate character encoding to use to open the file"""
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
        return f"{yyyy}-{int(mm):02d}-{int(dd):02d} 00:00"

    f_mtime = path_.stat().st_mtime
    return datetime.datetime.fromtimestamp(f_mtime).strftime(c.DB_DATETIME_FORMAT)


def get_tags(path_: Path) -> list[str]:
    """Utility method to return any tags embedded in the file path if the filepath conforms to
    the 'Novoid' filetagging syntax, we can extract "tags".

    RegEx's cribbed from https://github.com/novoid/filetags/blob/master/filetags/__init__.py
    """
    components = re.match(FILE_WITH_TAGS_REGEX, path_.stem)
    if components:
        return [tag for tag in components.group(2).split(' ') if tag]
    return []


def get_org_links(path_: Path, line: str) -> list[str]:
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
            return None

        if '][' in url_desc:
            # [[https:/www.google.com][Google search]]
            try:
                url, desc = url_desc.split('][')
            except ValueError as err:
                return None
        else:
            # [[https:/www.google.com]]
            url, desc = url_desc, None

        return_.append((url, desc))

        with suppress(ValueError):
            _, line = line.split(']]', 1)  # Any more on the line?

    return return_

def cleanup() -> int:
    """Go through all the documents currently stored and make sure that all of their
    respective files still exist on disk, if not, delete the respective document."""
    return_ = 0
    with dbp.database.connection_context() as ctx:
        for doc in dbl.Document.select():
            if not Path(doc.path).exists():
                doc.delete_instance(recursive=True)
                return_ += 1
    return return_
