from collections import defaultdict
from pathlib import Path
from typing import Tuple

import sqlite3
from rich import print

SKIP_DIRS = (
    ".git",
    ".venv",
    "venv",
    "_vm",
    ".vm",
    "__pycache__",
    "node_modules",
)


def emitter(path):
    def _walk(path):
        for p in Path(path).iterdir():
            if p.is_dir() and p.name not in SKIP_DIRS:
                yield from _walk(p)
                continue
            yield p.resolve()
    for path_ in _walk(path):
        yield path_


class Index:
    def __init__(self, conn):
        self.conn = conn

    def _iter_files_to_index(self, dir: str, arg_suffix: str) -> Tuple[Path, str]:
        """Iterator over files to index from dir and maybe matching suffix provided"""
        p_dir = Path(dir).expanduser().resolve()
        for path_ in emitter(p_dir):
            suffix = path_.suffix if path_.suffix else "-NONE-"
            if arg_suffix and suffix:
                if arg_suffix.lower() == suffix.lower():
                    yield path_, suffix.lower()[1:]
            else:
                # ie. either we don't need to filter by suffix OR file has no suffix.
                yield path_, None

    def index(self, arg_dir: str, arg_suffix: str) -> int:

        type_counts = defaultdict(int)

        for path_, suffix in self._iter_files_to_index(arg_dir, arg_suffix):
            type_counts[suffix] += 1
            self._index(path_, suffix)

        return sum(list(type_counts.values()))


    def _index(self, path_, suffix):
        if suffix == "txt":
            return self.index_txt(path_)
        elif suffix == "org":
            return self.index_org(path_)
        else:
            return None

    def index_txt(self, path_, suffix="txt"):
        """Insert an index for a text file"""
        csr = self.conn.cursor()

        body = ''.join(path_.read_text())
        insert = f"""
           INSERT INTO docs(path, suffix, body) VALUES(
           '{path_}',
           '{suffix}',
           '{body}'
        )
        """
        csr.execute(insert)
        self.conn.commit()

    def index_org(self, path_, suffix="org"):
        """Insert an index for an org file"""
        csr = self.conn.cursor()
        body = ''.join(path_.read_text()).replace("'", '"')

        # statement = "INSERT INTO {0} ({1}) VALUES ({2},{3},{4});".format(table,columns,1,2,3)

        stmt = """INSERT INTO docs(path, suffix, body) VALUES('{0}','{1}','{2}')""".format(path_, suffix, body)
        try:
            csr.execute(stmt)
        except sqlite3.OperationalError:
            breakpoint()

        self.conn.commit()
