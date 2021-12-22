# All Abstract Data Types used in RepoMan
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class SortOrderChoices(Enum):
    # <display value> = <query result anonymous obj attribute>
    lastmod = "last_mod"
    name    = "name"
    path    = "path_full"
    rank    = "rank"
    suffix  = "suffix"

SORT_ORDER_CHOICES = ', '.join([enum for enum in SortOrderChoices.__members__])


@dataclass
class QueryCommandParameters:
    """Keep track of query parameters"""
    query_string : str
    top_n        : int = None
    suffix       : str = ""
    sort_order   : str = "lastmod"


@dataclass
class IndexCommandParameters:
    """Keep track of index command parameters"""
    root   : str(Path.home())
    suffix : str
    force  : str = "No"


@dataclass
class QueryResult:
    """Composite information on each document returned from a query"""
    doc_id    : int
    rank      : str
    name      : str
    path_full : str
    path_rel  : str
    suffix    : str
    last_mod  : str
    snippet   : str
