# Constants
from enum import Enum
from pathlib import Path
from rich import box

DEFAULT_BOX_STYLE = box.SIMPLE_HEAVY

REPOMAN_DB = "repoman.db"
STATE_ROOT = ".cli_state"

REPOMAN_PATH = Path("~/.config").expanduser() / Path("repoman")
if not REPOMAN_PATH.exists():
    REPOMAN_PATH.mkdir(parents=True)

DB_PATH = REPOMAN_PATH / Path(REPOMAN_DB)
DB_DATETIME_FORMAT = '%Y-%m-%d %H:%M'

################################################################################
# State management of cli command history.
################################################################################
class SortOrderChoices(Enum):
    # <display value> = <query result anonymous obj attribute>
    lastmod = "last_mod"
    name    = "name"
    path    = "path_full"
    rank    = "rank"
    suffix  = "suffix"

SORT_ORDER_CHOICES = ', '.join([enum for enum in SortOrderChoices.__members__])

DEFAULTS = {
    "index" : {
        "dir"    : str(Path.home()),
        "suffix" : "txt",
        "force"  : "No",
    },
    "query" : {
        "query_string" : "",
        "sort_order"   : "lastmod", # See above Enum for valid options
        "top_n"        : "",
        "suffix"       : "",
    },

}

# CLI UI support constants
def _italic(str_):
    return f"[italic]{str_}[/italic]"

INTRODUCTION = f"""Welcome to RepoMan!
{_italic('Ctrl-D')}, {_italic('.exit')}/{_italic('.quit')} to exit.
{_italic('.help')} for help.
"""

PROMPT = 'repoman> '
