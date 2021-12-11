# Constants
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

# State management of cli command history.
DEFAULTS = {
    "index" : {
        "dir"    : str(Path.home()),
        "suffix" : "txt",
        "force"  : "No",
    },

}
