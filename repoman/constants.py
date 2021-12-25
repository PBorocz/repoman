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

# Indexing constants:
SKIP_DIRS = (
    # Directories to SKIP indexing on..
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
    # File extensions TO index, rest are skipped.
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

# CLI UI support constants:
def _italic(str_):
    return f"[italic]{str_}[/italic]"

INTRODUCTION = f"""Welcome to RepoMan!
{_italic('Ctrl-D')} or {_italic('.exit')}/{_italic('.quit')} to exit, {_italic('.help')} for help."""

PROMPT = 'repoman> '
