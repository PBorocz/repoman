# Constants
from pathlib import Path

CONFIG_PATH  = Path.home() / Path(".config")
REPOMAN_PATH = CONFIG_PATH / Path("repoman")
REPOMAN_DB   = "repoman.db"
DB_FILE      = str(REPOMAN_PATH / Path(REPOMAN_DB))
