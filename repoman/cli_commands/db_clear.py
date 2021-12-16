from rich.console import Console
from rich.prompt import Confirm

import db_logical as dbl


def command(console: Console, verbose: bool) -> None:
    """
    command: __name__
    description: Delete all data in all tables (requires confirmation).
    """
    if Confirm.ask("Are you sure?", default=False):
        dbl.clear()
        console.print(f"Database [bold]cleared[/bold].")
