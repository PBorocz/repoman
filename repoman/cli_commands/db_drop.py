from rich.console import Console
from rich.prompt import Confirm

import db_physical as dbp


def command(console: Console, verbose: bool) -> None:
    """
    command: __name__
    description: Delete the database.
    """
    if Confirm.ask("Are you sure?", default=False):
        dbp.drop()
        console.print(f"Database [bold]dropped[/bold].")
