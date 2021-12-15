from rich.console import Console
from rich.prompt import Confirm

import db_logical as dbl


def command(console: Console, verbose: bool) -> None:
    """
    command: __name__
    description: Create the schema in an existing database.
    """
    if Confirm.ask("Are you sure?", default=False):
        dbl.create_schema()
        console.print(f"Database [bold]created[/bold].")
