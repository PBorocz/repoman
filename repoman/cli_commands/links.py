from rich.console import Console
from rich.table import Table

import constants as c
import db_operations as dbo


def command(console: Console, verbose: bool) -> None:
    """
    command: __name__
    description: Display a summary of links encountered.
    """
    link_counts = dbo.link_summary()
    if not link_counts:
        console.print("[bold italic]No[/bold italic] links have been encountered yet.")
    else:
        table = Table(show_header=True, show_footer=True, box=c.DEFAULT_BOX_STYLE)
        table.add_column("Tag")
        table.add_column("Documents")
        for obj in link_counts:
            table.add_row(obj.url, f"{obj.count:,d}")
        console.print(table)
