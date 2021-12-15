from rich.console import Console
from rich.table import Table
from rich.text import Text

import constants as c
import db_operations as dbo

def command(console: Console, verbose: bool) -> None:
    """
    command: __name__
    description: Display the overall status of the database.
    """
    status = dbo.status()
    if not status:
        return

    if not status.total_docs:
        console.print("[bold italic]No[/bold italic] documents have been indexed yet, database is empty.")
        return

    # Documents...
    table = Table(show_header=True, show_footer=True, box=c.DEFAULT_BOX_STYLE)
    table.add_column("Suffix", footer=Text("Total"))
    table.add_column("Documents", footer=Text(f"{status.total_docs:,d}"), justify="right")
    for (suffix, count) in status.suffix_counts.items():
        table.add_row(suffix, f"{count:,d}")
    console.print(table)

    if status.total_tags:
        console.print(f"Total tags  : [bold]{status.total_tags:,d}[/bold]")

    # Links...
    if status.total_links:
        console.print(f"Total links : [bold]{status.total_links:,d}[/bold]")
