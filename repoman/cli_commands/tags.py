from rich.console import Console
from rich.table import Table

import db_operations as dbo


def command(console: Console, verbose: bool) -> None:
    """
    command: __name__
    description: Display a summary of tags encountered.
    """
    tag_counts = dbo.tag_count()
    if not tag_counts:
        console.print("[bold italic]No[/bold italic] tags have been encountered yet.")
    else:
        table = Table(show_header=True, show_footer=True, box=c.DEFAULT_BOX_STYLE)
        table.add_column("Tag")
        table.add_column("Documents")
        for dt_ in tag_counts:
            table.add_row(dt_.tag, f"{dt_.COUNT:,d}")
        console.print(table)
