from rich.console import Console
from rich.markup import escape
from rich.table import Table

import constants as c
import db_operations as dbo

LAST_QUERY_RESULT = None

def command(console: Console, query_string: str) -> None:
    """
    command: .q/.query
    description: Execute a text-search query with all options available."""
    global LAST_QUERY_RESULT

    results = dbo.query(query_string)
    LAST_QUERY_RESULT = results # Store away for subsequent use!

    if results:
        console.clear()
        _display_query_results(console, results)
    else:
        console.print(f"Sorry, nothing matched: [italic]'{query_string}'[/italic]\n")


def markup_snippet(snippet):
    """On our queries, we can't use the Rich markup to delineate matching text,
    here, we "undo" that and convert to that which'll be displayed to the user.
    """
    snippet = escape(snippet)
    snippet = snippet.replace(">>>", "[green bold]")
    snippet = snippet.replace("<<<", "[/]")
    return snippet


def _display_query_results(console, results: list) -> None:
    table = Table(show_header=True, header_style="bold", box=c.DEFAULT_BOX_STYLE)
    table.add_column("#")
    table.add_column("File")
    table.add_column("Snippet")
    table.add_column("LastMod")
    for ith, obj in enumerate(results, 1):
        table.add_row(
            f"{ith:,d}",
            obj.path_full.name,
            markup_snippet(obj.snippet),
            obj.last_mod.split(' ')[0],  # Don't need time..
        )
    console.print(table)
