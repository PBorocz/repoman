#!/usr/bin/env py
import db
from rich.console import Console
from rich.table import Table

PROMPT = 'Query string? (ctrl-D or exit<cr> to exit) -> '

# Primary UI for repoman queries.
def query_ui(string):
    console = Console()
    console.clear()
    print(PROMPT, end='')
    while True:
        try:
            string = input()
        except (KeyboardInterrupt, EOFError):
            break
        
        if string.lower() == "exit":
            break
        
        if string:
            results = db.query_db(string)
            if results:
                console.clear()
                display_query_results(console, results)
            else:
                print(f"Sorry, nothing matched: '{string}'\n")
                
        print(PROMPT, end='')


def display_query_results(console, results: list) -> None:

    table = Table(show_header=True, header_style="bold")
    table.add_column("Snippet")
    table.add_column("Path")
    table.add_column("LastMod")
    table.add_column("Rank")
    for row in results:
        table.add_row(row.snippet, row.path, row.last_mod, row.rank)
    console.print(table)
