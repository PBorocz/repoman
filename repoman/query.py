#!/usr/bin/env py
import db
from rich.console import Console
from rich.table import Table

FIRST_PROMPT = "Query string? (ctrl-D, '.exit' or '.q' to exit) -> "
SUBSEQUENT_PROMPT = 'repoman> '

# Primary UI for repoman queries.
def query_ui(string):
    console = Console()
    console.clear()
    print(FIRST_PROMPT, end='')
    while True:
        try:
            string = input()
        except (KeyboardInterrupt, EOFError):
            break

        if string.lower() in (".exit", ".q"):
            break

        if string:
            results = db.query_db(string)
            if results:
                console.clear()
                display_query_results(console, results)
            else:
                print(f"Sorry, nothing matched: '{string}'\n")

        print(SUBSEQUENT_PROMPT, end='')
