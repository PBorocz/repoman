#!/usr/bin/env py
import sys
import inspect
from pathlib import Path

import click
from prompt_toolkit import PromptSession, prompt
from prompt_toolkit.history import FileHistory
from pyfiglet import Figlet
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

import db
from sqlite3 import Connection

from index import Index
from utils import get_user_history_path

INTRODUCTION = "Welcome to RepoMan! ctrl-D, '.exit/.q' to exit, .help for help."
PROMPT = 'repoman> '

# Primary CLI/UI for repoman
@click.command()
@click.option('--verbose/--no-verbose', default=False, help='Verbose mode?')
def cli(verbose: bool) -> None:
    
    console = Console()
    console.clear()
    con = db.get_db_conn()
    
    print(Figlet(font='standard').renderText('Repo-Man'))
    print(INTRODUCTION)

    # Create prompt session to allow commands over sessions.
    session = PromptSession(history=FileHistory(get_user_history_path()))
    
    while True:
        try:
            response = session.prompt(PROMPT)
        except (KeyboardInterrupt, EOFError):
            break

        if response.lower() in (".exit", ".q"):
            break

        # As Ahnold would say...DOO EET!
        if response:
            execute(verbose, con, response)

            
def execute(verbose: bool, con: Connection, response: str) -> bool:
    """Execute the "response" provided, determining whether or not
    it's a "command" or "simply" a query to be executed."""
    # Look for command before assuming a query...

    console = Console()
    if response.startswith('.'):
        # It's a *command*...
        s_method = response[1:]
        try:
            method = globals()[f"command_{s_method}"]
        except KeyError:
            console.print(f"Sorry, [red bold]{response}[/red bold] is not a known command (.help to list them)")
            return False
        return method(console, con, verbose)
    else:
        # Otherwise, we assume "response" represents a query!
        query(console, con, response)

        
def query(console: Console, con: Connection, query_string: str) -> None:
    """Execute a query against the doc store"""
    
    def _display_query_results(console, results: list) -> None:
        table = Table(show_header=True, header_style="bold")
        table.add_column("Snippet")
        table.add_column("Path")
        table.add_column("LastMod")
        for row in results:
            table.add_row(
                row.snippet,
                row.path,
                row.last_mod.split(' ')[0],  # Don't need time..
            )
        console.print(table)
    
    results = db.query(con, query_string)
    if results:
        console.clear()
        _display_query_results(console, results)
    else:
        console.print(f"Sorry, nothing matched: [italic]'{query_string}'[/italic]\n")

################################################################################
# Management commands 
################################################################################
def command_createdb(console: Console, con: Connection, verbose: bool) -> None:
    """Create the schema in an existing database (requires confirmation)"""
    db.create(con)
    console.print(f"Database [bold]created[/bold].")

    
def command_dropdb(console: Console, con: Connection, verbose: bool) -> None:
    """Delete the database (requires confirmation)"""
    db.drop(con)
    console.print(f"Database [bold]dropped[/bold].")

    
def command_cleardb(console: Console, con: Connection, verbose: bool) -> None:
    """Clean out the database of all data (requires confirmation)"""
    db.clear(con)
    console.print(f"Database [bold]cleared[/bold].")

    
def command_status(console: Console, con: Connection, verbose: bool) -> None:
    """Display the status of the database"""
    status = db.status(con)
    
    if not status.total_docs:
        console.print("[bold italic]No[/bold italic] documents have been indexed yet, database is empty.")
        return

    # Documents...
    table = Table(show_header=True, show_footer=True, box=box.SIMPLE_HEAVY)
    table.add_column("Suffix", footer=Text("Total"))
    table.add_column("Documents", footer=Text(f"{status.total_docs:,d}"), justify="right")
    for (suffix, count) in status.suffix_counts:
        table.add_row(suffix, f"{count:,d}")
    console.print(table)

    # Tags...
    if status.total_docs:
        table = Table(show_header=True, show_footer=True, box=box.SIMPLE_HEAVY)
        table.add_column("Tag", footer=Text("Total"))
        table.add_column("Documents", footer=Text(f"{status.total_tags:,d}"), justify="right")
        for (suffix, count) in status.tag_counts:
            table.add_row(suffix, f"{count:,d}")
        console.print(table)

    # Links...
    if status.total_links:
        console.print(f"Total links extracted from org files: [bold]{status.total_links:,d}[/bold]")

        
def command_index(console: Console, con: Connection, verbose: bool) -> bool:
    """Index a set of files (by root directory and/or suffix)"""
    indexer = Index(con)

    dir = prompt(f'Root directory? > ', default="~/Repository")
    if not Path(dir).expanduser().exists():
        print(f"Sorry, {dir} does not exist")
        return False
        
    suffix = prompt(f'Suffix? > ', default="txt")
    
    s_force = prompt(f'Force? > ', default="False")
    if s_force:
        b_force = False if s_force.lower().startswith('fa') else True
    else:
        b_force = default

    num_indexed = indexer.index(True, dir, suffix, b_force)
    if num_indexed:
        console.print(f"Successfully indexed [bold]{num_indexed:,d}[/bold] file(s).")
    else:
        console.print("[bold]No[/bold] files indexed.")
    return True
        
def command_help(console: Console, con: Connection, verbose: bool):
    """Display the list of all commands available"""

    command_funcs = [obj for name,obj in inspect.getmembers(sys.modules[__name__]) 
                     if (inspect.isfunction(obj) and 
                         name.startswith('command_') and
                         obj.__module__ == __name__)]

    commands = [(func.__name__.replace("command_","."), func.__doc__) for func in command_funcs]
    
    table = Table(show_header=False)
    table.add_column("Command")
    table.add_column("Explanation")
    for command in sorted(commands):
        table.add_row(*command)
    console.print(table)


if __name__ == "__main__":
    cli()
