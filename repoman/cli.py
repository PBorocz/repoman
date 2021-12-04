#!/usr/bin/env py
import sys
import inspect

import click
from prompt_toolkit import PromptSession, prompt
from prompt_toolkit.history import FileHistory
from pyfiglet import Figlet
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

import db
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
            execute(verbose, response)

            
def execute(verbose: bool, response: str) -> bool:
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
        return method(console, verbose)
    else:
        # Otherwise, we assume "response" represents a query!
        query(console, response)

        
def query(console, query_string: str) -> None:
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
    
    results = db.query(query_string)
    if results:
        console.clear()
        _display_query_results(console, results)
    else:
        console.print(f"Sorry, nothing matched: [italic]'{query_string}'[/italic]\n")

################################################################################
# Management commands 
################################################################################
def command_createdb(console, verbose: bool) -> None:
    """Create the schema in an existing database (requires confirmation)"""
    db.create()
    console.print(f"Database [bold]created[/bold].")

    
def command_dropdb(console, verbose: bool) -> None:
    """Delete the database (requires confirmation)"""
    db.drop()
    console.print(f"Database [bold]dropped[/bold].")

    
def command_cleardb(console, verbose: bool) -> None:
    """Clean out the database of all data (requires confirmation)"""
    db.clear()
    console.print(f"Database [bold]cleared[/bold].")

    
def command_status(console, verbose: bool) -> None:
    """Display the status of the database"""
    status = db.status()
    
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

        
def command_index(console, verbose: bool) -> None:
    """Index a set of files (by root directory and/or suffix)"""
    indexer = Index(db.get_db_conn())

    dir = prompt(f'Root directory? > ', default="~/Repository/3.Resources")
    
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
            
def command_help(console, verbose: bool):
    """Display the list of all commands available"""

    command_funcs = [obj for name,obj in inspect.getmembers(sys.modules[__name__]) 
                     if (inspect.isfunction(obj) and 
                         name.startswith('command_') and
                         obj.__module__ == __name__)]

    commands = [(func.__name__.replace("command_","."), func.__doc__) for func in command_funcs]
    
    table = Table(show_header=False)
    table.add_column("Command")
    table.add_column("Explanation")

    # FIXME: Make this list dynamic by going through globals and finding all functions that
    # match 'command_*' and using their docstring as the help.
    for command in sorted(commands):
        table.add_row(*command)
    # table.add_row(".help", "Display the list of all commands available")
    # table.add_row(".index", "Index a set of files (by directory and/or suffix)")
    # table.add_row(".createdb", "Empty the database and recreate it's schema from scratch (requires confirmation)")
    # table.add_row(".dropdb", "Delete the database (requires confirmation)")
    console.print(table)


if __name__ == "__main__":
    cli()
