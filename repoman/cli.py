#!/usr/bin/env py
import re
import sys
import inspect
from pathlib import Path
from textwrap import dedent

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

import constants as c
from cli_state import get_state, save_state
from index import index
from utils import get_user_history_path, AnonymousObj


def italic(str_):
    return f"[italic]{str_}[/italic]"

INTRODUCTION = f"""Welcome to RepoMan!
{italic('Ctrl-D')}, {italic('.exit')} or {italic('.q')} to exit.
{italic('.help')} for help.
"""
PROMPT = 'repoman> '

# c.DEFAULT_BOX_STYLE = box.SIMPLE_HEAVY


# Primary CLI/UI for repoman
@click.command()
@click.option('--verbose/--no-verbose', default=False, help='Verbose mode?')
def cli(verbose: bool) -> None:
    
    console = Console()
    console.clear()
    
    print(Figlet(font='standard').renderText('Repo-Man'))
    console.print(INTRODUCTION)

    # Create prompt session to allow commands over sessions.
    session = PromptSession(history=FileHistory(get_user_history_path()))
    
    while True:
        try:
            response = session.prompt(PROMPT)
        except (KeyboardInterrupt, EOFError):
            break

        if response.lower() in (".exit", ".q"):  # Done?
            break

        if response:            # As Ahnold would say...DOO EET!
            execute(verbose, response)

            
def execute(verbose: bool, response: str) -> bool:
    """Execute the "response" provided, determining whether or not
    it's a "command" or "simply" a query to be executed."""
    console = Console()
    conn = db.get_db_conn()
    # Look for command before assuming a query...
    if response.startswith('.'):
        #####################################################
        # It's a *command*...
        #####################################################
        s_method = response[1:]
        try:
            method = globals()[f"command_{s_method}"]
        except KeyError:
            console.print(f"Sorry, [red bold]{response}[/red bold] is not a known command (.help to list them)")
            return False
        console.clear()
        return method(console, conn, verbose)
    else:
        #####################################################
        # Otherwise, we assume "response" represents a query!
        #####################################################
        query(console, conn, response)

        
def query(console: Console, conn: Connection, query_string: str) -> None:
    """Execute a query against the doc store"""
    
    def _display_query_results(console, results: list) -> None:
        table = Table(show_header=True, header_style="bold", box=c.DEFAULT_BOX_STYLE)
        table.add_column("Path")
        table.add_column("LastMod")
        table.add_column("Snippet")
        for row in results:
            table.add_row(
                row.path,
                row.last_mod.split(' ')[0],  # Don't need time..
                row.snippet,
            )
        console.print(table)
    
    results = db.query(conn, query_string)
    if results:
        console.clear()
        _display_query_results(console, results)
    else:
        console.print(f"Sorry, nothing matched: [italic]'{query_string}'[/italic]\n")

################################################################################
# Management commands 
################################################################################
def command_status(console: Console, conn: Connection, verbose: bool) -> None:
    """Display the overall status of the database"""
    status = db.status(conn)
    if not status:
        return
    
    if not status.total_docs:
        console.print("[bold italic]No[/bold italic] documents have been indexed yet, database is empty.")
        return

    # Documents...
    table = Table(show_header=True, show_footer=True, box=c.DEFAULT_BOX_STYLE)
    table.add_column("Suffix", footer=Text("Total"))
    table.add_column("Documents", footer=Text(f"{status.total_docs:,d}"), justify="right")
    for (suffix, count) in status.suffix_counts:
        table.add_row(suffix, f"{count:,d}")
    console.print(table)
    
    if status.total_tags:
        console.print(f"Total tags  : [bold]{status.total_tags:,d}[/bold]")

    # Links...
    if status.total_links:
        console.print(f"Total links : [bold]{status.total_links:,d}[/bold]")

        
def command_tags(console: Console, conn: Connection, verbose: bool) -> None:
    """Display a summary of tags encountered."""
    tag_counts = db.tag_count(conn)
    if not tag_counts:
        console.print("[bold italic]No[/bold italic] tags have been encountered yet.")
    else:
        table = Table(show_header=True, show_footer=True, box=box.SIMPLE_HEAVY)
        table.add_column("Tag")
        table.add_column("Documents")
        for (suffix, count) in tag_counts:
            table.add_row(suffix, f"{count:,d}")
        console.print(table)

        
def command_index(console: Console, conn: Connection, verbose: bool) -> bool:
    """Index a set of files (by root directory and/or suffix)"""

    def sub_prompt(prompt_: str, default_: str) -> str:
        return prompt(f"{prompt_:11s} : ", default=default_)

    # Get the values we last used for this command..
    index_command = get_state("index")

    ############################################################
    # Using these as defaults, prompt for any updated values
    ############################################################
    # Root directory to index from..
    index_command.root = sub_prompt('Root',index_command.root)
    if not Path(index_command.root).expanduser().exists():
        print(f"Sorry, {index_command.root} does not exist")
        return False  # Don't pass go and definitely, don't update state!
    
    # What file suffix to index (if any)
    index_command.suffix = sub_prompt('Suffix', index_command.suffix)

    # Should we overwrite existing entries?
    index_command.force  = sub_prompt('Force [y/n]', index_command.force)

    # Save away these values for the next time we run this command.
    save_state("index", index_command)

    ############################################################
    # DO IT!
    ############################################################
    num_indexed, time_taken = index(True, index_command)
    if num_indexed:
        docs_per_sec = num_indexed / time_taken
        table = Table(show_header=False, box=c.DEFAULT_BOX_STYLE)
        table.add_column("-")
        table.add_column("-", justify="right")
        table.add_row(f"Total Documents"  , f"[bold]{num_indexed:,d}[/bold]")
        table.add_row(f"Total Time (sec)" , f"[bold]{time_taken:.4f}[/bold]")
        table.add_row(f"Documents per sec", f"[bold]{docs_per_sec:.4f}[/bold]")
        console.print(table)
    else:
        console.print("[bold]No[/bold] files indexed.")
        
    return True


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

    
def command_help(console: Console, con: Connection, verbose: bool):
    """Display the list of all RepoMan commands available."""
    # Display the list of all commands available by finding all the methods in this module that start with "command_" and using their doc-strings as the "help" text for their operation.
    def check_func(name, func):
        """Confirm that the function sent in represents a RepoMan command"""
        return inspect.isfunction(func) and name.startswith('command_') and func.__module__ == __name__

    commands = []
    for name, func in inspect.getmembers(sys.modules[__name__]):
        if not check_func(name, func):
            continue
        name = func.__name__.replace("command_",".")
        docs = dedent(func.__doc__)
        docs = docs.replace("\n", "")
        docs = re.sub('\s+',' ', docs)
        commands.append((name, docs))
    

    console.print("- All entries that don't start with '.' are consider queries.")
    console.print("- Entries start with '.' are RepoMan commands:")
    table = Table(box=c.DEFAULT_BOX_STYLE)
    table.add_column("Command")
    table.add_column("Description")
    for command in sorted(commands):
        table.add_row(*command)
    console.print(table)


if __name__ == "__main__":
    cli()
