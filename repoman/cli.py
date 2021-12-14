#!/usr/bin/env py
import os
import re
import sys
import inspect
from pathlib import Path
from textwrap import dedent

import click
from prompt_toolkit import PromptSession, prompt
from prompt_toolkit.completion import PathCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.validation import Validator, ValidationError
from pyfiglet import Figlet
from rich import box
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table
from rich.text import Text
from rich.markup import escape

import constants as c
import db_operations as dbo
import db_logical as dbl
import db_physical as dbp
from cli_state import get_state, save_state
from index import index
from utils import get_user_history_path

LAST_QUERY_RESULT = None

################################################################################
# Primary CLI/UI for RepoMan!
################################################################################
@click.command()
@click.option('--verbose/--no-verbose', default=False, help='Verbose mode?')
def cli(verbose: bool) -> None:

    # Setup and introduction...
    console = Console()
    console.clear()
    print(Figlet(font='standard').renderText('Repo-Man'))
    console.print(c.INTRODUCTION)

    # Prompt session allow commands over sessions (!)
    session = PromptSession(history=FileHistory(get_user_history_path()))

    while True:
        try:
            response = session.prompt(c.PROMPT)
        except (KeyboardInterrupt, EOFError):
            break

        if response.lower() in (".exit", ".q"):  # Done?
            break

        if response:  # As Ahhhnold would say...DOO EET!
            with dbp.database.connection_context() as ctx:
                execute(verbose, response)


def execute(verbose: bool, response: str) -> bool:
    """Execute the "response" provided, determining whether or not
    it's a "command" or "simply" a query to be executed."""
    console = Console()      # Every command is going to put out to the console.

    # Look for a "command" before assuming a query...


    if response.startswith('.'):
        #############################
        # A *RepoMan* command
        #############################
        s_response = response[1:]

        command_method = globals().get(f"command_{s_response}", None)
        if command_method:
            # Good to go, clear our console and run the command
            console.clear()
            return command_method(console, verbose)
        else:
            console.print(f"Sorry, [red bold]{response}[/red bold] is not a known command (.help to list them)")
    elif response.startswith("!"):
        #############################
        # A Document command
        #############################
        try:
            selected_file = int(response[1:])
        except ValueError:
            console.print(f"Sorry, [red bold]{response}[/red bold] isn't a valid document open command,")
            console.print(f"Must be valid integer within the range of 1 -> {len(LAST_QUERY_RESULT)}.")
            return False

        # Lookup and open the "selected_fileth" file in the last query!
        try:
            path_full = LAST_QUERY_RESULT[selected_file-1].path_full
        except IndexError:
            console.print(f"Sorry, [red bold]{response}[/red bold] must be between 1 and {len(LAST_QUERY_RESULT)}.")
            return False

        # Open the file based on the local system's file associations
        home_dir = os.system(f'open "{path_full}"')

    else:
        #############################
        # A query!
        #############################
        query(console, response)

    return True


def query(console: Console, query_string: str) -> None:
    """Execute a query against the doc store"""
    global LAST_QUERY_RESULT

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

    results = dbo.query(query_string)
    LAST_QUERY_RESULT = results # Store away for subsequent use!

    if results:
        console.clear()
        _display_query_results(console, results)
    else:
        console.print(f"Sorry, nothing matched: [italic]'{query_string}'[/italic]\n")

################################################################################
# Management commands
################################################################################
def command_status(console: Console, verbose: bool) -> None:
    """Display the overall status of the database"""
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


def command_tags(console: Console, verbose: bool) -> None:
    """Display a summary of tags encountered."""
    tag_counts = dbo.tag_count()
    if not tag_counts:
        console.print("[bold italic]No[/bold italic] tags have been encountered yet.")
    else:
        table = Table(show_header=True, show_footer=True, box=box.SIMPLE_HEAVY)
        table.add_column("Tag")
        table.add_column("Documents")
        for dt_ in tag_counts:
            table.add_row(dt_.tag, f"{dt_.COUNT:,d}")
        console.print(table)


def command_links(console: Console, verbose: bool) -> None:
    """Display a summary of links encountered."""
    link_counts = dbo.link_summary()
    if not link_counts:
        console.print("[bold italic]No[/bold italic] links have been encountered yet.")
    else:
        table = Table(show_header=True, show_footer=True, box=box.SIMPLE_HEAVY)
        table.add_column("Tag")
        table.add_column("Documents")
        for obj in link_counts:
            table.add_row(obj.url, f"{obj.count:,d}")
        console.print(table)


class PathValidator(Validator):
    def validate(self, document):
        path = Path(document.text)
        if not path.exists():
            raise ValidationError(message="Sorry, this path doesn't exist")
        if not path.is_dir():
            raise ValidationError(message="Sorry, this path isn't a directory")


def command_index(console: Console, verbose: bool) -> bool:
    """Index a set of files (by root directory and/or suffix)"""

    def sub_prompt(prompt_: str, default_: str, *args, **kwargs) -> str:
        return prompt(f"{prompt_:11s} : ", default=default_, *args, **kwargs)

    # Get the values we last used for this command..
    index_command = get_state("index")

    ############################################################
    # Using these as defaults, prompt for any updated values
    ############################################################
    # Root directory to index from..
    index_command.root = sub_prompt(
        'Root',
        index_command.root,
        completer=PathCompleter(only_directories=True),
        validator=PathValidator())

    # What file suffix to index (if any)
    index_command.suffix = sub_prompt('Suffix', index_command.suffix)

    # Should we overwrite existing entries?
    index_command.force  = sub_prompt('Force [y/n]', index_command.force)

    # Save away these values for the next time we run this command.
    save_state("index", index_command)

    ############################################################
    # DO IT!
    ############################################################
    num_indexed, num_cleansed, time_taken = index(index_command, True)

    # Print a nice summary of what we did (based on what occurred)
    table = Table(show_header=False, box=c.DEFAULT_BOX_STYLE)
    table.add_column("-")
    table.add_column("-", justify="right")

    if num_indexed:
        metric_value = num_indexed / time_taken
        if metric_value > 1.0:
            metric_desc = "Documents per Sec"
        else:
            metric_desc = "Seconds per Doc"
            metric_value = 1.0 / metric_value
        table.add_row(f"Documents Indexed", f"[bold]{num_indexed:,d}[/bold]")
        table.add_row(f"Total Time (sec)" , f"[bold]{time_taken:.4f}[/bold]")
        table.add_row(f"{metric_desc}"    , f"[bold]{metric_value:.4f}[/bold]")
    if num_cleansed:
        table.add_row(f"Entries Cleaned"  , f"[bold]{num_cleansed:,d}[/bold]")

    if num_indexed or num_cleansed:
        console.print(table)
        return True

    console.print("\n[bold]Nothing[/bold] done.\n")
    return False


def command_db_create(console: Console, verbose: bool) -> None:
    """Create the schema in an existing database."""
    # FIXME: Confirmation!!!
    if Confirm.ask("Are you sure?", default=False):
        dbl.create_schema()
        console.print(f"Database [bold]created[/bold].")


def command_db_drop(console: Console, verbose: bool) -> None:
    """Delete the database."""
    if Confirm.ask("Are you sure?", default=False):
        dbp.drop()
        console.print(f"Database [bold]dropped[/bold].")


def command_db_clear(console: Console, verbose: bool) -> None:
    """Clean out the database of all data."""
    if Confirm.ask("Are you sure?", default=False):
        dbl.clear()
        console.print(f"Database [bold]cleared[/bold].")


def command_help(console: Console, verbose: bool):
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

    console.print("● All entries that don't start with '.' are consider queries.")

    console.print("● Entries start with '!' are Document commands:")
    table = Table(box=c.DEFAULT_BOX_STYLE)
    table.add_column("Command")
    table.add_column("Description")
    table.add_row("!<i>", "Open the file associated with the number from the last query.")
    console.print(table)

    console.print("● Entries start with '.' are RepoMan commands:")
    table = Table(box=c.DEFAULT_BOX_STYLE)
    table.add_column("Command")
    table.add_column("Description")
    for command in sorted(commands):
        table.add_row(*command)
    console.print(table)


if __name__ == "__main__":
    cli()
