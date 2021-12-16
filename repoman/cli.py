#!/usr/bin/env py
import os
from importlib import import_module
from pathlib import Path
from types import ModuleType

import click
from prompt_toolkit import PromptSession, prompt
from prompt_toolkit.completion import PathCompleter
from prompt_toolkit.history import FileHistory
from pyfiglet import Figlet
from rich.console import Console
from rich.table import Table

import constants as c
import db_operations as dbo
import db_physical as dbp
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
            console.print("[italic]Goodbye![/]")
            break

        if response.lower() in (".exit", ".q"):  # Done?
            console.print("[italic]Goodbye![/]")
            break

        if response:  # As Ahhhnold would say...DOO EET!
            with dbp.database.connection_context() as ctx:
                execute(verbose, response)


def execute(verbose: bool, response: str) -> bool:
    """Execute the "response" provided, determining whether or not
    it's a "command" or "simply" a query to be executed."""
    console = Console()      # Every command is going to put out to the console.

    if response.startswith('.'):
        #############################
        # A *RepoMan* command
        #############################
        command = response[1:]
        command_module = get_module_for_command(command)
        if command_module:
            console.clear() # We're good, execute it!
            command_module.command(console, verbose)
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
        console.clear()
        command_module = get_module_for_command('query')
        command_module.command(console, response)

    return True

################################################################################
# Cache of modules in the "cli_commands" directory allowing for
# auto-discovery of new commands
################################################################################
COMMAND_MODULES = dict()
def populate_command_modules_cache() -> None:
    global COMMAND_MODULES
    commands_path = Path(__file__).parent / Path("cli_commands")
    for path_ in commands_path.glob("*.py"):
        if not path_.stem.startswith('__'):  # Skip "__init__.py"
            COMMAND_MODULES[path_.stem] = import_module(f"cli_commands.{path_.stem}")
populate_command_modules_cache()


def get_command_modules() -> dict[str, ModuleType]:
    return COMMAND_MODULES


def get_module_for_command(command: str) -> ModuleType:
    return COMMAND_MODULES.get(command, None)


if __name__ == "__main__":
    cli()
