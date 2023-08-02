#!/usr/bin/env py
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Optional

import click
from prompt_toolkit import PromptSession, prompt
from prompt_toolkit.history import FileHistory
from pyfiglet import Figlet
from rich.console import Console

import constants as c
import db_physical as dbp
import db_operations as dbo
from utils import get_user_history_path

################################################################################
# Primary CLI/UI for RepoMan!
################################################################################
@click.command()
@click.option('--query', help='Query to execute on startup?', type=str)
@click.option('--verbose/--no-verbose', default=False, help='Verbose mode?')
def cli(verbose: bool, query: Optional[str]) -> None:

    console = Console()

    # If we have a query to run from the command-line, do it,
    # otherwise, put out the regular introductory banner.
    if query:
        with dbp.database.connection_context() as ctx:
            message_or_none = get_command_method('query')(console, query_string=query)
            if message_or_none:
                console.print(message_or_none)
    else:
        console.clear()
        print(Figlet(font='standard').renderText('Repo-Man'))
        console.print(c.INTRODUCTION)
        with dbp.database.connection_context() as ctx:
            status = dbo.status()
            if status.total_docs:
                console.print(f"[bold italic]{status.total_docs:,d}[/bold italic] documents to query from.\n")

    # Setup our prompt session allow commands over sessions (!)
    session = PromptSession(history=FileHistory(get_user_history_path()))

    while True:
        try:
            response = session.prompt(c.PROMPT)
        except (KeyboardInterrupt, EOFError):
            console.print("[italic]Goodbye![/]")
            break

        if response.lower() in (".exit",):  # Done?
            console.print("[italic]Goodbye![/]")
            break

        if response:  # As Ahhhnold would say...DOO EET!
            with dbp.database.connection_context() as ctx:
                execute(verbose, console, response)


def execute(verbose: bool, console: Console, response: str) -> bool:
    """Execute the "response" provided, determining whether or not
    it's a "command" or "simply" a query to be executed."""

    if response.startswith('.'):
        ##########################################################
        # A non-query repoman command
        ##########################################################
        command = response[1:]
        command_method = get_command_method(command)
        if command_method:
            result = command_method(console, verbose=verbose)
        else:
            console.print(f"Sorry, [red bold]{response}[/red bold] is not a known command (.help to list them)")
    else:
        ##########################################################
        # A query (either short or "advanced")
        ##########################################################
        query_string = None if response.lower() == '.q' else response
        get_command_method('query')(console, query_string=query_string)

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


def get_command_method(command: str) -> callable:
    module_ = COMMAND_MODULES.get(command, None)
    if module_:
        return module_.command
    return None

def get_command_modules() -> dict[str, ModuleType]:
    return COMMAND_MODULES



if __name__ == "__main__":
    cli()
