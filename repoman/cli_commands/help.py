import re
import sys
import inspect
from textwrap import dedent

from rich.console import Console
from rich.table import Table

import constants as c
from cli import get_command_modules

def command(console: Console, verbose: bool):
    """
    command: __name__
    description: Display the list of all RepoMan commands available.
    """
    # Display the list of all commands available by finding all the
    # methods in this module that start with "command_" and using
    # their doc-strings as the "help" text for their operation.
    def parse_docstring(module, docstring: str) -> tuple[str, str]:
        """Parse the docstring and get the command invocation and description"""
        command, description = "", ""
        for line in docstring.split("\n"):
            if "command:" in line.strip():
                command += line.split(":")[1].strip()
                if command == '__name__':
                    command = module.__name__.replace("cli_commands", "")

            if "description:" in line.strip():
                description += line.split(":")[1].strip()
        return (command, description)

    commands = []
    for module in get_command_modules().values():
        for name, func in inspect.getmembers(module):
            if name != 'command':
                continue
            (command, description) = parse_docstring(module, func.__doc__)
            commands.append((command, description))

    console.print("All entries that don't start with '.' are consider queries.\n")

    console.print("Entries start with '.' are [italic]repoman[/] commands:")
    left_column_width = max([len(command) for command, _ in commands]) + 1
    for (command, description) in sorted(commands):
        console.print(f"[bold]{command:{left_column_width}s}[/] {description}")

    # console.print("\nEntries start with '!' are [italic]document[/] commands:")
    # console.print(f"[bold]{'!<i>':{left_column_width}s}[/] Open the file associated with the number from the last query.\n")
