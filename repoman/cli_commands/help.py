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
    # Display the list of all commands available by finding all the methods in this module that start with "command_" and using their doc-strings as the "help" text for their operation.
    def check_func(name, func):
        """Confirm that the function sent in represents a RepoMan command"""
        return inspect.isfunction(func) and name.startswith('command_') and func.__module__ == __name__

    def parse_docstring(module, docstring: str) -> tuple[str, str]:
        """Parse the docstring and get the command invocation and description"""
        command, description = "", ""
        for line in docstring.split("\n"):
            if "command:" in line.strip():
                command += line.split(":")[1].strip()
                if command == '__name__':
                    command = module.__name__.replace("cli_commands", "")
            if "description:" in line.strip():
                description += line.split(":")[1]
        return (command, description)

    commands = []
    for module in get_command_modules().values():
        for name, func in inspect.getmembers(module):
            if name != 'command':
                continue
            (command, description) = parse_docstring(module, func.__doc__)
            commands.append((command, description))

    console.print("All entries that don't start with '.' are consider queries.\n")

    console.print("Entries start with '!' are Document commands:")
    table = Table(box=c.DEFAULT_BOX_STYLE)
    table.add_column("Command")
    table.add_column("Description")
    table.add_row("!<i>", "Open the file associated with the number from the last query.")
    console.print(table)

    console.print("\nEntries start with '.' are RepoMan commands:")
    table = Table(box=c.DEFAULT_BOX_STYLE)
    table.add_column("Command")
    table.add_column("Description")
    for command in sorted(commands):
        table.add_row(*command)
    console.print(table)
