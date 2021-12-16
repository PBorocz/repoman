from pathlib import Path

from prompt_toolkit import prompt
from prompt_toolkit.completion import PathCompleter
from prompt_toolkit.validation import Validator, ValidationError
from rich.console import Console
from rich.table import Table

import constants as c
from cli_state import get_state, save_state
from index import index
from utils import get_user_history_path


class PathValidator(Validator):
    def validate(self, document):
        path = Path(document.text)
        if not path.exists():
            raise ValidationError(message="Sorry, this path doesn't exist")
        if not path.is_dir():
            raise ValidationError(message="Sorry, this path isn't a directory")


def command(console: Console, verbose: bool) -> bool:
    """
    command: __name__
    description: Index a set of files (by root directory and/or suffix)
    """
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
