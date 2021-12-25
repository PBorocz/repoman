from functools import partial
from pathlib import Path

from prompt_toolkit import prompt
from prompt_toolkit.completion import PathCompleter
from rich.console import Console
from rich.table import Table

import constants as c
from cli_utils import get_state, save_state, YesNoValidator, PathValidator, sub_prompt
from index import index, cleanup
from utils import get_user_history_path


def command(console: Console, verbose: bool) -> bool:
    """
    command: __name__
    description: Index a set of files (by root directory and/or suffix)
    """
    _sub_prompt = partial(sub_prompt, length=13)

    # Get the values we last used for this command..
    index_parms = get_state("index")

    ############################################################
    # Using these as defaults, prompt for any updated values
    ############################################################
    # Root directory to index from..
    index_parms.root = _sub_prompt(
        'Root',
        index_parms.root,
        completer=PathCompleter(only_directories=True),
        validator=PathValidator())

    # What file suffix to index (if any)
    index_parms.suffix = _sub_prompt(
        'Suffix',
        index_parms.suffix)

    # Should we overwrite existing entries?
    index_parms.force = _sub_prompt(
        'Force (y/n)',
        index_parms.force,
        validator=YesNoValidator())

    # Should we overwrite existing entries?
    index_parms.verbose = _sub_prompt(
        'Verbose (y/n)',
        index_parms.verbose,
        validator=YesNoValidator())

    # Save away these values for the next time we run this command.
    save_state("index", index_parms)

    ############################################################
    # DO IT!
    ############################################################
    num_indexed, time_taken = index(console, index_parms)

    # Cleanup any documents in the database that no longer appear on disk.
    num_cleansed = cleanup()

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
        table.add_row(f"Documents Indexed:", f"[bold]{num_indexed:,d}[/bold]")
        table.add_row(f"Total Time (sec):" , f"[bold]{time_taken:.4f}[/bold]")
        table.add_row(f"{metric_desc}:"    , f"[bold]{metric_value:.4f}[/bold]")

    if num_cleansed:
        table.add_row(f"Entries Cleaned"  , f"[bold]{num_cleansed:,d}[/bold]")

    if num_indexed or num_cleansed:
        console.print(table)
        return True

    console.print("\nOk, [bold]Nothing[/bold] done.\n")
    return False
