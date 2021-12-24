import os
from contextlib import suppress
from functools import partial
from pathlib import Path

from prompt_toolkit import prompt
from rich.console import Console # Typing
from rich.markup import escape
from rich.prompt import Prompt
from rich.table import Table
from typing import Optional, Tuple

import constants as c
import db_operations as dbo
from db_logical import Document
from cli_utils import get_state, save_state, update_state, SortOrderValidator, IntValidator, sub_prompt
from utils import humanify_size
from adts import QueryCommandParameters, SortOrderChoices, QueryResult


LAST_QUERY_RESULTS = None

def command(
        console: Console,
        query_string: Optional[str] = None,
        response: Optional[str]=None,
        verbose: Optional[bool]=False,
) -> Optional[str]:
    """
    command: .q/.query
    description: Execute a text-search query with all options available.
    """
    global LAST_QUERY_RESULTS

    # What type of ui are we executing?
    if not query_string:
        # Advanced query execution (prompt for query parameters...)

        # Get the state (if any) of the last time we did this..
        query_parms = get_state("query")

        # Ask user for any changes to these parameters..
        query_parms = get_query_parms(console, query_parms)

        # Save away these values for the next time we run this command.
        save_state("query", query_parms)
    else:
        # Direct query execution (we've been given what to search for)
        query_parms = QueryCommandParameters(query_string=query_string)
        update_state("query", "query_string", query_string)

    # DO our query based on the specified query parameters available!
    if results := dbo.query(query_parms):
        results = _sort_filter_results(query_parms, results)
        message_or_none = _display_query_results(console, results)
        LAST_QUERY_RESULTS = results
        return message_or_none

    LAST_QUERY_RESULTS = None
    console.print(f"Sorry, nothing matched: [italic]'{query_parms.query_string}'[/italic]\n")
    return None


def get_query_parms(console: Console, query_parms: QueryCommandParameters) -> QueryCommandParameters:
    """Advanced query, gather query string and allow for other options
    to be selected as well (e.g. sort order, columns etc.)
    """
    _sub_prompt = partial(sub_prompt, length=13)

    query_parms.query_string = _sub_prompt(  # What query string?
        'Query',
        getattr(query_parms, 'query_string', query_parms.query_string))

    query_parms.suffix = _sub_prompt(  # Limit to a particular suffix?
        'File Suffix',
        getattr(query_parms, "suffix", query_parms.suffix))

    query_parms.sort_order = _sub_prompt(  # What order to return results?
        'Sort Order',
        getattr(query_parms, "sort_order", query_parms.sort_order),
        validator=SortOrderValidator())

    return query_parms


def _sort_filter_results(query_parms: QueryCommandParameters, results: list[QueryResult]) -> list[QueryResult]:

    def filter_results_by_suffix(query_parms: QueryCommandParameters, results: list[QueryResult]) -> list[QueryResult]:
        if query_parms.suffix:
            results = list(filter(
                lambda doc: doc.suffix.lower() == query_parms.suffix.lower(),
                results))
        return results

    def sort_results(query_parms: QueryCommandParameters, results: list[QueryResult]) -> list[QueryResult]:
        """Return a set of results but sorted based on the respective
        attribute from the query_parms.
        """
        # Reverse order?
        reverse = True if query_parms.sort_order.startswith("-") else False

        # By what attribute?
        order_by_attribute = SortOrderChoices[query_parms.sort_order.replace("-", "")].value

        # Do it..
        results.sort(
            key=lambda ao_: getattr(ao_, order_by_attribute),
            reverse=reverse)

        return results

    results = filter_results_by_suffix(query_parms, results)
    results = sort_results(query_parms, results)
    return results


def _display_query_results(console: Console, results: list[QueryResult]) -> Optional[str]:

    def chunker(lst: list, n: int) -> list:
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def markup_snippet(snippet):
        """On our queries, we can't use the Rich markup to delineate matching text,
        here, we "undo" that and convert to that which'll be displayed to the user.
        """
        snippet = escape(snippet)
        snippet = snippet.replace(">>>", "[green bold]")
        snippet = snippet.replace("<<<", "[/]")
        return snippet

    ith = 0

    for page, chunk in enumerate(chunker(results, console.size.height-1)):
        table = Table(show_header=True, header_style="bold", box=c.DEFAULT_BOX_STYLE)
        table.add_column("#")
        table.add_column("Name")
        table.add_column("Snippet")
        table.add_column("LastMod")
        for obj in chunk:
            table.add_row(
                f"{ith+1:,d}",
                obj.path_full.name,
                markup_snippet(obj.snippet),
                obj.last_mod.split(' ')[0],  # Don't need time..
            )
            ith += 1
        console.clear()
        console.print(table)

        remaining = len(results) - ith
        if remaining > 0:
            prompt_ = f"[b]{remaining:,d}[/] left; [b]<ith>[/] doc to open; [i]<ret>[/] for next set; [b]q[/] to quit"
        else:
            prompt_ = "[b]<ith>[/] doc to open; [b]q[/] to quit"

        try:
            # Use Rich's prompt here to be able to take advantage of formatting
            next_ = Prompt.ask(prompt_)
        except (KeyboardInterrupt, EOFError):
            return None         # Break out, we're done...

        if not next_:
            continue            # No response..keep paging through results

        if next_.lower().startswith("q"):
            return None

        # If the response is an integer, open the respective document path
        try:
            return open_file(console, results[int(next_)-1].doc_id)
        except ValueError:
            continue

def open_file(console: Console, doc_id: int) -> Optional[str]:

    docs = Document.select().where(Document.id == doc_id).execute()
    if not docs:
        return None
    doc = docs[0]

    # Get information about the file...
    path_ = Path(doc.path)
    stat_ = os.stat(path_)
    size_ = stat_.st_size

    console.print(f"   {'Doc Id':16s} {doc.id}")
    console.print(f"   {'Path':16s} {path_.parent}")
    console.print(f"   {'File':16s} {path_.name}")
    console.print(f"   {'Last Modified':16s} {doc.last_mod}")
    console.print(f"   {'Size':16s} {humanify_size(size_)}")
    yes_no_other = Prompt.ask("Open this file \[y/n]?")

    if yes_no_other and yes_no_other.lower().startswith("y"):
        os.system(f'open "{path_}"')
        return f"Opening file: [b]{path_full.name}..."
    return None
