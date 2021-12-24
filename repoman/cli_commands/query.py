import os
from contextlib import suppress
from functools import partial

from prompt_toolkit import prompt
from rich.console import Console
from rich.markup import escape
from rich.prompt import Prompt
from rich.table import Table
from typing import Optional, Tuple

import constants as c
import db_operations as dbo
from cli_utils import get_state, save_state, update_state, SortOrderValidator, IntValidator
from utils import AnonymousObj, sub_prompt
from adts import QueryCommandParameters, SortOrderChoices


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
    global LAST_QUERY_RESULTS
    if LAST_QUERY_RESULTS := dbo.query(query_parms):
        query_results   = _sort_filter_results(query_parms, LAST_QUERY_RESULTS)
        message_or_none = _display_query_results(console, LAST_QUERY_RESULTS)
        return message_or_none

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

    query_parms.top_n = _sub_prompt(  # Top "n" results?
        'Top N Results',
        getattr(query_parms, "top_n", query_parms.top_n),
        validator=IntValidator())

    return query_parms


def _sort_filter_results(query_parms: QueryCommandParameters, results: list[AnonymousObj]) -> list[AnonymousObj]:

    def filter_results_by_suffix(query_parms: QueryCommandParameters, results: list[AnonymousObj]) -> list[AnonymousObj]:
        if query_parms.suffix:
            results = list(filter(
                lambda doc: doc.suffix.lower() == query_parms.suffix.lower(),
                results))
        return results

    def top_n_results(query_parms: QueryCommandParameters, results: list[AnonymousObj]) -> list[AnonymousObj]:
        more = None
        if query_parms.top_n:
            top_n = int(query_parms.top_n)
            if len(results) > top_n:
                more = len(results) - top_n
            results = results[0:top_n]
        return results

    def sort_results(query_parms: QueryCommandParameters, results: list[AnonymousObj]) -> list[AnonymousObj]:
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
    # results = top_n_results(query_parms, results)
    results = sort_results(query_parms, results)
    return results


def _display_query_results(console, results: list[AnonymousObj]) -> Optional[str]:

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
            row = results[int(next_)-1]
            os.system(f'open "{row.path_full}"')
            return f"Opening file: [b]{row.path_full.name}"

        except ValueError:
            continue
