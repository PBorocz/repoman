from prompt_toolkit import prompt
from prompt_toolkit.validation import Validator, ValidationError
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from typing import Optional, Tuple

import constants as c
import db_operations as dbo
from cli_state import get_state, save_state
from utils import AnonymousObj


def command(
        console: Console,
        query_string: Optional[str] = None,
        response: Optional[str]=None,
        verbose: Optional[bool]=False,
) -> AnonymousObj:
    """
    command: .q/.query
    description: Execute a text-search query with all options available.
    """
    # Get the state (if any) of the last time we did this..
    query_parms = get_state("query")

    # What type of ui are we executing?
    if not query_string:
        # Advanced query execution (prompt for query parameters...)
        query_parms = get_query_parms(console, query_parms)
    else:
        # Direct query execution (we've been given what to search for)
        query_parms.query_string = query_string

    # Save away these values for the next time we run this command.
    save_state("query", query_parms)

    # DO our query based on the specified query parameters available!
    if query_results := dbo.query(query_parms.query_string):
        console.clear()
        query_results, more = _sort_filter_results(query_parms, query_results)
        _display_query_results(console, query_results, more)
        return query_results

    console.print(f"Sorry, nothing matched: [italic]'{query_parms.query_string}'[/italic]\n")
    return None

def get_query_parms(console: Console, query_parms: AnonymousObj) -> AnonymousObj:
    """Advanced query, gather query string and allow for other options
    to be selected as well (e.g. sort order, columns etc.)
    """
    def sub_prompt(prompt_: str, default_: str, *args, **kwargs) -> str:
        return prompt(f"{prompt_:14s} > ", default=default_, *args, **kwargs)

    query_parms.query_string = sub_prompt(  # What query string?
        'Query',
        getattr(query_parms, 'query_string', c.DEFAULTS["query"]["query_string"]))

    query_parms.suffix = sub_prompt(  # Limit to a particular suffix?
        'File Suffix',
        getattr(query_parms, "suffix", c.DEFAULTS["query"]["suffix"]))

    query_parms.sort_order = sub_prompt(  # What order to return results?
        'Sort Order',
        getattr(query_parms, "sort_order", c.DEFAULTS["query"]["sort_order"]),
        validator=SortOrderValidator())

    query_parms.top_n = sub_prompt(  # Top "n" results?
        'Top N Results',
        getattr(query_parms, "top_n", c.DEFAULTS["query"]["top_n"]),
        validator=IntValidator())

    return query_parms


class SortOrderValidator(Validator):
    def validate(self, document):
        try:
            c.SortOrderChoices[document.text.lower().replace("-", "")]
        except KeyError:
            raise ValidationError(message=f"Sorry, only valid sort order entries are: {c.SORT_ORDER_CHOICES}")


class IntValidator(Validator):
    def validate(self, document):
        try:
            if document.text:   # We allow empty here...
                int(document.text)
        except ValueError:
            raise ValidationError(message=f"Sorry, Top-N must be an integer or empty")

def _sort_filter_results(query_parms: AnonymousObj, results: list[AnonymousObj]) -> tuple[list[AnonymousObj], bool]:

    def filter_results_by_suffix(query_parms: AnonymousObj, results: list[AnonymousObj]) -> list[AnonymousObj]:
        if query_parms.suffix:
            results = list(filter(
                lambda doc: doc.suffix.lower() == query_parms.suffix.lower(),
                results))
        return results

    def top_n_results(query_parms: AnonymousObj, results: list[AnonymousObj]) -> tuple[list[AnonymousObj], int]:
        more = None
        if query_parms.top_n:
            top_n = int(query_parms.top_n)
            if len(results) > top_n:
                more = len(results) - top_n
            results = results[0:top_n]
        return results, more

    def sort_results(query_parms: AnonymousObj, results: list[AnonymousObj]) -> list[AnonymousObj]:
        """Return a set of results but sorted based on the respective
        attribute from the query_parms.
        """
        # Reverse order?
        reverse = True if query_parms.sort_order.startswith("-") else False

        # By what attribute?
        order_by_attribute = c.SortOrderChoices[query_parms.sort_order.replace("-", "")].value

        # Do it..
        results.sort(
            key=lambda ao_: getattr(ao_, order_by_attribute),
            reverse=reverse)

        return results

    results = filter_results_by_suffix(query_parms, results)
    results, more = top_n_results(query_parms, results)
    results = sort_results(query_parms, results)
    return results, more


def _display_query_results(console, results: list[AnonymousObj], more: bool) -> None:

    def markup_snippet(snippet):
        """On our queries, we can't use the Rich markup to delineate matching text,
        here, we "undo" that and convert to that which'll be displayed to the user.
        """
        snippet = escape(snippet)
        snippet = snippet.replace(">>>", "[green bold]")
        snippet = snippet.replace("<<<", "[/]")
        return snippet

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
    if more:
        console.print(f"({more:,d} more)")
