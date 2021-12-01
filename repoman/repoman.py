#!/usr/bin/env py
from pathlib import Path

import click

import db
from index import Index
from rich.console import Console
from rich.table import Table

# Utility methods..
def abort_if_false(ctx, param, value):
    if not value:
        ctx.abort()


@click.group()
def main():
    ...

# ###############################################################################
@main.command()
def createdb():
    db.createdb()
    click.echo('Created the database...')


# ###############################################################################
@main.command()
@click.option('--yes', is_flag=True, callback=abort_if_false,
              expose_value=False,
              prompt='Are you sure you want to drop the db?')
def dropdb():
    db.dropdb()
    click.echo('Dropped the database')


# ###############################################################################
@main.command()
@click.option(
    '--dir',
    default="~/Repository/3.Resources",
    type=click.Path(),
    help='Directory to start indexing from, default is'
)
@click.option(
    '--suffix',
    default=".txt",
    help='File suffixes to index, eg. txt, org, pdf ...'
)
@click.option(
    '--debug/--no-debug',
    default=False,
    help='Debug mode?'
)
@click.option(
    '--force/--no-force',
    default=False,
    help='Force updates if document has already been indexed?'
)
def index(dir, suffix, debug, force):
    click.echo(f"Indexing the database from: '{dir}'...")
    num_indexed = Index(db.get_db_conn()).index(debug, dir, suffix, force)
    if num_indexed:
        click.echo(f"Indexed {num_indexed:,d} files.")
    else:
        click.echo(f"No files to be indexed!")


# ###############################################################################
@main.command()
@click.option(
    '--string',
    help='Query string...'
)
def query(string):

    console = Console()
    console.clear()
    click.echo('Query string? (ctrl-D or exit<cr> to exit) -> ', nl=False)
    while True:
        try:
            string = input()
        except (KeyboardInterrupt, EOFError):
            break               # Allow for clean interrupts..
        if string.lower() == "exit":
            break
        if string:
            results = db.query_db(string)
            if results:
                console.clear()
                display_query_results(console, results)
            else:
                print(f"Sorry, nothing matched: '{string}'\n")
        click.echo('Query string? (ctrl-D or exit<cr> to exit) -> ', nl=False)


def display_query_results(console, results: list) -> None:

    table = Table(show_header=True, header_style="bold")
    table.add_column("Type", width=4)
    table.add_column("Path")
    for (path_, suffix) in results:
        table.add_row(suffix, path_)
    console.print(table)

if __name__ == "__main__":
    main()
