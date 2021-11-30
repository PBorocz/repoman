#!/usr/bin/env py
from pathlib import Path

import click

import db
from index import Index

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
    click.echo('Creating the database...')
    db.createdb()


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
def index(dir, suffix):
    click.echo(f"Indexing the database from: '{dir}'...")
    num_indexed = Index(db.get_db_conn()).index(dir, suffix)
    click.echo(f"Indexed {num_indexed:,d} files.")


# ###############################################################################
@main.command()
@click.option(
    '--string',
    help='Query string...'
)
def query(string):
    while True:
        click.echo('Query string? (ctrl-D or exit<cr> to exit) -> ', nl=False)
        try:
            string = input()
        except (KeyboardInterrupt, EOFError):
            break               # Allow for clean interrupts..
        if string.lower() == "exit":
            break
        if string:
            results = db.query_db(string)
            if results:
                display_query_results(results)
            else:
                print(f"Sorry, nothing matched: '{string}'\n")


def display_query_results(results: list) -> None:
    from rich.console import Console
    from rich.table import Table

    console = Console()

    table = Table(show_header=True, header_style="bold")
    table.add_column("Type", width=4)
    table.add_column("Path")
    for (path_, suffix) in results:
        table.add_row(suffix, path_)
    console.print(table)


if __name__ == "__main__":
    main()
