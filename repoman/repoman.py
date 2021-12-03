#!/usr/bin/env py
from pathlib import Path

import click

import db
from index import Index
from query import query_ui

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
@click.option(
    '--yes',
    is_flag=True,
    callback=abort_if_false,
    expose_value=False,
    prompt='Are you sure you want to clear the database?'
)
def cleardb():
    db.cleardb()
    click.echo('Database cleared.')


# ###############################################################################
@main.command()
@click.option(
    '--yes',
    is_flag=True,
    callback=abort_if_false,
    expose_value=False,
    prompt='Are you sure you want to drop the database?'
)
def dropdb():
    db.dropdb()
    click.echo('Database dropped.')


# ###############################################################################
@main.command()
def createdb():
    db.createdb()
    click.echo('Database created.')


# ###############################################################################
@main.command()
@click.option(
    '--dir',
    default="~/Repository/4.Archives",
    type=click.Path(),
    help='Directory to start indexing from, default is'
)
@click.option(
    '--suffix',
    default=".txt",
    help='File suffixes to index, eg. txt, org, pdf ...'
)
@click.option(
    '--verbose/--no-verbose',
    default=False,
    help='Verbose mode?'
)
@click.option(
    '--force/--no-force',
    default=False,
    help='Force updates if document has already been indexed?'
)
def index(dir, suffix, verbose, force):
    click.echo(f"Indexing the database from: '{dir}'...")
    indexer = Index(db.get_db_conn())
    num_indexed = indexer.index(verbose, dir, suffix, force)
    if num_indexed:
        click.echo(f"Indexed {num_indexed:,d} file(s).")
    else:
        click.echo(f"No files to be indexed!")
    if verbose:
        Console().print(indexer.suffixes_skipped)

# ###############################################################################
@main.command()
@click.option(
    '--string',
    help='Query string...'
)
def query(string):
    query_ui(string)


if __name__ == "__main__":
    main()
