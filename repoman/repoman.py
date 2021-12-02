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
        click.echo(f"Indexed {num_indexed:,d} file(s).")
    else:
        click.echo(f"No files to be indexed!")


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
