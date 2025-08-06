from __future__ import annotations

import click

from .jobs.realtime import main as realtime_job


@click.group()
def cli():
    """Utility command line interface."""


@cli.command()
def realtime():
    """Run the realtime streaming job."""
    realtime_job()


if __name__ == "__main__":  # pragma: no cover
    cli()
