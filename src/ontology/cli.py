"""CLI commands for ontology graph operations.

Usage:
    # From backend directory
    uv run python -m src.ontology.cli sync
    uv run python -m src.ontology.cli sync --dry-run
"""

import asyncio
import sys

import click

from src.db.session import async_session_maker
from src.ontology.sync_service import GraphSyncService


@click.group()
def cli() -> None:
    """Ontology graph management commands."""
    pass


@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview sync without making changes.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable verbose logging.",
)
def sync(dry_run: bool, verbose: bool) -> None:  # noqa: ARG001
    """Sync relational data to the healthcare ontology graph.

    Reads signals, facilities, metrics, and domains from PostgreSQL
    and creates corresponding vertices and edges in the AGE graph.

    Note:
        The verbose flag is accepted for CLI consistency but logging
        configuration is handled by the application runner.
    """
    click.echo("Starting ontology graph sync...")

    if dry_run:
        click.echo("DRY RUN: No changes will be made.")
        click.echo("Would sync: domains, facilities, metrics, signals, edges")
        click.echo("Dry run complete.")
        return

    try:
        stats = asyncio.run(_run_sync())
        _display_stats(stats)
        click.echo("Sync complete!")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


async def _run_sync() -> dict[str, dict[str, int]]:
    """Execute the sync operation.

    Returns:
        Statistics from the sync operation.
    """
    async with async_session_maker() as session:
        service = GraphSyncService(session)
        return await service.sync_all()


def _display_stats(stats: dict[str, dict[str, int]]) -> None:
    """Display sync statistics in a formatted table.

    Args:
        stats: Dictionary of entity type to created/skipped counts.
    """
    click.echo("\nSync Statistics:")
    click.echo("-" * 40)
    click.echo(f"{'Entity':<15} {'Created':>10} {'Skipped':>10}")
    click.echo("-" * 40)

    total_created = 0
    total_skipped = 0

    for entity, counts in stats.items():
        created = counts.get("created", 0)
        skipped = counts.get("skipped", 0)
        total_created += created
        total_skipped += skipped
        click.echo(f"{entity:<15} {created:>10} {skipped:>10}")

    click.echo("-" * 40)
    click.echo(f"{'TOTAL':<15} {total_created:>10} {total_skipped:>10}")


if __name__ == "__main__":
    cli()
