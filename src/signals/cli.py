"""CLI for signal hydration and management.

Provides commands for hydrating signals from dbt fct_signals table.
The ETL script (load_insight_graph_to_dbt.py) must be run first to populate
raw tables, followed by dbt build to create the fct_signals mart.
"""

from __future__ import annotations

import asyncio

import click

from src.services.signal_hydrator import SignalHydrator


@click.group()
def cli() -> None:
    """Signal management commands."""
    pass


@cli.command()
@click.option(
    "--run-id",
    "-r",
    default=None,
    help="Filter to specific run_id (optional, uses latest if not specified).",
)
@click.option(
    "--facility-id",
    "-f",
    multiple=True,
    help="Filter to specific facility IDs (can be specified multiple times).",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=None,
    help="Limit number of signals to process.",
)
def hydrate(
    run_id: str | None,
    facility_id: tuple[str, ...],
    limit: int | None,
) -> None:
    """Hydrate signals from dbt fct_signals table into the database.

    Reads signals from the dbt mart (public_marts.fct_signals) which contains
    all 23 enriched fields from the consolidated data flow.

    Prerequisites:
        1. Run load_insight_graph_to_dbt.py to load raw data
        2. Run dbt build to create fct_signals mart
        3. Then run this command to populate the signals table
    """
    # Convert tuple to list for filtering
    facility_ids = list(facility_id) if facility_id else None

    click.echo("Hydrating signals from dbt fct_signals table...")
    if run_id:
        click.echo(f"Run ID: {run_id}")
    if facility_ids:
        click.echo(f"Filtering to facilities: {', '.join(facility_ids)}")
    if limit:
        click.echo(f"Limiting to {limit} signals")

    # Run the hydration
    stats = asyncio.run(_hydrate_async(run_id, facility_ids, limit))

    # Report results
    click.echo("\nHydration complete:")
    click.echo(f"  Signals processed: {stats['signals_processed']}")
    click.echo(f"  Signals created: {stats['signals_created']}")
    click.echo(f"  Signals updated: {stats['signals_updated']}")


async def _hydrate_async(
    run_id: str | None,
    facility_ids: list[str] | None,
    limit: int | None,
) -> dict[str, int]:
    """Async implementation of signal hydration."""
    from src.db.session import async_session_maker

    hydrator = SignalHydrator(
        run_id=run_id,
        session_factory=async_session_maker,
        facility_ids=facility_ids,
        limit=limit,
    )

    return await hydrator.hydrate_signals()


@cli.command()
@click.option(
    "--facility-id",
    "-f",
    default=None,
    help="Filter to specific facility ID.",
)
def count(facility_id: str | None) -> None:
    """Count signals in the database.

    Optionally filter by facility ID.
    """
    from src.db.session import async_session_maker

    async def _count_async() -> int:
        from sqlalchemy import func, select

        from src.db.models import Signal

        async with async_session_maker() as session:
            query = select(func.count()).select_from(Signal)
            if facility_id:
                query = query.where(Signal.facility_id == facility_id)
            result = await session.execute(query)
            return result.scalar_one()

    total = asyncio.run(_count_async())
    if facility_id:
        click.echo(f"Signals for facility {facility_id}: {total}")
    else:
        click.echo(f"Total signals: {total}")


if __name__ == "__main__":
    cli()
