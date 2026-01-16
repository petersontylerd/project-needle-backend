"""Integration tests for signal hydration data integrity.

These tests verify that hydration does not lose signals due to
ON CONFLICT overwrites after the grain fix.

The grain fix ensures fct_signals has one row per entity (not per method),
so hydration should insert all entities without data loss.

These tests require a live PostgreSQL database with dbt tables populated.
Run after: 1) load_insight_graph_to_dbt.py, 2) dbt run

Usage:
    cd backend
    UV_CACHE_DIR=../.uv-cache uv run pytest tests/integration/test_hydration_data_integrity.py -v
"""

from __future__ import annotations

import logging
import os

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.services.signal_hydrator import SignalHydrator

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Get database URL from environment or use default (port 5433 for docker-compose dbt setup)
DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:5433/quality_compass",
    ),
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def async_engine():
    """Create async SQLAlchemy engine for integration tests.

    Returns:
        AsyncEngine: SQLAlchemy async engine.
    """
    return create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )


@pytest.fixture
def session_maker(async_engine):
    """Create async session maker for integration tests.

    Args:
        async_engine: SQLAlchemy async engine.

    Returns:
        async_sessionmaker: Session factory.
    """
    return async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture
async def db_session(session_maker):
    """Provide a fresh database session for each test.

    Args:
        session_maker: Session factory fixture.

    Yields:
        AsyncSession: Database session.

    Raises:
        pytest.skip: If database connection fails.
    """
    async with session_maker() as session:
        try:
            # Verify connection
            await session.execute(text("SELECT 1"))
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
        yield session


@pytest.fixture
def hydrator(session_maker):
    """Create a SignalHydrator instance with dbt session factory.

    Args:
        session_maker: Session factory fixture pointing to dbt database.

    Returns:
        SignalHydrator: Hydrator instance with injected session factory.
    """
    return SignalHydrator(session_factory=session_maker)


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
async def test_hydration_preserves_all_entities(hydrator: SignalHydrator):
    """Verify hydration doesn't lose signals due to ON CONFLICT.

    After the grain fix, fct_signals has one row per entity.
    Hydration should insert all of them without overwrites.
    """
    # Get count of unique entities in fct_signals
    try:
        fct_count = await hydrator.get_fct_signal_count()
    except Exception as e:
        pytest.skip(f"Could not query fct_signals: {e}")

    if fct_count == 0:
        pytest.skip("No signals in fct_signals table")

    # Run hydration
    stats = await hydrator.hydrate_signals()

    # Verify all signals were processed
    assert stats["signals_processed"] == fct_count, f"Expected to process {fct_count} signals, but processed {stats['signals_processed']}"

    # Verify no signals were skipped
    assert stats["signals_skipped"] == 0, f"Expected 0 skipped signals, but {stats['signals_skipped']} were skipped"

    # Verify database count matches fct_signals count
    db_count = await hydrator.get_signal_count()
    assert db_count == fct_count, (
        f"Expected {fct_count} signals in database, but found {db_count}. "
        f"Data loss: {fct_count - db_count} signals "
        f"({(fct_count - db_count) / fct_count * 100:.1f}%)"
    )


@pytest.mark.asyncio
async def test_fct_signals_has_no_method_duplicates(db_session: AsyncSession):
    """Verify fct_signals has entity-level grain (no method fan-out).

    Note: We use signal_id (not entity_result_id) because signal_id is the
    surrogate key derived from (run_id, canonical_node_id, entity_result_id)
    and is exposed in the final output, while entity_result_id is internal.
    """
    result = await db_session.execute(
        text("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT signal_id) as unique_signals
        FROM public_marts.fct_signals
    """)
    )
    row = result.fetchone()

    if row is None or row[0] == 0:
        pytest.skip("No data in fct_signals")

    total_rows, unique_signals = row

    assert total_rows == unique_signals, (
        f"fct_signals has {total_rows} rows but only {unique_signals} unique signals. "
        f"This indicates method-level fan-out still exists. "
        f"Fan-out ratio: {total_rows / unique_signals:.2f}x"
    )


@pytest.mark.asyncio
async def test_get_technical_details_with_entity_hash(hydrator: SignalHydrator, db_session: AsyncSession):
    """Verify get_technical_details returns correct data with entity_dimensions_hash."""
    # Get a sample signal from fct_signals
    result = await db_session.execute(
        text("""
        SELECT canonical_node_id, entity_dimensions_hash
        FROM public_marts.fct_signals
        LIMIT 1
    """)
    )
    row = result.fetchone()

    if row is None:
        pytest.skip("No signals in fct_signals")

    canonical_node_id, entity_dimensions_hash = row

    # Fetch technical details with hash
    details = await hydrator.get_technical_details(
        canonical_node_id,
        entity_dimensions_hash=entity_dimensions_hash,
    )

    assert details is not None, "Technical details should be found"
    assert "statistical_methods" in details, "Should include statistical_methods JSONB"
    assert details["statistical_methods"] is not None, "statistical_methods should not be None"


@pytest.mark.asyncio
async def test_statistical_methods_jsonb_structure(db_session: AsyncSession):
    """Verify statistical_methods JSONB has expected structure."""
    result = await db_session.execute(
        text("""
        SELECT statistical_methods
        FROM public_marts.fct_signals
        WHERE statistical_methods IS NOT NULL
        LIMIT 1
    """)
    )
    row = result.fetchone()

    if row is None:
        pytest.skip("No signals with statistical_methods")

    methods = row[0]

    # Should be a list
    assert isinstance(methods, list), "statistical_methods should be a JSON array"
    assert len(methods) > 0, "statistical_methods should not be empty"

    # Each method should have required fields
    required_fields = ["method_name", "simple_zscore", "robust_zscore"]
    for method in methods:
        for field in required_fields:
            assert field in method, f"Method missing required field: {field}"
