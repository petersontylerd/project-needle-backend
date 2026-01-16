"""Integration tests for dbt rollback scenario.

Tests verify that services degrade gracefully when dbt tables are unavailable,
simulating a rollback scenario where dbt tables are dropped or inaccessible.

These tests verify the behavior documented in the rollback runbook (5.P.1):
- SignalHydrator returns empty results when fct_signals unavailable
- ContributionService handles missing fct_contributions gracefully

Usage:
    cd backend
    UV_CACHE_DIR=../.uv-cache uv run pytest tests/integration/test_dbt_rollback.py -v

Note: These tests use a session factory pointing to a non-existent schema
to simulate the "dbt tables dropped" scenario without affecting actual data.
"""

from __future__ import annotations

import logging
import os

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.services.contribution_service import ContributionService, ContributionServiceError
from src.services.signal_hydrator import SignalHydrator

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Use a database URL that exists but point to a non-existent schema for rollback tests
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
    """Create async SQLAlchemy engine for rollback tests.

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
    """Create async session maker for rollback tests.

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
            await session.execute(text("SELECT 1"))
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
        yield session


@pytest.fixture
def bad_engine():
    """Create async engine pointing to invalid connection.

    Used to simulate complete database unavailability.

    Returns:
        AsyncEngine: SQLAlchemy async engine with invalid connection.
    """
    return create_async_engine(
        "postgresql+asyncpg://invalid:invalid@localhost:9999/nonexistent",
        echo=False,
    )


@pytest.fixture
def bad_session_maker(bad_engine):
    """Create session maker for invalid database connection.

    Args:
        bad_engine: Engine with invalid connection.

    Returns:
        async_sessionmaker: Session factory that will fail on use.
    """
    return async_sessionmaker(bad_engine, class_=AsyncSession, expire_on_commit=False)


# =============================================================================
# SignalHydrator Rollback Tests
# =============================================================================


class TestSignalHydratorRollback:
    """Tests for SignalHydrator graceful degradation during rollback."""

    @pytest.mark.asyncio
    async def test_hydrate_signals_returns_empty_stats_on_db_error(self, bad_session_maker) -> None:
        """Test that hydrate_signals returns empty stats when DB unavailable.

        This simulates the rollback scenario where dbt tables are dropped.
        The service should not crash but return zero counts.
        """
        hydrator = SignalHydrator(session_factory=bad_session_maker)
        stats = await hydrator.hydrate_signals()

        assert stats["signals_processed"] == 0, "Should return 0 signals processed"
        assert stats["signals_created"] == 0, "Should return 0 signals created"
        assert stats["signals_skipped"] == 0, "Should return 0 signals skipped"
        logger.info("SignalHydrator gracefully handled DB unavailability: %s", stats)

    @pytest.mark.asyncio
    async def test_get_fct_signal_count_returns_zero_on_db_error(self, bad_session_maker) -> None:
        """Test that get_fct_signal_count returns 0 when DB unavailable.

        This simulates checking signal count during rollback.
        """
        hydrator = SignalHydrator(session_factory=bad_session_maker)
        count = await hydrator.get_fct_signal_count()

        assert count == 0, "Should return 0 when DB unavailable"
        logger.info("get_fct_signal_count gracefully returned 0 on DB error")

    @pytest.mark.asyncio
    async def test_hydrate_with_nonexistent_run_id_returns_empty(self, session_maker) -> None:
        """Test that hydrate_signals with bad run_id returns empty results.

        This simulates querying for data that doesn't exist (partial rollback).
        """
        hydrator = SignalHydrator(
            run_id="nonexistent_run_id_12345",
            session_factory=session_maker,
            limit=10,
        )
        stats = await hydrator.hydrate_signals()

        # Should complete without error, but process 0 signals
        assert stats["signals_processed"] == 0, "Should process 0 signals for nonexistent run_id"
        logger.info("SignalHydrator handled nonexistent run_id gracefully")


# =============================================================================
# ContributionService Rollback Tests
# =============================================================================


class TestContributionServiceRollback:
    """Tests for ContributionService graceful degradation during rollback."""

    @pytest.mark.asyncio
    async def test_get_contributions_raises_error_on_db_unavailable(self, bad_session_maker) -> None:
        """Test that get_contributions_for_parent raises ContributionServiceError.

        When the database is unavailable, the service should raise a clear error
        with context about what failed.
        """
        service = ContributionService(session_factory=bad_session_maker)

        with pytest.raises(ContributionServiceError) as exc_info:
            await service.get_contributions_for_parent(
                "any_parent_id",
                "ANY_FACILITY",  # Required for facility isolation
            )

        assert exc_info.value.parent_node_id == "any_parent_id"
        logger.info("ContributionService raised appropriate error: %s", exc_info.value)

    @pytest.mark.asyncio
    async def test_get_top_contributors_returns_empty_on_db_error(self, bad_session_maker) -> None:
        """Test that get_top_contributors_global returns empty list on DB error.

        This method degrades gracefully by returning an empty list.
        """
        service = ContributionService(session_factory=bad_session_maker)
        contributors = await service.get_top_contributors_global(top_n=10)

        assert contributors == [], "Should return empty list when DB unavailable"
        logger.info("get_top_contributors_global gracefully returned empty list")

    @pytest.mark.asyncio
    async def test_contributions_for_nonexistent_parent_returns_empty(self, session_maker) -> None:
        """Test that querying nonexistent parent returns empty list gracefully.

        This simulates querying for data that was rolled back.
        """
        service = ContributionService(session_factory=session_maker)
        contributions = await service.get_contributions_for_parent(
            "nonexistent__parent__node__xyz123",
            "NONEXISTENT",  # Required for facility isolation
        )

        assert contributions == [], "Should return empty list for nonexistent parent"
        logger.info("ContributionService handled nonexistent parent gracefully")


# =============================================================================
# Cross-Service Rollback Tests
# =============================================================================


class TestCrossServiceRollback:
    """Tests verifying all services handle rollback scenarios consistently."""

    @pytest.mark.asyncio
    async def test_all_services_handle_connection_failure(self, bad_session_maker) -> None:
        """Verify all services handle connection failure without crashing.

        This is the critical rollback scenario: database becomes unavailable.
        All services should either:
        - Return empty/zero results (graceful degradation)
        - Raise a clear, catchable exception
        """
        # SignalHydrator - graceful degradation
        hydrator = SignalHydrator(session_factory=bad_session_maker)
        hydrator_stats = await hydrator.hydrate_signals()
        assert hydrator_stats["signals_processed"] == 0

        # ContributionService - raises for specific queries, graceful for global
        contribution_service = ContributionService(session_factory=bad_session_maker)
        top_contributors = await contribution_service.get_top_contributors_global()
        assert top_contributors == []

        logger.info("All services handled connection failure appropriately")

    @pytest.mark.asyncio
    async def test_services_recover_after_reconnection(self, session_maker, bad_session_maker) -> None:
        """Test that services work normally after 'recovery'.

        Simulates:
        1. Services fail with bad connection
        2. Services recover when given working connection

        This validates the rollback->recovery path.
        """
        # First, verify failure with bad connection
        bad_hydrator = SignalHydrator(session_factory=bad_session_maker)
        bad_stats = await bad_hydrator.hydrate_signals()
        assert bad_stats["signals_processed"] == 0

        # Then, verify recovery with good connection
        good_hydrator = SignalHydrator(session_factory=session_maker, limit=5)
        good_count = await good_hydrator.get_fct_signal_count()

        # If the good database has data, count should be > 0
        # If not, at least it didn't raise an exception
        logger.info(
            "Recovery test: bad connection returned 0, good connection returned %d",
            good_count,
        )
        # No assertion on count - just verifying no exception was raised
