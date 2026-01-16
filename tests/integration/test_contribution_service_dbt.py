"""Integration tests for ContributionService dbt functionality.

Tests verify that ContributionService correctly queries dbt mart tables
and returns contribution records matching the expected format.

These tests require a live PostgreSQL database with dbt tables populated.
Run after: 1) load_insight_graph_to_dbt.py, 2) dbt run

Usage:
    cd backend
    UV_CACHE_DIR=../.uv-cache uv run pytest tests/integration/test_contribution_service_dbt.py -v

Note: These tests use a session factory pointing to the dbt database (port 5433)
for the ContributionService to access the public_marts schema with fct_contributions table.
"""

from __future__ import annotations

import logging
import os

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.schemas.contribution import ContributionRecord
from src.services.contribution_service import ContributionService, ContributionServiceError

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

# Known run_id from test data
TEST_RUN_ID = "20251210170210"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def async_engine():
    """Create async SQLAlchemy engine for integration tests.

    Uses function scope to avoid event loop issues with pytest-asyncio.

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
            await session.execute(text("SELECT 1"))
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
        yield session


@pytest.fixture
def contribution_service(session_maker):
    """Create a ContributionService instance with dbt session factory.

    Args:
        session_maker: Session factory fixture pointing to dbt database.

    Returns:
        ContributionService: Service instance with injected session factory.
    """
    return ContributionService(session_factory=session_maker)


@pytest.fixture
def contribution_service_with_run_id(session_maker):
    """Create a ContributionService with a specific run_id.

    Args:
        session_maker: Session factory fixture pointing to dbt database.

    Returns:
        ContributionService: Service with run_id filter and injected session factory.
    """
    return ContributionService(run_id=TEST_RUN_ID, session_factory=session_maker)


# =============================================================================
# Table Availability Tests
# =============================================================================


class TestDbtTableAvailability:
    """Tests verifying dbt tables are accessible."""

    @pytest.mark.asyncio
    async def test_fct_contributions_table_accessible(self, db_session: AsyncSession) -> None:
        """Test that fct_contributions table exists and is queryable.

        Verifies the dbt mart table is available for ContributionService.
        """
        result = await db_session.execute(text("SELECT COUNT(*) FROM public_marts.fct_contributions"))
        count = result.scalar()
        assert count is not None, "fct_contributions table not accessible"
        assert count > 0, "fct_contributions table is empty"
        logger.info("fct_contributions has %d rows", count)


# =============================================================================
# Get Contributions for Parent Tests
# =============================================================================


class TestGetContributionsForParent:
    """Tests for get_contributions_for_parent method."""

    @pytest.mark.asyncio
    async def test_get_contributions_returns_list(self, db_session: AsyncSession, contribution_service: ContributionService) -> None:
        """Test that get_contributions_for_parent returns a list of records.

        First finds a parent node with contributions, then queries for it.
        """
        # Find a parent node that has contributions
        result = await db_session.execute(
            text("""
                SELECT parent_node_id, parent_facility_id
                FROM public_marts.fct_contributions
                LIMIT 1
            """)
        )
        row = result.fetchone()
        if row is None:
            pytest.skip("No contributions in database")

        parent_node_id = row[0]
        parent_facility_id = row[1]

        # Query contributions for this parent
        records = await contribution_service.get_contributions_for_parent(
            parent_node_id,
            parent_facility_id,  # Required for facility isolation
        )

        assert isinstance(records, list), "Should return a list"
        assert len(records) > 0, "Should return at least one contribution"
        assert all(isinstance(r, ContributionRecord) for r in records), "All items should be ContributionRecord"

    @pytest.mark.asyncio
    async def test_get_contributions_respects_top_n(self, db_session: AsyncSession, contribution_service: ContributionService) -> None:
        """Test that top_n parameter limits results.

        Verifies the limit is applied correctly.
        """
        # Find a parent node with multiple contributions
        result = await db_session.execute(
            text("""
                SELECT parent_node_id, parent_facility_id, COUNT(*) as cnt
                FROM public_marts.fct_contributions
                GROUP BY parent_node_id, parent_facility_id
                HAVING COUNT(*) > 3
                LIMIT 1
            """)
        )
        row = result.fetchone()
        if row is None:
            pytest.skip("No parent with > 3 contributions")

        parent_node_id = row[0]
        parent_facility_id = row[1]

        # Query with limit
        records = await contribution_service.get_contributions_for_parent(
            parent_node_id,
            parent_facility_id,  # Required for facility isolation
            top_n=3,
        )

        assert len(records) <= 3, f"Should return at most 3, got {len(records)}"

    @pytest.mark.asyncio
    async def test_get_contributions_nonexistent_parent(self, contribution_service: ContributionService) -> None:
        """Test that nonexistent parent returns empty list.

        Verifies graceful handling of missing data.
        """
        records = await contribution_service.get_contributions_for_parent(
            "nonexistent__parent__node__id",
            "NONEXISTENT",  # Required for facility isolation
        )

        assert isinstance(records, list), "Should return a list"
        assert len(records) == 0, "Should return empty list for nonexistent parent"


# =============================================================================
# Get Top Contributors Global Tests
# =============================================================================


class TestGetTopContributorsGlobal:
    """Tests for get_top_contributors_global method."""

    @pytest.mark.asyncio
    async def test_get_top_contributors_returns_records(self, contribution_service: ContributionService) -> None:
        """Test that get_top_contributors_global returns contributor records.

        Verifies the method returns valid contribution records.
        """
        records = await contribution_service.get_top_contributors_global(top_n=5)

        assert isinstance(records, list), "Should return a list"
        assert len(records) > 0, "Should return at least one contributor"
        assert all(isinstance(r, ContributionRecord) for r in records), "All items should be ContributionRecord"

    @pytest.mark.asyncio
    async def test_get_top_contributors_respects_limit(self, contribution_service: ContributionService) -> None:
        """Test that top_n parameter limits results.

        Verifies the limit is applied correctly.
        """
        records = await contribution_service.get_top_contributors_global(top_n=3)

        assert len(records) <= 3, f"Should return at most 3, got {len(records)}"


# =============================================================================
# ContributionRecord Field Tests
# =============================================================================


class TestContributionRecordFields:
    """Tests for ContributionRecord field population."""

    @pytest.mark.asyncio
    async def test_record_has_expected_fields(self, db_session: AsyncSession, contribution_service: ContributionService) -> None:
        """Test that ContributionRecord has all expected fields populated.

        Verifies the data mapping is correct.
        """
        # Find a parent node with contributions
        result = await db_session.execute(
            text("""
                SELECT parent_node_id, parent_facility_id
                FROM public_marts.fct_contributions
                LIMIT 1
            """)
        )
        row = result.fetchone()
        if row is None:
            pytest.skip("No contributions in database")

        parent_node_id = row[0]
        parent_facility_id = row[1]
        records = await contribution_service.get_contributions_for_parent(
            parent_node_id,
            parent_facility_id,  # Required for facility isolation
            top_n=1,
        )

        if len(records) == 0:
            pytest.skip("No contributions found")

        record = records[0]

        # Check required ContributionRecord fields are populated
        assert record.method is not None, "method should be populated"
        assert record.weight_field is not None, "weight_field should be populated"
        assert record.parent_node_id is not None, "parent_node_id should be populated"
        assert record.parent_entity is not None, "parent_entity should be populated"

    @pytest.mark.asyncio
    async def test_excess_over_parent_values(self, contribution_service: ContributionService) -> None:
        """Test that excess_over_parent field has valid numeric values.

        Verifies data quality of the contribution calculation.
        """
        records = await contribution_service.get_top_contributors_global(top_n=20)

        for record in records:
            if record.excess_over_parent is not None:
                assert isinstance(record.excess_over_parent, float), f"excess_over_parent should be float: {type(record.excess_over_parent)}"


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling in ContributionService."""

    @pytest.mark.asyncio
    async def test_service_handles_connection_errors_gracefully(self) -> None:
        """Test that service raises ContributionServiceError on connection failure.

        Verifies proper exception propagation.
        """
        # Create service with invalid connection
        bad_engine = create_async_engine(
            "postgresql+asyncpg://invalid:invalid@localhost:9999/invalid",
            echo=False,
        )
        bad_session_maker = async_sessionmaker(bad_engine, class_=AsyncSession, expire_on_commit=False)
        service = ContributionService(session_factory=bad_session_maker)

        with pytest.raises(ContributionServiceError):
            await service.get_contributions_for_parent(
                "any_parent_id",
                "ANY_FACILITY",  # Required for facility isolation
            )
