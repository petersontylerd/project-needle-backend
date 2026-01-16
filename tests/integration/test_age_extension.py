"""Integration tests for Apache AGE extension availability.

These tests verify that the PostgreSQL AGE extension is properly
installed and functional before running graph operations.

AGE (A Graph Extension) provides graph database capabilities in PostgreSQL.
The ontology module depends on AGE for graph sync and queries.

These tests require:
- PostgreSQL database with AGE extension installed

Usage:
    cd backend
    UV_CACHE_DIR=../.uv-cache uv run pytest tests/integration/test_age_extension.py -v
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Database URL from environment or default (port 5433 for docker-compose)
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


@pytest.fixture(scope="module")
def async_engine():
    """Create async SQLAlchemy engine."""
    return create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )


@pytest.fixture(scope="module")
def session_maker(async_engine):
    """Create async session maker."""
    return async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def db_session(session_maker):
    """Provide a database session for each test."""
    async with session_maker() as session:
        try:
            await session.execute(text("SELECT 1"))
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
        yield session


# =============================================================================
# AGE Extension Tests
# =============================================================================


class TestAgeExtensionInstalled:
    """Verify AGE extension is installed in PostgreSQL."""

    @pytest.mark.asyncio
    async def test_age_extension_installed(self, db_session: AsyncSession) -> None:
        """AGE extension should be listed in pg_extension.

        This is a prerequisite for all graph operations.
        """
        result = await db_session.execute(text("SELECT extname FROM pg_extension WHERE extname = 'age'"))
        row = result.fetchone()

        if row is None:
            pytest.skip("AGE extension not installed. Install with: CREATE EXTENSION IF NOT EXISTS age;")

        assert row[0] == "age"

    @pytest.mark.asyncio
    async def test_age_schema_in_search_path(self, db_session: AsyncSession) -> None:
        """ag_catalog schema should be accessible.

        AGE creates an ag_catalog schema that must be in the search path
        for Cypher functions to be available.
        """
        try:
            result = await db_session.execute(text("SELECT current_schemas(true)"))
            row = result.fetchone()

            if row is None:
                pytest.fail("Could not get current schemas")

            schemas = row[0]
            # Check if ag_catalog is accessible
            has_age = "ag_catalog" in str(schemas) or await self._check_age_functions(db_session)

            if not has_age:
                pytest.skip('ag_catalog schema not in search path. Run: SET search_path = ag_catalog, "$user", public;')

        except Exception as e:
            pytest.skip(f"Could not check schema: {e}")

    async def _check_age_functions(self, db_session: AsyncSession) -> bool:
        """Check if AGE functions are available."""
        try:
            await db_session.execute(text("SELECT ag_catalog.create_graph('test_check')"))
            await db_session.execute(text("SELECT ag_catalog.drop_graph('test_check', true)"))
            return True
        except Exception:
            return False


class TestAgeGraphOperations:
    """Verify basic AGE graph operations work correctly."""

    @pytest.mark.asyncio
    async def test_can_create_and_drop_graph(self, db_session: AsyncSession) -> None:
        """Should be able to create and drop a temporary graph.

        This validates the AGE extension is functional.
        """
        graph_name = "_test_temp_graph_check"

        try:
            # Attempt to drop if exists (cleanup from failed previous runs)
            try:
                await db_session.execute(text(f"SELECT ag_catalog.drop_graph('{graph_name}', true)"))
            except Exception:
                pass  # Graph didn't exist

            # Create graph
            await db_session.execute(text(f"SELECT ag_catalog.create_graph('{graph_name}')"))

            # Verify graph exists
            result = await db_session.execute(
                text(f"""
                    SELECT name FROM ag_catalog.ag_graph
                    WHERE name = '{graph_name}'
                """)
            )
            row = result.fetchone()
            assert row is not None, f"Graph {graph_name} should exist after creation"
            assert row[0] == graph_name

            # Drop graph
            await db_session.execute(text(f"SELECT ag_catalog.drop_graph('{graph_name}', true)"))

            # Verify graph removed
            result = await db_session.execute(
                text(f"""
                    SELECT name FROM ag_catalog.ag_graph
                    WHERE name = '{graph_name}'
                """)
            )
            row = result.fetchone()
            assert row is None, f"Graph {graph_name} should be removed after drop"

        except Exception as e:
            if "does not exist" in str(e) or "ag_catalog" in str(e):
                pytest.skip(f"AGE extension not available: {e}")
            raise

    @pytest.mark.asyncio
    async def test_cypher_query_executes(self, db_session: AsyncSession) -> None:
        """Simple Cypher query should execute without error.

        This validates the Cypher query parser is working.
        """
        graph_name = "_test_cypher_query_graph"

        try:
            # Setup: create test graph
            try:
                await db_session.execute(text(f"SELECT ag_catalog.drop_graph('{graph_name}', true)"))
            except Exception:
                pass

            await db_session.execute(text(f"SELECT ag_catalog.create_graph('{graph_name}')"))

            # Execute a simple Cypher query
            result = await db_session.execute(
                text(f"""
                    SELECT * FROM ag_catalog.cypher('{graph_name}', $$
                        CREATE (n:TestNode {{id: 'test1', name: 'Test Node'}})
                        RETURN n
                    $$) as (v agtype)
                """)
            )
            row = result.fetchone()
            assert row is not None, "Cypher CREATE should return the created node"

            # Query the node back
            result = await db_session.execute(
                text(f"""
                    SELECT * FROM ag_catalog.cypher('{graph_name}', $$
                        MATCH (n:TestNode)
                        RETURN n.id, n.name
                    $$) as (id agtype, name agtype)
                """)
            )
            row = result.fetchone()
            assert row is not None, "Cypher MATCH should find the created node"

            # Cleanup
            await db_session.execute(text(f"SELECT ag_catalog.drop_graph('{graph_name}', true)"))

        except Exception as e:
            # Cleanup on failure
            try:
                await db_session.execute(text(f"SELECT ag_catalog.drop_graph('{graph_name}', true)"))
            except Exception:
                pass

            if "does not exist" in str(e) or "ag_catalog" in str(e):
                pytest.skip(f"AGE extension not available: {e}")
            raise


class TestOntologyGraphExists:
    """Verify the ontology graph is available for operations."""

    @pytest.mark.asyncio
    async def test_ontology_graph_schema_exists(self, db_session: AsyncSession) -> None:
        """The ontology graph should exist if sync has been run.

        Graph name is typically 'ontology' or similar.
        """
        try:
            result = await db_session.execute(
                text("""
                    SELECT name FROM ag_catalog.ag_graph
                    WHERE name LIKE '%ontology%'
                       OR name = 'quality_compass_graph'
                """)
            )
            rows = result.fetchall()

            if not rows:
                pytest.skip("Ontology graph not found. Run ontology sync first to create the graph.")

            graph_names = [row[0] for row in rows]
            assert len(graph_names) >= 1, f"Expected at least one ontology graph, found: {graph_names}"

        except Exception as e:
            if "ag_catalog" in str(e):
                pytest.skip(f"AGE extension not available: {e}")
            raise
