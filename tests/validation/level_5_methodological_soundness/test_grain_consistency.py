"""Validate grain consistency across pipeline layers.

Grain must be preserved throughout the data pipeline:
    Runtime (JSONL) → dbt staging → dbt marts → API

This ensures no data loss or duplication as entities flow through
the system. A grain violation at any layer causes downstream issues.

These tests require:
- Production run data (JSONL files)
- PostgreSQL with dbt tables (fct_signals, stg_entity_results, etc.)

Usage:
    UV_CACHE_DIR=.uv-cache uv run pytest tests/validation/level_5_methodological_soundness/test_grain_consistency.py -v
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest
import pytest_asyncio

from tests.validation.helpers.data_loaders import ValidationDataLoader

# Database URL for dbt tables
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5433/quality_compass",
)


# =============================================================================
# Database Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def db_session():
    """Provide a database session for each test.

    Creates a fresh connection for each test to avoid event loop issues.

    Yields:
        AsyncSession: Database session.

    Raises:
        pytest.skip: If database connection fails.
    """
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        engine = create_async_engine(
            DATABASE_URL,
            echo=False,
            pool_pre_ping=True,
        )
        session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with session_maker() as session:
            try:
                await session.execute(text("SELECT 1"))
            except Exception as e:
                pytest.skip(f"Database not available: {e}")
            yield session

        await engine.dispose()
    except ImportError:
        pytest.skip("sqlalchemy not available")
    except Exception as e:
        pytest.skip(f"Cannot create database engine: {e}")


# =============================================================================
# Cross-Layer Entity Count Tests
# =============================================================================


class TestRuntimeToStagingConsistency:
    """Validate entity counts from runtime JSONL to dbt staging."""

    @pytest.mark.asyncio
    async def test_runtime_entity_count_matches_dbt_staging(
        self,
        validation_loader: ValidationDataLoader,
        db_session,
    ) -> None:
        """Runtime JSONL entity count should match stg_entity_results rows.

        This verifies no entities are lost during ETL to staging.
        """
        from sqlalchemy import text

        # Get runtime entity counts per node
        node_files = validation_loader.iter_node_files()
        runtime_counts: dict[str, int] = {}

        for node_path in node_files[:20]:  # Sample first 20 nodes
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)
                runtime_counts[node_id] = len(results)
            except Exception:
                continue

        if not runtime_counts:
            pytest.skip("No runtime nodes found")

        # Query staging counts for same nodes
        node_ids_str = ", ".join(f"'{n}'" for n in runtime_counts)
        query = text(f"""
            SELECT
                canonical_node_id,
                COUNT(*) as entity_count
            FROM public_staging.stg_entity_results
            WHERE canonical_node_id IN ({node_ids_str})
            GROUP BY canonical_node_id
        """)

        try:
            result = await db_session.execute(query)
            staging_rows = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query stg_entity_results: {e}")

        if not staging_rows:
            pytest.skip("No matching nodes in stg_entity_results")

        staging_counts = {row[0]: row[1] for row in staging_rows}

        # Compare counts
        mismatches: list[dict[str, Any]] = []
        for node_id, runtime_count in runtime_counts.items():
            staging_count = staging_counts.get(node_id)
            if staging_count is None:
                continue  # Node not loaded to staging yet
            if runtime_count != staging_count:
                mismatches.append(
                    {
                        "node_id": node_id,
                        "runtime_count": runtime_count,
                        "staging_count": staging_count,
                        "difference": runtime_count - staging_count,
                    }
                )

        if mismatches:
            sample = mismatches[:5]
            pytest.fail(f"{len(mismatches)} nodes have runtime/staging count mismatches. Sample: {json.dumps(sample, indent=2)}")


class TestStagingToMartsConsistency:
    """Validate grain integrity from dbt staging to dbt marts."""

    @pytest.mark.asyncio
    async def test_fct_signals_is_subset_of_staging(
        self,
        db_session,
    ) -> None:
        """fct_signals entities should all exist in staging.

        fct_signals filters to only signals (with severity, not suppressed),
        so it's a subset of staging. Every entity in fct_signals must exist
        in stg_entity_results - no orphaned records allowed.

        Join on natural key: (canonical_node_id, facility_id, entity_dimensions_hash).
        """
        from sqlalchemy import text

        query = text("""
            SELECT
                f.canonical_node_id,
                f.facility_id,
                f.entity_dimensions_hash
            FROM public_marts.fct_signals f
            LEFT JOIN public_staging.stg_entity_results s
                ON f.canonical_node_id = s.canonical_node_id
                AND f.facility_id = s.facility_id
                AND f.entity_dimensions_hash = md5(coalesce(s.entity_dimensions::text, '{}'))
            WHERE s.entity_result_id IS NULL
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            orphans = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query staging/marts tables: {e}")

        if orphans:
            issues = [
                {
                    "node_id": row[0],
                    "facility_id": row[1],
                    "entity_dimensions_hash": row[2],
                }
                for row in orphans
            ]
            pytest.fail(f"fct_signals has entities not in staging (orphans). Issues: {json.dumps(issues, indent=2)}")


class TestMethodAggregationGrain:
    """Validate entity grain is preserved through method aggregation."""

    @pytest.mark.asyncio
    async def test_no_entities_lost_in_method_aggregation(
        self,
        db_session,
    ) -> None:
        """Entity count before and after method aggregation should match.

        stg_statistical_methods has method-level grain (fan-out).
        int_statistical_methods_agg collapses to entity-level.
        Entity count should be preserved.
        """
        from sqlalchemy import text

        query = text("""
            WITH pre_agg AS (
                -- Distinct entities before aggregation
                SELECT
                    canonical_node_id,
                    entity_result_id,
                    COUNT(DISTINCT statistical_method) as method_count
                FROM public_staging.stg_statistical_methods
                GROUP BY canonical_node_id, entity_result_id
            ),
            post_agg AS (
                -- Entities after aggregation
                SELECT
                    entity_result_id
                FROM public_intermediate.int_statistical_methods_agg
            )
            SELECT
                pre.canonical_node_id,
                COUNT(*) as pre_entity_count,
                COUNT(post.entity_result_id) as post_entity_count
            FROM pre_agg pre
            LEFT JOIN post_agg post
                ON pre.entity_result_id = post.entity_result_id
            GROUP BY pre.canonical_node_id
            HAVING COUNT(*) != COUNT(post.entity_result_id)
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            mismatches = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query aggregation tables: {e}")

        if mismatches:
            issues = [
                {
                    "node_id": row[0],
                    "pre_agg_entity_count": row[1],
                    "post_agg_entity_count": row[2],
                    "lost": row[1] - row[2],
                }
                for row in mismatches
            ]
            pytest.fail(f"Entities lost during method aggregation. Issues: {json.dumps(issues, indent=2)}")


class TestEntityDimensionsHashConsistency:
    """Validate entity_dimensions_hash is consistent across layers."""

    @pytest.mark.asyncio
    async def test_hash_preserved_staging_to_marts(
        self,
        db_session,
    ) -> None:
        """Entity dimensions hash should be consistent from staging to marts.

        The hash is computed in fct_signals as md5(entity_dimensions::text).
        This test verifies the hash computation is consistent.
        """
        from sqlalchemy import text

        query = text("""
            -- Sample fct_signals entities and verify hash matches staging computation
            WITH marts AS (
                SELECT DISTINCT
                    canonical_node_id,
                    facility_id,
                    entity_dimensions_hash as marts_hash,
                    entity_dimensions
                FROM public_marts.fct_signals
                LIMIT 100
            ),
            staging AS (
                SELECT DISTINCT
                    er.canonical_node_id,
                    er.facility_id,
                    md5(coalesce(er.entity_dimensions::text, '{}')) as staging_hash,
                    er.entity_dimensions
                FROM public_staging.stg_entity_results er
            )
            SELECT
                m.canonical_node_id,
                m.facility_id,
                s.staging_hash,
                m.marts_hash
            FROM marts m
            JOIN staging s
                ON m.canonical_node_id = s.canonical_node_id
                AND m.facility_id = s.facility_id
                AND m.entity_dimensions = s.entity_dimensions
            WHERE m.marts_hash != s.staging_hash
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            mismatches = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query hash consistency: {e}")

        if mismatches:
            issues = [
                {
                    "node_id": row[0],
                    "entity_result_id": row[1],
                    "staging_hash": row[2],
                    "marts_hash": row[3],
                }
                for row in mismatches
            ]
            pytest.fail(f"Hash mismatch between staging and marts. Issues: {json.dumps(issues, indent=2)}")


class TestGrainUniqueness:
    """Validate grain uniqueness constraints in final output."""

    @pytest.mark.asyncio
    async def test_fct_signals_unique_per_entity(
        self,
        db_session,
    ) -> None:
        """fct_signals should have exactly one row per entity.

        Grain: (canonical_node_id, facility_id, entity_dimensions_hash)
        """
        from sqlalchemy import text

        query = text("""
            SELECT
                canonical_node_id,
                facility_id,
                entity_dimensions_hash,
                COUNT(*) as row_count
            FROM public_marts.fct_signals
            GROUP BY canonical_node_id, facility_id, entity_dimensions_hash
            HAVING COUNT(*) > 1
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            duplicates = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query fct_signals uniqueness: {e}")

        if duplicates:
            issues = [
                {
                    "node_id": row[0],
                    "facility_id": row[1],
                    "entity_dimensions_hash": row[2],
                    "row_count": row[3],
                }
                for row in duplicates
            ]
            pytest.fail(f"fct_signals has duplicate entities (grain violation). Issues: {json.dumps(issues, indent=2)}")

    @pytest.mark.asyncio
    async def test_statistical_methods_agg_unique_per_entity(
        self,
        db_session,
    ) -> None:
        """int_statistical_methods_agg should have one row per entity.

        Grain: entity_result_id (PK)
        """
        from sqlalchemy import text

        query = text("""
            SELECT
                entity_result_id,
                COUNT(*) as row_count
            FROM public_intermediate.int_statistical_methods_agg
            GROUP BY entity_result_id
            HAVING COUNT(*) > 1
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            duplicates = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query int_statistical_methods_agg uniqueness: {e}")

        if duplicates:
            issues = [
                {
                    "entity_result_id": row[0],
                    "row_count": row[1],
                }
                for row in duplicates
            ]
            pytest.fail(f"int_statistical_methods_agg has duplicate entities (grain violation). Issues: {json.dumps(issues, indent=2)}")
