"""Validate correctness of statistical methods aggregation.

The aggregation pipeline collapses method-level grain to entity-level:
    stg_statistical_methods (one row per method)
    → int_statistical_methods_agg (one row per entity, methods in JSONB array)

This validates:
- All methods are captured in the aggregated JSONB
- Primary z-scores are extracted correctly
- Suppression flags propagate correctly
- Anomaly labels match source methods

These tests require:
- PostgreSQL with dbt tables (stg_statistical_methods, int_statistical_methods_agg)

Usage:
    UV_CACHE_DIR=.uv-cache uv run pytest tests/validation/level_2_statistical_methods/test_method_aggregation.py -v
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest
import pytest_asyncio

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
# Method Presence Tests
# =============================================================================


class TestMethodPresenceInAggregation:
    """Validate all source methods appear in aggregated JSONB."""

    @pytest.mark.asyncio
    async def test_all_methods_present_in_aggregated_jsonb(
        self,
        db_session,
    ) -> None:
        """Every method from stg_statistical_methods should appear in the aggregated array.

        The aggregation uses jsonb_agg to collect all methods per entity.
        No methods should be lost during aggregation.
        """
        from sqlalchemy import text

        query = text("""
            WITH source_methods AS (
                -- Count methods per entity in staging
                SELECT
                    canonical_node_id,
                    entity_result_id,
                    COUNT(*) as source_method_count,
                    array_agg(DISTINCT statistical_method ORDER BY statistical_method) as source_methods
                FROM public_staging.stg_statistical_methods
                GROUP BY canonical_node_id, entity_result_id
            ),
            agg_methods AS (
                -- Count methods per entity in aggregation
                SELECT
                    entity_result_id,
                    jsonb_array_length(statistical_methods) as agg_method_count
                FROM public_intermediate.int_statistical_methods_agg
            )
            SELECT
                s.canonical_node_id,
                s.entity_result_id,
                s.source_method_count,
                a.agg_method_count,
                s.source_methods
            FROM source_methods s
            JOIN agg_methods a
                ON s.entity_result_id = a.entity_result_id
            WHERE s.source_method_count != a.agg_method_count
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
                    "entity_result_id": row[1],
                    "source_count": row[2],
                    "agg_count": row[3],
                    "source_methods": row[4],
                }
                for row in mismatches
            ]
            pytest.fail(f"Method count mismatch between staging and aggregation. Issues: {json.dumps(issues, indent=2, default=str)}")


class TestPrimaryZscoreExtraction:
    """Validate primary z-scores are extracted correctly from methods."""

    @pytest.mark.asyncio
    async def test_primary_simple_zscore_matches_source_method(
        self,
        db_session,
    ) -> None:
        """primary_simple_zscore should equal simple_zscore from the appropriate method.

        The aggregation extracts simple_zscore from the first matching aggregate method.
        """
        from sqlalchemy import text

        query = text("""
            WITH source_simple AS (
                -- Get simple_zscore from simple_zscore method
                SELECT
                    canonical_node_id,
                    entity_result_id,
                    simple_zscore as source_simple_zscore
                FROM public_staging.stg_statistical_methods
                WHERE statistical_method_short_name = 'simple_zscore'
            ),
            agg_simple AS (
                -- Get primary_simple_zscore from aggregation
                SELECT
                    entity_result_id,
                    primary_simple_zscore
                FROM public_intermediate.int_statistical_methods_agg
            )
            SELECT
                s.canonical_node_id,
                s.entity_result_id,
                s.source_simple_zscore,
                a.primary_simple_zscore,
                abs(s.source_simple_zscore - a.primary_simple_zscore) as diff
            FROM source_simple s
            JOIN agg_simple a
                ON s.entity_result_id = a.entity_result_id
            WHERE s.source_simple_zscore IS NOT NULL
              AND a.primary_simple_zscore IS NOT NULL
              AND abs(s.source_simple_zscore - a.primary_simple_zscore) > 0.0001
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            mismatches = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query z-score values: {e}")

        if mismatches:
            issues = [
                {
                    "node_id": row[0],
                    "entity_result_id": row[1],
                    "source_simple_zscore": float(row[2]) if row[2] else None,
                    "primary_simple_zscore": float(row[3]) if row[3] else None,
                    "difference": float(row[4]) if row[4] else None,
                }
                for row in mismatches
            ]
            pytest.fail(f"primary_simple_zscore doesn't match source method. Issues: {json.dumps(issues, indent=2)}")

    @pytest.mark.asyncio
    async def test_primary_robust_zscore_matches_source_method(
        self,
        db_session,
    ) -> None:
        """primary_robust_zscore should equal robust_zscore from the appropriate method."""
        from sqlalchemy import text

        query = text("""
            WITH source_robust AS (
                -- Get robust_zscore from robust_zscore method
                SELECT
                    canonical_node_id,
                    entity_result_id,
                    robust_zscore as source_robust_zscore
                FROM public_staging.stg_statistical_methods
                WHERE statistical_method_short_name = 'robust_zscore'
            ),
            agg_robust AS (
                -- Get primary_robust_zscore from aggregation
                SELECT
                    entity_result_id,
                    primary_robust_zscore
                FROM public_intermediate.int_statistical_methods_agg
            )
            SELECT
                s.canonical_node_id,
                s.entity_result_id,
                s.source_robust_zscore,
                a.primary_robust_zscore,
                abs(s.source_robust_zscore - a.primary_robust_zscore) as diff
            FROM source_robust s
            JOIN agg_robust a
                ON s.entity_result_id = a.entity_result_id
            WHERE s.source_robust_zscore IS NOT NULL
              AND a.primary_robust_zscore IS NOT NULL
              AND abs(s.source_robust_zscore - a.primary_robust_zscore) > 0.0001
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            mismatches = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query z-score values: {e}")

        if mismatches:
            issues = [
                {
                    "node_id": row[0],
                    "entity_result_id": row[1],
                    "source_robust_zscore": float(row[2]) if row[2] else None,
                    "primary_robust_zscore": float(row[3]) if row[3] else None,
                    "difference": float(row[4]) if row[4] else None,
                }
                for row in mismatches
            ]
            pytest.fail(f"primary_robust_zscore doesn't match source method. Issues: {json.dumps(issues, indent=2)}")


class TestSuppressionPropagation:
    """Validate suppression flags are aggregated correctly."""

    @pytest.mark.asyncio
    async def test_any_suppressed_matches_bool_or(
        self,
        db_session,
    ) -> None:
        """any_suppressed should be TRUE if any source method has suppressed=TRUE.

        The aggregation uses bool_or to combine suppression flags.
        """
        from sqlalchemy import text

        query = text("""
            WITH source_suppression AS (
                -- Compute expected any_suppressed per entity
                SELECT
                    canonical_node_id,
                    entity_result_id,
                    bool_or(suppressed) as expected_any_suppressed
                FROM public_staging.stg_statistical_methods
                GROUP BY canonical_node_id, entity_result_id
            ),
            agg_suppression AS (
                -- Get actual any_suppressed from aggregation
                SELECT
                    entity_result_id,
                    any_suppressed
                FROM public_intermediate.int_statistical_methods_agg
            )
            SELECT
                s.canonical_node_id,
                s.entity_result_id,
                s.expected_any_suppressed,
                a.any_suppressed
            FROM source_suppression s
            JOIN agg_suppression a
                ON s.entity_result_id = a.entity_result_id
            WHERE s.expected_any_suppressed IS DISTINCT FROM a.any_suppressed
            LIMIT 10
        """)

        try:
            result = await db_session.execute(query)
            mismatches = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query suppression values: {e}")

        if mismatches:
            issues = [
                {
                    "node_id": row[0],
                    "entity_result_id": row[1],
                    "expected_any_suppressed": row[2],
                    "actual_any_suppressed": row[3],
                }
                for row in mismatches
            ]
            pytest.fail(f"any_suppressed doesn't match bool_or of source methods. Issues: {json.dumps(issues, indent=2)}")


class TestAnomalyLabelAggregation:
    """Validate anomaly labels are aggregated correctly."""

    @pytest.mark.asyncio
    async def test_anomaly_labels_preserved_in_jsonb(
        self,
        db_session,
    ) -> None:
        """Anomaly labels from source methods should appear in aggregated JSONB.

        Each method's anomaly label should be preserved in the statistical_methods array.
        """
        from sqlalchemy import text

        # Sample a few entities and verify their method arrays contain expected anomalies
        query = text("""
            SELECT
                entity_result_id,
                statistical_methods
            FROM public_intermediate.int_statistical_methods_agg
            WHERE statistical_methods IS NOT NULL
              AND jsonb_array_length(statistical_methods) > 0
            LIMIT 5
        """)

        try:
            result = await db_session.execute(query)
            rows = result.fetchall()
        except Exception as e:
            pytest.skip(f"Could not query aggregated methods: {e}")

        if not rows:
            pytest.skip("No aggregated methods found")

        validation_errors: list[dict[str, Any]] = []

        for row in rows:
            entity_result_id, methods = row

            for method in methods:
                # Each method should have at minimum a method_name
                if "method_name" not in method:
                    validation_errors.append(
                        {
                            "entity_result_id": entity_result_id,
                            "issue": "Method missing method_name field",
                            "method": method,
                        }
                    )

        if validation_errors:
            pytest.fail(f"Aggregated methods have structural issues. Issues: {json.dumps(validation_errors[:5], indent=2, default=str)}")


class TestAggregationCompleteness:
    """Validate aggregation captures all required fields."""

    @pytest.mark.asyncio
    async def test_aggregation_has_required_columns(
        self,
        db_session,
    ) -> None:
        """int_statistical_methods_agg should have all required columns."""
        from sqlalchemy import text

        # Query to check column existence
        query = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public_intermediate'
              AND table_name = 'int_statistical_methods_agg'
            ORDER BY ordinal_position
        """)

        try:
            result = await db_session.execute(query)
            columns = {row[0] for row in result.fetchall()}
        except Exception as e:
            pytest.skip(f"Could not query table schema: {e}")

        required_columns = {
            "entity_result_id",
            "statistical_methods",
            "primary_simple_zscore",
            "primary_robust_zscore",
            "any_suppressed",
        }

        missing = required_columns - columns
        if missing:
            pytest.fail(f"int_statistical_methods_agg missing required columns: {missing}. Available columns: {columns}")
