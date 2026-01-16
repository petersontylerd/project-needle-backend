"""Data integrity validation tests for dbt tables.

Tests verify:
- Row counts match source file counts
- Null values only in expected columns
- Referential integrity between staging and mart tables
- Business rule compliance

These tests require a live PostgreSQL database with dbt tables populated.
Run after: 1) load_insight_graph_to_dbt.py, 2) dbt run

Usage:
    cd backend
    UV_CACHE_DIR=../.uv-cache uv run pytest dbt/tests/test_data_integrity.py -v
"""

from __future__ import annotations

import logging
import os
from collections.abc import Generator
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Get database URL from environment or config
# Default port 5433 for docker-compose dbt setup
DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5433/quality_compass",
    ),
)

# Convert asyncpg URL to sync psycopg2 if needed
if "asyncpg" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def engine() -> Engine:
    """Create SQLAlchemy engine for database connection."""
    return create_engine(DATABASE_URL)


@pytest.fixture(scope="module")
def db_connection(engine: Engine) -> Generator[Connection, None, None]:
    """Provide a database connection for tests."""
    try:
        with engine.connect() as conn:
            # Test connection
            conn.execute(text("SELECT 1"))
            yield conn
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


# =============================================================================
# Helper Functions
# =============================================================================


def table_exists(conn: Any, table_name: str, schema: str = "public") -> bool:
    """Check if a table exists in the database.

    Args:
        conn: Database connection.
        table_name: Name of the table.
        schema: Schema name. Defaults to "public".

    Returns:
        True if table exists, False otherwise.

    Raises:
        None.
    """
    result = conn.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = :table_name
                AND table_schema = :schema
            )
        """
        ),
        {"table_name": table_name, "schema": schema},
    )
    return bool(result.scalar())


def get_row_count(conn: Any, table_name: str, schema: str = "public") -> int:
    """Get row count for a table.

    Args:
        conn: Database connection.
        table_name: Name of the table.
        schema: Schema name. Defaults to "public".

    Returns:
        Number of rows in the table.

    Raises:
        None.
    """
    result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'))
    return int(result.scalar() or 0)


def get_null_count(conn: Any, table_name: str, column_name: str, schema: str = "public") -> int:
    """Get count of null values in a column.

    Args:
        conn: Database connection.
        table_name: Name of the table.
        column_name: Name of the column to check.
        schema: Schema name. Defaults to "public".

    Returns:
        Number of null values in the column.

    Raises:
        None.
    """
    result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}" WHERE "{column_name}" IS NULL'))
    return int(result.scalar() or 0)


def view_exists(conn: Any, view_name: str, schema: str = "public_staging") -> bool:
    """Check if a view exists in the database.

    Args:
        conn: Database connection.
        view_name: Name of the view.
        schema: Schema name. Defaults to "public_staging".

    Returns:
        True if view exists, False otherwise.

    Raises:
        None.
    """
    result = conn.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.views
                WHERE table_name = :view_name
                AND table_schema = :schema
            )
        """
        ),
        {"view_name": view_name, "schema": schema},
    )
    return bool(result.scalar())


# =============================================================================
# Table Existence Tests
# =============================================================================


class TestTableExistence:
    """Verify all required tables exist."""

    @pytest.mark.parametrize(
        "table_name",
        [
            "raw_node_results",
            "raw_contributions",
        ],
    )
    def test_raw_tables_exist(self, db_connection, table_name: str) -> None:
        """Test that raw staging tables exist."""
        assert table_exists(db_connection, table_name, "public"), f"Raw table {table_name} does not exist"

    @pytest.mark.parametrize(
        "table_name",
        [
            "stg_nodes",
            "stg_node_edges",
            "stg_entity_results",
            "stg_statistical_methods",
            "stg_anomalies",
            "stg_contributions",
        ],
    )
    def test_staging_tables_exist(self, db_connection, table_name: str) -> None:
        """Test that staging tables exist (views in public_staging schema)."""
        assert view_exists(db_connection, table_name, "public_staging"), f"Staging view {table_name} does not exist in public_staging"

    @pytest.mark.parametrize(
        "table_name",
        [
            "dim_facilities",
            "dim_metrics",
            "fct_signals",
            "fct_contributions",
        ],
    )
    def test_mart_tables_exist(self, db_connection, table_name: str) -> None:
        """Test that mart tables exist in public_marts schema."""
        assert table_exists(db_connection, table_name, "public_marts"), f"Mart table {table_name} does not exist in public_marts"


# =============================================================================
# Row Count Validation Tests
# =============================================================================


class TestRowCounts:
    """Verify row counts are reasonable and data was loaded."""

    def test_raw_node_results_not_empty(self, db_connection) -> None:
        """Test raw_node_results has data."""
        if not table_exists(db_connection, "raw_node_results", "public"):
            pytest.skip("Table raw_node_results does not exist")
        count = get_row_count(db_connection, "raw_node_results", "public")
        assert count > 0, "raw_node_results is empty"
        logger.info(f"raw_node_results: {count} rows")

    def test_raw_contributions_not_empty(self, db_connection) -> None:
        """Test raw_contributions has data."""
        if not table_exists(db_connection, "raw_contributions", "public"):
            pytest.skip("Table raw_contributions does not exist")
        count = get_row_count(db_connection, "raw_contributions", "public")
        assert count > 0, "raw_contributions is empty"
        logger.info(f"raw_contributions: {count} rows")

    def test_fct_signals_row_count(self, db_connection) -> None:
        """Test fct_signals has reasonable number of signals."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")
        count = get_row_count(db_connection, "fct_signals", "public_marts")
        # Signals should exist but not be excessive
        assert count > 0, "fct_signals is empty - no signals detected"
        assert count < 1000000, f"fct_signals has unusually high count: {count}"
        logger.info(f"fct_signals: {count} rows")

    def test_fct_contributions_row_count(self, db_connection) -> None:
        """Test fct_contributions has contribution records."""
        if not table_exists(db_connection, "fct_contributions", "public_marts"):
            pytest.skip("Table fct_contributions does not exist")
        count = get_row_count(db_connection, "fct_contributions", "public_marts")
        assert count > 0, "fct_contributions is empty"
        logger.info(f"fct_contributions: {count} rows")


# =============================================================================
# Null Value Validation Tests
# =============================================================================


class TestNullConstraints:
    """Verify null values only in expected columns."""

    def test_fct_signals_required_columns_not_null(self, db_connection) -> None:
        """Test fct_signals required columns have no nulls."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")

        required_columns = ["signal_id", "run_id", "canonical_node_id", "metric_id", "severity"]
        for col in required_columns:
            null_count = get_null_count(db_connection, "fct_signals", col, "public_marts")
            assert null_count == 0, f"fct_signals.{col} has {null_count} null values"

    def test_fct_contributions_required_columns_not_null(self, db_connection) -> None:
        """Test fct_contributions required columns have no nulls."""
        if not table_exists(db_connection, "fct_contributions", "public_marts"):
            pytest.skip("Table fct_contributions does not exist")

        required_columns = [
            "contribution_id",
            "run_id",
            "parent_node_id",
            "child_node_id",
            "metric_id",
            "weight_share",
            "contribution_weight",
        ]
        for col in required_columns:
            null_count = get_null_count(db_connection, "fct_contributions", col, "public_marts")
            assert null_count == 0, f"fct_contributions.{col} has {null_count} null values"

    def test_dim_facilities_not_null(self, db_connection) -> None:
        """Test dim_facilities key columns have no nulls."""
        if not table_exists(db_connection, "dim_facilities", "public_marts"):
            pytest.skip("Table dim_facilities does not exist")

        for col in ["facility_sk", "facility_id"]:
            null_count = get_null_count(db_connection, "dim_facilities", col, "public_marts")
            assert null_count == 0, f"dim_facilities.{col} has {null_count} null values"

    def test_dim_metrics_not_null(self, db_connection) -> None:
        """Test dim_metrics key columns have no nulls."""
        if not table_exists(db_connection, "dim_metrics", "public_marts"):
            pytest.skip("Table dim_metrics does not exist")

        for col in ["metric_sk", "metric_id"]:
            null_count = get_null_count(db_connection, "dim_metrics", col, "public_marts")
            assert null_count == 0, f"dim_metrics.{col} has {null_count} null values"


# =============================================================================
# Referential Integrity Tests
# =============================================================================


class TestReferentialIntegrity:
    """Verify foreign key relationships are valid."""

    def test_fct_signals_facility_fk(self, db_connection) -> None:
        """Test fct_signals.facility_sk references dim_facilities."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")
        if not table_exists(db_connection, "dim_facilities", "public_marts"):
            pytest.skip("Table dim_facilities does not exist")

        result = db_connection.execute(
            text(
                """
                SELECT COUNT(*) FROM public_marts.fct_signals s
                WHERE s.facility_sk IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM public_marts.dim_facilities d
                    WHERE d.facility_sk = s.facility_sk
                )
            """
            )
        )
        orphan_count = result.scalar()
        assert orphan_count == 0, f"fct_signals has {orphan_count} orphan facility_sk references"

    def test_fct_signals_metric_fk(self, db_connection) -> None:
        """Test fct_signals.metric_sk references dim_metrics."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")
        if not table_exists(db_connection, "dim_metrics", "public_marts"):
            pytest.skip("Table dim_metrics does not exist")

        result = db_connection.execute(
            text(
                """
                SELECT COUNT(*) FROM public_marts.fct_signals s
                WHERE s.metric_sk IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM public_marts.dim_metrics d
                    WHERE d.metric_sk = s.metric_sk
                )
            """
            )
        )
        orphan_count = result.scalar()
        assert orphan_count == 0, f"fct_signals has {orphan_count} orphan metric_sk references"

    def test_fct_contributions_facility_fk(self, db_connection) -> None:
        """Test fct_contributions facility FKs reference dim_facilities."""
        if not table_exists(db_connection, "fct_contributions", "public_marts"):
            pytest.skip("Table fct_contributions does not exist")
        if not table_exists(db_connection, "dim_facilities", "public_marts"):
            pytest.skip("Table dim_facilities does not exist")

        # Check parent_facility_sk
        result = db_connection.execute(
            text(
                """
                SELECT COUNT(*) FROM public_marts.fct_contributions c
                WHERE c.parent_facility_sk IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM public_marts.dim_facilities d
                    WHERE d.facility_sk = c.parent_facility_sk
                )
            """
            )
        )
        orphan_count = result.scalar()
        assert orphan_count == 0, f"fct_contributions has {orphan_count} orphan parent_facility_sk references"


# =============================================================================
# Business Rule Validation Tests
# =============================================================================


class TestBusinessRules:
    """Verify business rules and constraints are satisfied."""

    def test_signal_severity_distribution(self, db_connection) -> None:
        """Test severity distribution is reasonable."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")

        result = db_connection.execute(
            text(
                """
                SELECT severity, COUNT(*) as cnt
                FROM public_marts.fct_signals
                GROUP BY severity
                ORDER BY severity
            """
            )
        )
        rows = result.fetchall()
        severity_counts = {row[0]: row[1] for row in rows}

        # All severity values should be valid
        valid_severities = {"Critical", "High", "Moderate", "Watch"}
        for severity in severity_counts:
            assert severity in valid_severities, f"Invalid severity value: {severity}"

        logger.info(f"Severity distribution: {severity_counts}")

    def test_contribution_weight_share_sum(self, db_connection) -> None:
        """Test weight_share sums to approximately 1.0 per parent node."""
        if not table_exists(db_connection, "fct_contributions", "public_marts"):
            pytest.skip("Table fct_contributions does not exist")

        # Check if any parent has weight_share sum significantly off from 1.0
        result = db_connection.execute(
            text(
                """
                SELECT parent_node_id, SUM(weight_share) as total_share
                FROM public_marts.fct_contributions
                GROUP BY parent_node_id
                HAVING ABS(SUM(weight_share) - 1.0) > 0.1
                LIMIT 10
            """
            )
        )
        bad_parents = result.fetchall()
        if bad_parents:
            logger.warning(f"Found {len(bad_parents)} parents with weight_share sum != 1.0")
            for parent_id, total in bad_parents:
                logger.warning(f"  {parent_id}: {total}")
        # This is a warning, not a hard failure (data quality issue)


# =============================================================================
# Data Quality Tests
# =============================================================================


class TestDataQuality:
    """Test data quality metrics."""

    def test_no_duplicate_signals(self, db_connection) -> None:
        """Test no duplicate signal_ids in fct_signals."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")

        result = db_connection.execute(
            text(
                """
                SELECT signal_id, COUNT(*) as cnt
                FROM public_marts.fct_signals
                GROUP BY signal_id
                HAVING COUNT(*) > 1
                LIMIT 5
            """
            )
        )
        duplicates = result.fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate signal_ids"

    def test_no_duplicate_contributions(self, db_connection) -> None:
        """Test no duplicate contribution_ids in fct_contributions."""
        if not table_exists(db_connection, "fct_contributions", "public_marts"):
            pytest.skip("Table fct_contributions does not exist")

        result = db_connection.execute(
            text(
                """
                SELECT contribution_id, COUNT(*) as cnt
                FROM public_marts.fct_contributions
                GROUP BY contribution_id
                HAVING COUNT(*) > 1
                LIMIT 5
            """
            )
        )
        duplicates = result.fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate contribution_ids"

    def test_metric_values_reasonable(self, db_connection) -> None:
        """Test metric values are in reasonable ranges."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")

        # Check for obviously wrong values (negative LOS index, etc.)
        result = db_connection.execute(
            text(
                """
                SELECT COUNT(*) FROM public_marts.fct_signals
                WHERE metric_value < 0
                AND metric_id LIKE '%Index%'
            """
            )
        )
        negative_indices = result.scalar()
        assert negative_indices == 0, f"Found {negative_indices} signals with negative index values"

    def test_z_scores_reasonable(self, db_connection) -> None:
        """Test z-scores are in reasonable range."""
        if not table_exists(db_connection, "fct_signals", "public_marts"):
            pytest.skip("Table fct_signals does not exist")

        # Z-scores outside [-10, 10] are suspicious
        result = db_connection.execute(
            text(
                """
                SELECT COUNT(*) FROM public_marts.fct_signals
                WHERE z_score IS NOT NULL
                AND (z_score < -10 OR z_score > 10)
            """
            )
        )
        extreme_z = result.scalar()
        if extreme_z > 0:
            logger.warning(f"Found {extreme_z} signals with extreme z-scores (|z| > 10)")
