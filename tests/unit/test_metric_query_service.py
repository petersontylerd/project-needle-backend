"""Tests for metric query service.

Tests the MetricQueryService which generates and executes SQL queries
for metrics based on semantic model definitions.
"""

import json
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.metric_query_service import (
    MetricQueryResult,
    MetricQueryService,
    get_metric_query_service,
)
from src.services.semantic_manifest_service import SemanticManifestService


@pytest.fixture
def sample_semantic_manifest() -> dict[str, Any]:
    """Sample semantic manifest data for testing."""
    return {
        "semantic_models": [
            {
                "name": "signals",
                "description": "Quality signals fact table.",
                "node_relation": {
                    "alias": "fct_signals",
                    "schema_name": "public_marts",
                    "database": "quality_compass",
                },
                "defaults": {"agg_time_dimension": "detected_at"},
                "entities": [
                    {"name": "signal", "type": "primary", "expr": "signal_id"},
                ],
                "dimensions": [
                    {"name": "severity", "type": "categorical", "expr": "severity"},
                    {
                        "name": "detected_at",
                        "type": "time",
                        "expr": "detected_at",
                        "type_params": {"time_granularity": "day"},
                    },
                    {"name": "domain", "type": "categorical", "expr": "domain"},
                    {"name": "facility_id", "type": "categorical", "expr": "facility_id"},
                ],
                "measures": [
                    {
                        "name": "signal_count",
                        "agg": "count",
                        "expr": "signal_id",
                        "description": "Total signal count",
                    },
                    {
                        "name": "z_score_avg",
                        "agg": "average",
                        "expr": "ABS(z_score)",
                        "description": "Average z-score",
                    },
                    {
                        "name": "unique_facilities",
                        "agg": "count_distinct",
                        "expr": "facility_id",
                        "description": "Unique facility count",
                    },
                ],
            },
        ],
        "metrics": [
            {
                "name": "total_signals",
                "description": "Total count of signals",
                "label": "Total Signals",
                "type": "simple",
                "type_params": {"measure": {"name": "signal_count"}},
                "config": {"meta": {"category": "Volume"}},
            },
            {
                "name": "critical_signals",
                "description": "Critical severity signals",
                "label": "Critical Signals",
                "type": "simple",
                "type_params": {"measure": {"name": "signal_count"}},
                "filter": {"where_filters": [{"where_sql_template": "{{ Dimension('signal__severity') }} = 'Critical'"}]},
                "config": {"meta": {"category": "Volume"}},
            },
            {
                "name": "efficiency_signals",
                "description": "Efficiency domain signals",
                "label": "Efficiency Signals",
                "type": "simple",
                "type_params": {"measure": {"name": "signal_count"}},
                "filter": {"where_filters": [{"where_sql_template": "{{ Dimension('signal__domain') }} = 'Efficiency'"}]},
                "config": {"meta": {"category": "Domain"}},
            },
            {
                "name": "avg_z_score",
                "description": "Average z-score magnitude",
                "label": "Avg Z-Score",
                "type": "simple",
                "type_params": {"measure": {"name": "z_score_avg"}},
                "config": {"meta": {"category": "Statistical"}},
            },
            {
                "name": "facility_count",
                "description": "Number of unique facilities",
                "label": "Facility Count",
                "type": "simple",
                "type_params": {"measure": {"name": "unique_facilities"}},
                "config": {"meta": {"category": "Coverage"}},
            },
            {
                "name": "critical_rate",
                "description": "Percentage of critical signals",
                "label": "Critical Rate",
                "type": "derived",
                "type_params": {
                    "expr": "critical_signals * 100.0 / NULLIF(total_signals, 0)",
                    "metrics": [{"name": "critical_signals"}, {"name": "total_signals"}],
                },
                "config": {"meta": {"category": "Ratio"}},
            },
        ],
    }


@pytest.fixture
def manifest_path(tmp_path: Path, sample_semantic_manifest: dict[str, Any]) -> Path:
    """Create a temporary semantic manifest file."""
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True)
    manifest_file = target_dir / "semantic_manifest.json"
    manifest_file.write_text(json.dumps(sample_semantic_manifest))
    return tmp_path


@pytest.fixture
def manifest_service(manifest_path: Path) -> SemanticManifestService:
    """Create SemanticManifestService with test manifest."""
    return SemanticManifestService(dbt_project_path=manifest_path)


@pytest.fixture
def query_service(manifest_service: SemanticManifestService) -> MetricQueryService:
    """Create MetricQueryService with test manifest service."""
    return MetricQueryService(manifest_service=manifest_service)


class TestMetricQueryServiceSqlGeneration:
    """Tests for SQL generation."""

    def test_generate_simple_metric_sql(self, query_service: MetricQueryService) -> None:
        """Test generating SQL for a simple metric without grouping."""
        sql = query_service.generate_metric_sql("total_signals")

        assert "SELECT" in sql
        assert "COUNT(signal_id) AS total_signals" in sql
        assert "FROM public_marts.fct_signals" in sql
        assert "GROUP BY" not in sql

    def test_generate_metric_sql_with_average(self, query_service: MetricQueryService) -> None:
        """Test generating SQL for an average metric."""
        sql = query_service.generate_metric_sql("avg_z_score")

        assert "AVG(ABS(z_score)) AS avg_z_score" in sql
        assert "FROM public_marts.fct_signals" in sql

    def test_generate_metric_sql_with_count_distinct(self, query_service: MetricQueryService) -> None:
        """Test generating SQL for a count_distinct metric."""
        sql = query_service.generate_metric_sql("facility_count")

        assert "COUNT(DISTINCT facility_id) AS facility_count" in sql
        assert "FROM public_marts.fct_signals" in sql

    def test_generate_metric_sql_with_filter(self, query_service: MetricQueryService) -> None:
        """Test generating SQL for a filtered metric."""
        sql = query_service.generate_metric_sql("critical_signals")

        assert "COUNT(signal_id) AS critical_signals" in sql
        assert "WHERE" in sql
        assert "severity = 'Critical'" in sql

    def test_generate_metric_sql_with_group_by(self, query_service: MetricQueryService) -> None:
        """Test generating SQL with GROUP BY dimensions."""
        sql = query_service.generate_metric_sql("total_signals", group_by=["severity"])

        assert "severity AS severity" in sql
        assert "COUNT(signal_id) AS total_signals" in sql
        assert "GROUP BY severity" in sql
        assert "ORDER BY severity" in sql

    def test_generate_metric_sql_with_multiple_dimensions(self, query_service: MetricQueryService) -> None:
        """Test generating SQL with multiple GROUP BY dimensions."""
        sql = query_service.generate_metric_sql("total_signals", group_by=["severity", "domain"])

        assert "severity AS severity" in sql
        assert "domain AS domain" in sql
        assert "GROUP BY severity, domain" in sql
        assert "ORDER BY severity" in sql

    def test_generate_metric_sql_with_where_clause(self, query_service: MetricQueryService) -> None:
        """Test generating SQL with additional WHERE filter."""
        sql = query_service.generate_metric_sql("total_signals", where="facility_id = '12345'")

        assert "WHERE" in sql
        assert "(facility_id = '12345')" in sql

    def test_generate_metric_sql_with_filter_and_where(self, query_service: MetricQueryService) -> None:
        """Test generating SQL with both metric filter and user WHERE."""
        sql = query_service.generate_metric_sql("critical_signals", where="facility_id = '12345'")

        assert "WHERE" in sql
        assert "severity = 'Critical'" in sql
        assert "(facility_id = '12345')" in sql
        assert "AND" in sql

    def test_generate_metric_sql_with_time_range(self, query_service: MetricQueryService) -> None:
        """Test generating SQL with time range filter."""
        sql = query_service.generate_metric_sql(
            "total_signals",
            time_range=(date(2024, 1, 1), date(2024, 12, 31)),
        )

        assert "WHERE" in sql
        assert "detected_at BETWEEN '2024-01-01' AND '2024-12-31'" in sql

    def test_generate_metric_sql_with_limit(self, query_service: MetricQueryService) -> None:
        """Test generating SQL with LIMIT clause."""
        sql = query_service.generate_metric_sql("total_signals", group_by=["severity"], limit=10)

        assert "LIMIT 10" in sql

    def test_generate_metric_sql_metric_not_found(self, query_service: MetricQueryService) -> None:
        """Test error when metric doesn't exist."""
        with pytest.raises(ValueError, match="Metric 'nonexistent' not found"):
            query_service.generate_metric_sql("nonexistent")

    def test_generate_metric_sql_derived_not_supported(self, query_service: MetricQueryService) -> None:
        """Test error when trying to query derived metric directly."""
        with pytest.raises(ValueError, match="Only simple metrics supported"):
            query_service.generate_metric_sql("critical_rate")


class TestMetricQueryServiceExecution:
    """Tests for query execution."""

    @pytest.mark.asyncio
    async def test_query_metric_executes_sql(self, query_service: MetricQueryService) -> None:
        """Test that query_metric executes generated SQL."""
        # Create mock database session
        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            MagicMock(_mapping={"total_signals": 100}),
        ]
        mock_db.execute.return_value = mock_result

        result = await query_service.query_metric(mock_db, "total_signals")

        # Verify SQL was executed
        mock_db.execute.assert_called_once()
        call_args = mock_db.execute.call_args[0][0]
        assert "COUNT(signal_id)" in str(call_args)

        # Verify result
        assert result.metric_name == "total_signals"
        assert result.row_count == 1
        assert result.rows == [{"total_signals": 100}]
        assert "COUNT(signal_id)" in result.sql

    @pytest.mark.asyncio
    async def test_query_metric_with_dimensions(self, query_service: MetricQueryService) -> None:
        """Test query_metric with GROUP BY dimensions."""
        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            MagicMock(_mapping={"severity": "Critical", "total_signals": 50}),
            MagicMock(_mapping={"severity": "High", "total_signals": 30}),
        ]
        mock_db.execute.return_value = mock_result

        result = await query_service.query_metric(mock_db, "total_signals", group_by=["severity"])

        assert result.dimensions == ["severity"]
        assert result.row_count == 2
        assert len(result.rows) == 2
        assert result.rows[0]["severity"] == "Critical"

    @pytest.mark.asyncio
    async def test_query_metric_aggregate(self, query_service: MetricQueryService) -> None:
        """Test query_metric_aggregate for single value."""
        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            MagicMock(_mapping={"total_signals": 500}),
        ]
        mock_db.execute.return_value = mock_result

        result = await query_service.query_metric_aggregate(mock_db, "total_signals")

        assert result is not None
        assert result["total_signals"] == 500

    @pytest.mark.asyncio
    async def test_query_metric_aggregate_no_data(self, query_service: MetricQueryService) -> None:
        """Test query_metric_aggregate when no data returned."""
        mock_db = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_db.execute.return_value = mock_result

        result = await query_service.query_metric_aggregate(mock_db, "total_signals")

        assert result is None


class TestDimensionFilterResolution:
    """Tests for MetricFlow dimension filter resolution."""

    def test_resolve_severity_dimension_filter(self, query_service: MetricQueryService) -> None:
        """Test resolving {{ Dimension('signal__severity') }} to column."""
        sql = query_service.generate_metric_sql("critical_signals")

        # The filter should resolve to actual column expression
        assert "severity = 'Critical'" in sql
        # Should not contain the MetricFlow template syntax
        assert "{{ Dimension" not in sql

    def test_resolve_domain_dimension_filter(self, query_service: MetricQueryService) -> None:
        """Test resolving {{ Dimension('signal__domain') }} to column."""
        sql = query_service.generate_metric_sql("efficiency_signals")

        assert "domain = 'Efficiency'" in sql
        assert "{{ Dimension" not in sql


class TestMetricQueryResult:
    """Tests for MetricQueryResult dataclass."""

    def test_metric_query_result_creation(self) -> None:
        """Test creating a MetricQueryResult."""
        result = MetricQueryResult(
            metric_name="total_signals",
            dimensions=["severity"],
            rows=[{"severity": "Critical", "total_signals": 50}],
            sql="SELECT ...",
            row_count=1,
        )

        assert result.metric_name == "total_signals"
        assert result.dimensions == ["severity"]
        assert result.row_count == 1
        assert len(result.rows) == 1


class TestMetricQueryServiceSingleton:
    """Tests for singleton pattern."""

    def test_get_metric_query_service_returns_singleton(self) -> None:
        """Test that get_metric_query_service returns same instance."""
        # Reset the singleton first
        import src.services.metric_query_service as module

        module._query_service = None

        service1 = get_metric_query_service()
        service2 = get_metric_query_service()
        assert service1 is service2
