"""Tests for semantic manifest service.

Tests the SemanticManifestService which parses dbt semantic_manifest.json
and provides metric definitions and semantic model metadata.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from src.services.semantic_manifest_service import (
    DimensionDefinition,
    EntityDefinition,
    MeasureDefinition,
    MetricDefinition,
    SemanticManifestService,
    SemanticModelDefinition,
    get_semantic_manifest_service,
)


@pytest.fixture
def sample_semantic_manifest() -> dict[str, Any]:
    """Sample semantic manifest data for testing."""
    return {
        "semantic_models": [
            {
                "name": "signals",
                "description": "Quality signals detected by Project Needle analytics.",
                "node_relation": {
                    "alias": "fct_signals",
                    "schema_name": "public_marts",
                    "database": "quality_compass",
                },
                "defaults": {"agg_time_dimension": "detected_at"},
                "entities": [
                    {
                        "name": "signal",
                        "type": "primary",
                        "expr": "signal_id",
                        "description": "Unique signal identifier",
                    },
                    {
                        "name": "facility",
                        "type": "foreign",
                        "expr": "facility_sk",
                        "description": "Reference to facility dimension",
                    },
                ],
                "dimensions": [
                    {
                        "name": "severity",
                        "type": "categorical",
                        "expr": "severity",
                        "description": "Signal severity level",
                    },
                    {
                        "name": "detected_at",
                        "type": "time",
                        "expr": "detected_at",
                        "description": "Detection timestamp",
                        "type_params": {"time_granularity": "day"},
                    },
                    {
                        "name": "domain",
                        "type": "categorical",
                        "expr": "domain",
                        "description": "Quality domain",
                    },
                ],
                "measures": [
                    {
                        "name": "signal_count",
                        "agg": "count",
                        "expr": "signal_id",
                        "description": "Total signal count",
                        "create_metric": False,
                    },
                    {
                        "name": "z_score_magnitude_avg",
                        "agg": "average",
                        "expr": "ABS(z_score)",
                        "description": "Average z-score magnitude",
                        "create_metric": False,
                    },
                ],
            },
            {
                "name": "contributions",
                "description": "Contribution analysis data.",
                "node_relation": {
                    "alias": "fct_contributions",
                    "schema_name": "public_marts",
                    "database": "quality_compass",
                },
                "defaults": {"agg_time_dimension": "loaded_at"},
                "entities": [
                    {"name": "contribution", "type": "primary", "expr": "contribution_id"},
                ],
                "dimensions": [
                    {"name": "loaded_at", "type": "time", "expr": "loaded_at"},
                ],
                "measures": [
                    {"name": "contribution_count", "agg": "count", "expr": "contribution_id"},
                ],
            },
        ],
        "metrics": [
            {
                "name": "total_signals",
                "description": "Total count of quality signals detected",
                "label": "Total Signals",
                "type": "simple",
                "type_params": {"measure": {"name": "signal_count"}},
                "config": {"meta": {"category": "Volume", "display_format": "{:,.0f}"}},
            },
            {
                "name": "critical_signals",
                "description": "Count of critical severity signals",
                "label": "Critical Signals",
                "type": "simple",
                "type_params": {"measure": {"name": "signal_count"}},
                "filter": {"where_filters": [{"where_sql_template": "{{ Dimension('signal__severity') }} = 'Critical'"}]},
                "config": {"meta": {"category": "Volume"}},
            },
            {
                "name": "critical_signal_rate",
                "description": "Percentage of signals that are critical",
                "label": "Critical Signal Rate",
                "type": "derived",
                "type_params": {
                    "expr": "critical_signals * 100.0 / NULLIF(total_signals, 0)",
                    "metrics": [
                        {"name": "critical_signals"},
                        {"name": "total_signals"},
                    ],
                },
                "config": {"meta": {"category": "Ratio", "display_format": "{:.1f}%"}},
            },
            {
                "name": "avg_z_score_magnitude",
                "description": "Average z-score magnitude",
                "label": "Avg Z-Score",
                "type": "simple",
                "type_params": {"measure": {"name": "z_score_magnitude_avg"}},
                "config": {"meta": {"category": "Statistical"}},
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
def service(manifest_path: Path) -> SemanticManifestService:
    """Create SemanticManifestService with test manifest."""
    return SemanticManifestService(dbt_project_path=manifest_path)


class TestSemanticManifestService:
    """Tests for SemanticManifestService."""

    def test_manifest_not_found_raises_error(self, tmp_path: Path) -> None:
        """Test that missing manifest raises FileNotFoundError."""
        service = SemanticManifestService(dbt_project_path=tmp_path)
        with pytest.raises(FileNotFoundError, match="Semantic manifest not found"):
            service.get_all_metrics()

    def test_get_all_metrics_returns_all_metrics(self, service: SemanticManifestService) -> None:
        """Test getting all metric definitions."""
        metrics = service.get_all_metrics()
        assert len(metrics) == 4
        metric_names = {m.name for m in metrics}
        assert metric_names == {
            "total_signals",
            "critical_signals",
            "critical_signal_rate",
            "avg_z_score_magnitude",
        }

    def test_get_metric_returns_correct_metric(self, service: SemanticManifestService) -> None:
        """Test getting a specific metric by name."""
        metric = service.get_metric("total_signals")
        assert metric is not None
        assert metric.name == "total_signals"
        assert metric.label == "Total Signals"
        assert metric.type == "simple"
        assert metric.category == "Volume"
        assert metric.display_format == "{:,.0f}"
        assert metric.measure_reference == "signal_count"

    def test_get_metric_returns_none_for_unknown(self, service: SemanticManifestService) -> None:
        """Test that unknown metric returns None."""
        metric = service.get_metric("nonexistent_metric")
        assert metric is None

    def test_get_metric_with_filter(self, service: SemanticManifestService) -> None:
        """Test parsing metric with filter expression."""
        metric = service.get_metric("critical_signals")
        assert metric is not None
        assert metric.filter_expression is not None
        assert "signal__severity" in metric.filter_expression
        assert "Critical" in metric.filter_expression

    def test_get_derived_metric(self, service: SemanticManifestService) -> None:
        """Test parsing derived metric with expression."""
        metric = service.get_metric("critical_signal_rate")
        assert metric is not None
        assert metric.type == "derived"
        assert metric.derived_expr is not None
        assert "NULLIF" in metric.derived_expr
        assert len(metric.input_metrics) == 2
        assert "critical_signals" in metric.input_metrics
        assert "total_signals" in metric.input_metrics

    def test_get_metrics_by_category(self, service: SemanticManifestService) -> None:
        """Test filtering metrics by category."""
        volume_metrics = service.get_metrics_by_category("Volume")
        assert len(volume_metrics) == 2
        for m in volume_metrics:
            assert m.category == "Volume"

        ratio_metrics = service.get_metrics_by_category("Ratio")
        assert len(ratio_metrics) == 1
        assert ratio_metrics[0].name == "critical_signal_rate"

    def test_get_all_categories(self, service: SemanticManifestService) -> None:
        """Test getting all unique categories."""
        categories = service.get_all_categories()
        assert set(categories) == {"Volume", "Ratio", "Statistical"}
        # Should be sorted
        assert categories == sorted(categories)

    def test_get_all_semantic_models(self, service: SemanticManifestService) -> None:
        """Test getting all semantic model definitions."""
        models = service.get_all_semantic_models()
        assert len(models) == 2
        model_names = {m.name for m in models}
        assert model_names == {"signals", "contributions"}

    def test_get_semantic_model(self, service: SemanticManifestService) -> None:
        """Test getting a specific semantic model."""
        model = service.get_semantic_model("signals")
        assert model is not None
        assert model.name == "signals"
        assert model.table_name == "fct_signals"
        assert model.schema_name == "public_marts"
        assert model.database == "quality_compass"
        assert model.default_time_dimension == "detected_at"

    def test_semantic_model_entities(self, service: SemanticManifestService) -> None:
        """Test semantic model entity parsing."""
        model = service.get_semantic_model("signals")
        assert model is not None
        assert len(model.entities) == 2

        signal_entity = next((e for e in model.entities if e.name == "signal"), None)
        assert signal_entity is not None
        assert signal_entity.type == "primary"
        assert signal_entity.expr == "signal_id"

        facility_entity = next((e for e in model.entities if e.name == "facility"), None)
        assert facility_entity is not None
        assert facility_entity.type == "foreign"
        assert facility_entity.expr == "facility_sk"

    def test_semantic_model_dimensions(self, service: SemanticManifestService) -> None:
        """Test semantic model dimension parsing."""
        model = service.get_semantic_model("signals")
        assert model is not None
        assert len(model.dimensions) == 3

        severity_dim = next((d for d in model.dimensions if d.name == "severity"), None)
        assert severity_dim is not None
        assert severity_dim.type == "categorical"
        assert severity_dim.expr == "severity"

        time_dim = next((d for d in model.dimensions if d.name == "detected_at"), None)
        assert time_dim is not None
        assert time_dim.type == "time"
        assert time_dim.time_granularity == "day"

    def test_semantic_model_measures(self, service: SemanticManifestService) -> None:
        """Test semantic model measure parsing."""
        model = service.get_semantic_model("signals")
        assert model is not None
        assert len(model.measures) == 2

        count_measure = next((m for m in model.measures if m.name == "signal_count"), None)
        assert count_measure is not None
        assert count_measure.agg == "count"
        assert count_measure.expr == "signal_id"

        avg_measure = next((m for m in model.measures if m.name == "z_score_magnitude_avg"), None)
        assert avg_measure is not None
        assert avg_measure.agg == "average"
        assert avg_measure.expr == "ABS(z_score)"

    def test_get_available_dimensions(self, service: SemanticManifestService) -> None:
        """Test getting available dimensions for a metric."""
        dims = service.get_available_dimensions("total_signals")
        assert len(dims) == 3
        assert set(dims) == {"severity", "detected_at", "domain"}

    def test_get_available_dimensions_empty_for_derived(self, service: SemanticManifestService) -> None:
        """Test that derived metrics return empty dimensions list."""
        # Derived metrics don't have a direct measure reference
        dims = service.get_available_dimensions("critical_signal_rate")
        assert dims == []

    def test_find_measure_context(self, service: SemanticManifestService) -> None:
        """Test finding semantic model and measure for a measure name."""
        context = service.find_measure_context("signal_count")
        assert context is not None
        sm, measure = context
        assert sm.name == "signals"
        assert measure.name == "signal_count"
        assert measure.agg == "count"

    def test_find_measure_context_returns_none_for_unknown(self, service: SemanticManifestService) -> None:
        """Test that unknown measure returns None."""
        context = service.find_measure_context("nonexistent_measure")
        assert context is None

    def test_refresh_cache(self, service: SemanticManifestService) -> None:
        """Test that cache can be refreshed."""
        # First call loads the manifest
        metrics1 = service.get_all_metrics()
        assert len(metrics1) == 4

        # Refresh clears the cache
        service.refresh_cache()

        # Next call reloads
        metrics2 = service.get_all_metrics()
        assert len(metrics2) == 4


class TestSemanticManifestServiceSingleton:
    """Tests for singleton pattern."""

    def test_get_semantic_manifest_service_returns_singleton(self) -> None:
        """Test that get_semantic_manifest_service returns same instance."""
        # Reset the singleton first
        import src.services.semantic_manifest_service as module

        module._service = None

        service1 = get_semantic_manifest_service()
        service2 = get_semantic_manifest_service()
        assert service1 is service2


class TestDataclasses:
    """Tests for data classes."""

    def test_metric_definition_defaults(self) -> None:
        """Test MetricDefinition has correct defaults."""
        metric = MetricDefinition(
            name="test",
            description="Test metric",
            label="Test",
            type="simple",
        )
        assert metric.category is None
        assert metric.display_format is None
        assert metric.measure_reference is None
        assert metric.filter_expression is None
        assert metric.derived_expr is None
        assert metric.input_metrics == []

    def test_semantic_model_definition_defaults(self) -> None:
        """Test SemanticModelDefinition has correct defaults."""
        model = SemanticModelDefinition(
            name="test",
            description="Test model",
            table_name="test_table",
            schema_name="public",
            database="db",
        )
        assert model.entities == []
        assert model.dimensions == []
        assert model.measures == []
        assert model.default_time_dimension is None

    def test_measure_definition_defaults(self) -> None:
        """Test MeasureDefinition has correct defaults."""
        measure = MeasureDefinition(
            name="test",
            description="Test measure",
            agg="count",
            expr="id",
        )
        assert measure.create_metric is False

    def test_dimension_definition_defaults(self) -> None:
        """Test DimensionDefinition has correct defaults."""
        dim = DimensionDefinition(
            name="test",
            type="categorical",
            expr="test_col",
        )
        assert dim.description == ""
        assert dim.time_granularity is None

    def test_entity_definition_defaults(self) -> None:
        """Test EntityDefinition has correct defaults."""
        entity = EntityDefinition(
            name="test",
            type="primary",
            expr="test_id",
        )
        assert entity.description == ""
