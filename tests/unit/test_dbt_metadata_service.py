"""Tests for dbt metadata service.

Tests the DbtMetadataService which parses dbt artifacts (manifest.json, catalog.json)
and exposes model metadata, lineage, and documentation URLs.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from src.services.dbt_metadata_service import (
    DbtMetadataService,
    LineageGraph,
    ModelMetadata,
    get_dbt_metadata_service,
)


@pytest.fixture
def sample_manifest_data() -> dict[str, Any]:
    """Sample dbt manifest.json data."""
    return {
        "metadata": {
            "project_name": "quality_compass",
            "dbt_version": "1.9.0",
            "generated_at": "2025-01-01T00:00:00Z",
        },
        "nodes": {
            "model.quality_compass.fct_signals": {
                "name": "fct_signals",
                "resource_type": "model",
                "description": "Quality signals fact table containing anomaly detections",
                "schema": "marts",
                "database": "quality_compass",
                "config": {"materialized": "table"},
                "columns": {
                    "signal_id": {"description": "Primary key - unique signal identifier"},
                    "metric_id": {"description": "Foreign key to dim_metrics"},
                },
                "tags": ["marts"],
                "depends_on": {
                    "nodes": [
                        "model.quality_compass.stg_entity_results",
                        "model.quality_compass.stg_node_results",
                    ]
                },
            },
            "model.quality_compass.stg_entity_results": {
                "name": "stg_entity_results",
                "resource_type": "model",
                "description": "Staging entity results from raw data",
                "schema": "staging",
                "database": "quality_compass",
                "config": {"materialized": "view"},
                "columns": {},
                "tags": ["staging"],
                "depends_on": {"nodes": ["source.quality_compass.raw.entity_results"]},
            },
            "model.quality_compass.stg_node_results": {
                "name": "stg_node_results",
                "resource_type": "model",
                "description": "Staging node results from raw data",
                "schema": "staging",
                "database": "quality_compass",
                "config": {"materialized": "view"},
                "columns": {},
                "tags": ["staging"],
                "depends_on": {"nodes": ["source.quality_compass.raw.node_results"]},
            },
            "model.quality_compass.dim_facilities": {
                "name": "dim_facilities",
                "resource_type": "model",
                "description": "Facility dimension table",
                "schema": "marts",
                "database": "quality_compass",
                "config": {"materialized": "table"},
                "columns": {},
                "tags": ["marts"],
                "depends_on": {"nodes": ["model.quality_compass.fct_signals"]},
            },
        },
        "sources": {
            "source.quality_compass.raw.entity_results": {
                "name": "entity_results",
                "source_name": "raw",
                "resource_type": "source",
                "description": "Raw entity results",
            },
            "source.quality_compass.raw.node_results": {
                "name": "node_results",
                "source_name": "raw",
                "resource_type": "source",
                "description": "Raw node results",
            },
        },
        "metrics": [
            {"name": "total_signals", "description": "Count of signals"},
            {"name": "critical_signals", "description": "Count of critical signals"},
        ],
        "semantic_models": [
            {"name": "signals", "description": "Signals semantic model"},
        ],
    }


@pytest.fixture
def sample_catalog_data() -> dict[str, Any]:
    """Sample dbt catalog.json data."""
    return {
        "nodes": {
            "model.quality_compass.fct_signals": {
                "columns": {
                    "signal_id": {
                        "comment": "Primary key - unique signal identifier",
                        "type": "uuid",
                    },
                    "metric_id": {
                        "comment": "Foreign key to dim_metrics",
                        "type": "varchar",
                    },
                }
            }
        }
    }


@pytest.fixture
def dbt_project_with_manifest(tmp_path: Path, sample_manifest_data: dict[str, Any]) -> Path:
    """Create a temporary dbt project directory with manifest.json."""
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True)
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text(json.dumps(sample_manifest_data))
    return tmp_path


@pytest.fixture
def dbt_project_with_catalog(dbt_project_with_manifest: Path, sample_catalog_data: dict[str, Any]) -> Path:
    """Create a temporary dbt project directory with both manifest and catalog."""
    catalog_path = dbt_project_with_manifest / "target" / "catalog.json"
    catalog_path.write_text(json.dumps(sample_catalog_data))
    return dbt_project_with_manifest


class TestDbtMetadataServiceInit:
    """Tests for DbtMetadataService initialization."""

    def test_init_with_default_path(self) -> None:
        """Test that service initializes with default dbt project path."""
        service = DbtMetadataService()
        assert service._dbt_path is not None
        assert "dbt" in str(service._dbt_path)

    def test_init_with_custom_path(self, tmp_path: Path) -> None:
        """Test that service accepts custom dbt project path."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        assert service._dbt_path == tmp_path

    def test_manifest_path_property(self, tmp_path: Path) -> None:
        """Test manifest_path property returns correct path."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        expected = tmp_path / "target" / "manifest.json"
        assert service.manifest_path == expected

    def test_catalog_path_property(self, tmp_path: Path) -> None:
        """Test catalog_path property returns correct path."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        expected = tmp_path / "target" / "catalog.json"
        assert service.catalog_path == expected


class TestGetDocsUrl:
    """Tests for documentation URL generation."""

    def test_get_docs_url_for_model(self, tmp_path: Path) -> None:
        """Test URL generation for model resource type."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        url = service.get_docs_url("model", "fct_signals")
        assert "#!/model/model.quality_compass.fct_signals" in url

    def test_get_docs_url_for_source(self, tmp_path: Path) -> None:
        """Test URL generation for source resource type."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        url = service.get_docs_url("source", "entity_results", source_name="raw")
        assert "#!/source/source.quality_compass.raw.entity_results" in url

    def test_get_docs_url_for_seed(self, tmp_path: Path) -> None:
        """Test URL generation for seed resource type."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        url = service.get_docs_url("seed", "test_data")
        assert "#!/seed/seed.quality_compass.test_data" in url

    def test_get_docs_url_for_overview(self, tmp_path: Path) -> None:
        """Test URL generation for overview page."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        url = service.get_docs_url("overview", "")
        assert "#!/overview" in url

    def test_get_docs_url_unknown_type_defaults_to_overview(self, tmp_path: Path) -> None:
        """Test that unknown resource types default to overview URL."""
        service = DbtMetadataService(dbt_project_path=tmp_path)
        url = service.get_docs_url("unknown", "test")
        assert "#!/overview" in url


class TestGetSummary:
    """Tests for project summary retrieval."""

    def test_get_summary_returns_project_info(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_summary returns correct project information."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        summary = service.get_summary()

        assert summary["project_name"] == "quality_compass"
        assert summary["dbt_version"] == "1.9.0"
        assert summary["generated_at"] == "2025-01-01T00:00:00Z"

    def test_get_summary_returns_counts(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_summary returns correct counts."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        summary = service.get_summary()

        assert summary["model_count"] == 4
        assert summary["source_count"] == 2
        assert summary["metric_count"] == 2
        assert summary["semantic_model_count"] == 1

    def test_get_summary_includes_docs_base_url(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_summary includes docs base URL."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        summary = service.get_summary()

        assert "docs_base_url" in summary
        assert summary["docs_base_url"] is not None

    def test_get_summary_raises_when_manifest_missing(self, tmp_path: Path) -> None:
        """Test that get_summary raises FileNotFoundError when manifest missing."""
        service = DbtMetadataService(dbt_project_path=tmp_path)

        with pytest.raises(FileNotFoundError) as exc_info:
            service.get_summary()

        assert "manifest.json not found" in str(exc_info.value)


class TestGetAllModels:
    """Tests for retrieving all model metadata."""

    def test_get_all_models_returns_list(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_all_models returns list of ModelMetadata."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        models = service.get_all_models()

        assert len(models) == 4
        assert all(isinstance(m, ModelMetadata) for m in models)

    def test_get_all_models_includes_model_details(self, dbt_project_with_manifest: Path) -> None:
        """Test that models include all expected fields."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        models = service.get_all_models()

        fct_signals = next(m for m in models if m.name == "fct_signals")

        assert fct_signals.unique_id == "model.quality_compass.fct_signals"
        assert fct_signals.description == "Quality signals fact table containing anomaly detections"
        assert fct_signals.schema_name == "marts"
        assert fct_signals.materialization == "table"
        assert "marts" in fct_signals.tags

    def test_get_all_models_includes_dependencies(self, dbt_project_with_manifest: Path) -> None:
        """Test that models include dependency information."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        models = service.get_all_models()

        fct_signals = next(m for m in models if m.name == "fct_signals")

        assert len(fct_signals.depends_on) == 2
        assert "model.quality_compass.stg_entity_results" in fct_signals.depends_on
        assert "model.quality_compass.stg_node_results" in fct_signals.depends_on

    def test_get_all_models_includes_referenced_by(self, dbt_project_with_manifest: Path) -> None:
        """Test that models include downstream references."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        models = service.get_all_models()

        fct_signals = next(m for m in models if m.name == "fct_signals")

        assert len(fct_signals.referenced_by) == 1
        assert "model.quality_compass.dim_facilities" in fct_signals.referenced_by

    def test_get_all_models_includes_docs_url(self, dbt_project_with_manifest: Path) -> None:
        """Test that models include documentation URLs."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        models = service.get_all_models()

        fct_signals = next(m for m in models if m.name == "fct_signals")

        assert fct_signals.docs_url is not None
        assert "fct_signals" in fct_signals.docs_url

    def test_get_all_models_with_catalog_includes_data_types(self, dbt_project_with_catalog: Path) -> None:
        """Test that catalog data adds column data types."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_catalog)
        models = service.get_all_models()

        fct_signals = next(m for m in models if m.name == "fct_signals")

        signal_id_col = next(c for c in fct_signals.columns if c.name == "signal_id")
        assert signal_id_col.data_type == "uuid"


class TestGetModel:
    """Tests for retrieving a specific model."""

    def test_get_model_returns_model_metadata(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_model returns correct ModelMetadata."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        model = service.get_model("fct_signals")

        assert model is not None
        assert model.name == "fct_signals"
        assert isinstance(model, ModelMetadata)

    def test_get_model_returns_none_for_unknown(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_model returns None for unknown model."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        model = service.get_model("nonexistent_model")

        assert model is None


class TestGetLineage:
    """Tests for retrieving model lineage."""

    def test_get_lineage_returns_graph(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_lineage returns LineageGraph."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        lineage = service.get_lineage("fct_signals")

        assert lineage is not None
        assert isinstance(lineage, LineageGraph)
        assert lineage.target_model == "fct_signals"

    def test_get_lineage_includes_upstream(self, dbt_project_with_manifest: Path) -> None:
        """Test that lineage includes upstream dependencies."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        lineage = service.get_lineage("fct_signals")

        assert lineage is not None
        assert len(lineage.upstream) >= 2

        upstream_names = [n.name for n in lineage.upstream]
        assert "stg_entity_results" in upstream_names
        assert "stg_node_results" in upstream_names

    def test_get_lineage_includes_downstream(self, dbt_project_with_manifest: Path) -> None:
        """Test that lineage includes downstream dependents."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        lineage = service.get_lineage("fct_signals")

        assert lineage is not None
        assert len(lineage.downstream) >= 1

        downstream_names = [n.name for n in lineage.downstream]
        assert "dim_facilities" in downstream_names

    def test_get_lineage_nodes_have_layer(self, dbt_project_with_manifest: Path) -> None:
        """Test that lineage nodes have layer classification."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        lineage = service.get_lineage("fct_signals")

        assert lineage is not None

        stg_node = next(n for n in lineage.upstream if n.name == "stg_entity_results")
        assert stg_node.layer == "staging"

        dim_node = next(n for n in lineage.downstream if n.name == "dim_facilities")
        assert dim_node.layer == "marts"

    def test_get_lineage_nodes_have_docs_url(self, dbt_project_with_manifest: Path) -> None:
        """Test that lineage nodes have documentation URLs."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        lineage = service.get_lineage("fct_signals")

        assert lineage is not None

        for node in lineage.upstream + lineage.downstream:
            if node.resource_type == "model":
                assert node.docs_url is not None

    def test_get_lineage_returns_none_for_unknown(self, dbt_project_with_manifest: Path) -> None:
        """Test that get_lineage returns None for unknown model."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)
        lineage = service.get_lineage("nonexistent_model")

        assert lineage is None


class TestCacheRefresh:
    """Tests for cache refresh functionality."""

    def test_refresh_cache_clears_manifest(self, dbt_project_with_manifest: Path) -> None:
        """Test that refresh_cache clears cached manifest."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_manifest)

        # Load manifest into cache
        _ = service.get_summary()
        assert service._manifest is not None

        # Refresh cache
        service.refresh_cache()
        assert service._manifest is None

    def test_refresh_cache_clears_catalog(self, dbt_project_with_catalog: Path) -> None:
        """Test that refresh_cache clears cached catalog."""
        service = DbtMetadataService(dbt_project_path=dbt_project_with_catalog)

        # Load catalog into cache
        _ = service.get_all_models()
        assert service._catalog is not None

        # Refresh cache
        service.refresh_cache()
        assert service._catalog is None


class TestSingleton:
    """Tests for singleton pattern."""

    def test_get_dbt_metadata_service_returns_same_instance(self) -> None:
        """Test that get_dbt_metadata_service returns singleton."""
        # Reset singleton for test isolation
        import src.services.dbt_metadata_service as module

        module._metadata_service = None

        service1 = get_dbt_metadata_service()
        service2 = get_dbt_metadata_service()

        assert service1 is service2

        # Clean up
        module._metadata_service = None
