"""Unit tests for metadata bundle endpoints and helpers."""

from __future__ import annotations

import json
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from httpx import ASGITransport, AsyncClient

from src.main import app
from src.metadata.router import (
    _compute_taxonomy_hash,
    _load_edge_types_from_taxonomy,
    _load_metrics_from_taxonomy,
)


@pytest.fixture
def sample_taxonomy_dir(tmp_path: Path) -> Path:
    """Create a sample taxonomy directory with test data."""
    metrics_data = {
        "metadata": {"version": "1.0", "description": "Test metrics"},
        "entries": [
            {
                "metric_id": "test_metric",
                "display_name": "Test Metric",
                "description": "A test metric",
                "domain": "Testing",
                "polarity": "lower_is_better",
                "unit_type": "count",
                "display_format": "{:.0f}",
            },
        ],
    }

    edge_types_data = {
        "metadata": {"version": "1.0"},
        "entries": [
            {
                "id": "test_edge",
                "display_name": "Test Edge",
                "description": "A test edge",
                "ui_behavior": "slide_right",
                "direction": "forward",
                "category": "hierarchical",
            },
        ],
    }

    tag_types_data = {
        "metadata": {"version": "1.0"},
        "tags": [
            {
                "id": "test_tag",
                "display_name": "Test Tag",
                "category": "ui",
                "description": "A test tag",
                "ui_filter_ids": ["test_filter"],
            },
        ],
    }

    comparison_modes_data = {
        "metadata": {"version": "1.0"},
        "modes": [
            {
                "id": "delta",
                "display_name": "Delta",
                "description": "Compare to baseline",
                "baseline_description": "Baseline description",
                "authoring_hint": "Hint",
            },
        ],
    }

    group_by_types_data = {
        "metadata": {"version": "1.0"},
        "entries": [
            {
                "id": "facility",
                "display_name": "Facility",
                "category": "organization",
                "dataset_field": "facility_id",
                "description": "Facility dimension",
                "allowed_values": [
                    {"code": "A", "identifier": "facility_a", "label": "Facility A"},
                ],
            },
        ],
    }

    group_by_sets_data = {
        "metadata": {"version": "1.0"},
        "sets": [
            {
                "id": "default",
                "description": "Default set",
                "allowed_metric_method_sets": ["weighted_mean"],
                "allowed_dimensions": [["facility"]],
                "optional_time_dimensions": ["aggregate_time_period"],
            },
        ],
    }

    (tmp_path / "metrics.yaml").write_text(yaml.dump(metrics_data))
    (tmp_path / "edge_types.yaml").write_text(yaml.dump(edge_types_data))
    (tmp_path / "tag_types.yaml").write_text(yaml.dump(tag_types_data))
    (tmp_path / "comparison_modes.yaml").write_text(yaml.dump(comparison_modes_data))
    (tmp_path / "group_by_types.yaml").write_text(yaml.dump(group_by_types_data))
    (tmp_path / "group_by_sets.yaml").write_text(yaml.dump(group_by_sets_data))

    return tmp_path


class TestTaxonomyHashComputation:
    """Tests for _compute_taxonomy_hash helper."""

    def test_compute_hash_returns_16_chars(self, sample_taxonomy_dir: Path) -> None:
        """Hash should be 16 character hex string."""
        result = _compute_taxonomy_hash(sample_taxonomy_dir)
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)

    def test_hash_deterministic(self, sample_taxonomy_dir: Path) -> None:
        """Same taxonomy content should produce same hash."""
        hash1 = _compute_taxonomy_hash(sample_taxonomy_dir)
        hash2 = _compute_taxonomy_hash(sample_taxonomy_dir)
        assert hash1 == hash2

    def test_hash_changes_on_content_change(self, sample_taxonomy_dir: Path) -> None:
        """Hash should change when content changes."""
        hash1 = _compute_taxonomy_hash(sample_taxonomy_dir)

        # Modify content
        metrics_file = sample_taxonomy_dir / "metrics.yaml"
        content = yaml.safe_load(metrics_file.read_text())
        content["entries"][0]["description"] = "Modified description"
        metrics_file.write_text(yaml.dump(content))

        hash2 = _compute_taxonomy_hash(sample_taxonomy_dir)
        assert hash1 != hash2

    def test_empty_directory_returns_hash(self, tmp_path: Path) -> None:
        """Empty directory should still return valid hash."""
        result = _compute_taxonomy_hash(tmp_path)
        assert len(result) == 16


class TestLoadMetricsFromTaxonomy:
    """Tests for _load_metrics_from_taxonomy helper."""

    def test_load_metrics(self, sample_taxonomy_dir: Path) -> None:
        """Metrics should load correctly from taxonomy."""
        metrics = _load_metrics_from_taxonomy(sample_taxonomy_dir)

        assert len(metrics) == 1
        metric = metrics[0]
        assert metric.metric_id == "test_metric"
        assert metric.display_name == "Test Metric"
        assert metric.domain == "Testing"
        assert metric.polarity == "lower_is_better"

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        """Missing metrics.yaml should return empty list."""
        metrics = _load_metrics_from_taxonomy(tmp_path)
        assert metrics == []

    def test_invalid_format_returns_empty(self, tmp_path: Path) -> None:
        """Invalid YAML structure should return empty list."""
        (tmp_path / "metrics.yaml").write_text("not_a_dict: true")
        metrics = _load_metrics_from_taxonomy(tmp_path)
        assert metrics == []

    def test_metrics_sorted_by_id(self, tmp_path: Path) -> None:
        """Metrics should be sorted by metric_id."""
        data = {
            "entries": [
                {"metric_id": "z_metric", "display_name": "Z", "description": "Last"},
                {"metric_id": "a_metric", "display_name": "A", "description": "First"},
            ]
        }
        (tmp_path / "metrics.yaml").write_text(yaml.dump(data))

        metrics = _load_metrics_from_taxonomy(tmp_path)
        assert [m.metric_id for m in metrics] == ["a_metric", "z_metric"]


class TestLoadEdgeTypesFromTaxonomy:
    """Tests for _load_edge_types_from_taxonomy helper."""

    def test_load_edge_types(self, sample_taxonomy_dir: Path) -> None:
        """Edge types should load correctly from taxonomy."""
        edges = _load_edge_types_from_taxonomy(sample_taxonomy_dir)

        assert len(edges) == 1
        edge = edges[0]
        assert edge.id == "test_edge"
        assert edge.display_name == "Test Edge"
        assert edge.ui_behavior == "slide_right"

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        """Missing edge_types.yaml should return empty list."""
        edges = _load_edge_types_from_taxonomy(tmp_path)
        assert edges == []

    def test_edge_types_sorted_by_id(self, tmp_path: Path) -> None:
        """Edge types should be sorted by id."""
        data = {
            "entries": [
                {"id": "z_edge", "display_name": "Z", "description": "Last"},
                {"id": "a_edge", "display_name": "A", "description": "First"},
            ]
        }
        (tmp_path / "edge_types.yaml").write_text(yaml.dump(data))

        edges = _load_edge_types_from_taxonomy(tmp_path)
        assert [e.id for e in edges] == ["a_edge", "z_edge"]


# Get the actual module object for monkeypatching
_router_module = sys.modules["src.metadata.router"]


class TestMetadataBundleEndpoint:
    """Tests for GET /metadata/bundle endpoint."""

    @pytest.fixture
    async def client(self) -> AsyncGenerator[AsyncClient, None]:
        """Create async test client."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

    @pytest.mark.asyncio
    async def test_bundle_endpoint_with_valid_taxonomy(
        self,
        client: AsyncClient,
        sample_taxonomy_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Bundle endpoint should return valid response with taxonomy."""
        mock_settings = MagicMock()
        mock_settings.TAXONOMY_PATH = str(sample_taxonomy_dir)
        mock_settings.RUNS_ROOT = str(sample_taxonomy_dir / "runs")
        mock_settings.INSIGHT_GRAPH_RUN = "test_graph/20250101"
        monkeypatch.setattr(_router_module, "settings", mock_settings)

        response = await client.get("/api/metadata/bundle")

        assert response.status_code == 200
        data = response.json()

        assert data["version"] == "1.0.0"
        assert "generated_at" in data
        assert len(data["taxonomy_hash"]) == 16
        assert len(data["metrics"]) == 1
        assert len(data["edge_types"]) == 1
        assert len(data["tag_types"]) == 1
        assert len(data["comparison_modes"]) == 1
        assert len(data["group_by_types"]) == 1
        assert len(data["group_by_sets"]) == 1

    @pytest.mark.asyncio
    async def test_bundle_endpoint_prefers_run_bundle(
        self,
        client: AsyncClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Bundle endpoint should return runtime bundle when available."""
        runs_root = tmp_path / "runs"
        run_dir = runs_root / "test_graph" / "20250101"
        run_dir.mkdir(parents=True)

        bundle_payload = {
            "version": "1.0.0",
            "generated_at": "2025-01-01T00:00:00Z",
            "taxonomy_hash": "abcdef1234567890",
            "metrics": [
                {
                    "metric_id": "bundle_metric",
                    "display_name": "Bundle Metric",
                    "description": "Metric from bundle",
                    "domain": "Testing",
                    "polarity": "neutral",
                    "unit_type": "count",
                    "display_format": "{:.0f}",
                }
            ],
            "edge_types": [
                {
                    "id": "bundle_edge",
                    "display_name": "Bundle Edge",
                    "description": "Edge from bundle",
                    "ui_behavior": "expand",
                    "direction": "forward",
                    "category": "hierarchical",
                }
            ],
            "tag_types": [
                {
                    "id": "bundle_tag",
                    "display_name": "Bundle Tag",
                    "category": "ui",
                    "description": "Tag from bundle",
                    "ui_filter_ids": [],
                }
            ],
            "comparison_modes": [
                {
                    "id": "bundle_mode",
                    "display_name": "Bundle Mode",
                    "description": "Mode from bundle",
                    "baseline_description": "",
                    "authoring_hint": "",
                }
            ],
            "group_by_types": [
                {
                    "id": "bundle_group",
                    "display_name": "Bundle Group",
                    "category": "organization",
                    "dataset_field": "facility_id",
                    "description": "Group from bundle",
                    "allowed_values": [],
                }
            ],
            "group_by_sets": [
                {
                    "id": "bundle_set",
                    "description": "Set from bundle",
                    "allowed_metric_method_sets": [],
                    "allowed_dimensions": [],
                    "optional_time_dimensions": [],
                }
            ],
        }
        (run_dir / "metadata_bundle.json").write_text(json.dumps(bundle_payload))

        mock_settings = MagicMock()
        mock_settings.TAXONOMY_PATH = "/nonexistent/path"
        mock_settings.RUNS_ROOT = str(runs_root)
        mock_settings.INSIGHT_GRAPH_RUN = "test_graph/20250101"
        monkeypatch.setattr(_router_module, "settings", mock_settings)

        response = await client.get("/api/metadata/bundle")

        assert response.status_code == 200
        data = response.json()
        assert data["metrics"][0]["metric_id"] == "bundle_metric"
        assert data["edge_types"][0]["id"] == "bundle_edge"
        assert data["tag_types"][0]["id"] == "bundle_tag"
        assert data["comparison_modes"][0]["id"] == "bundle_mode"

    @pytest.mark.asyncio
    async def test_bundle_endpoint_missing_taxonomy(
        self,
        client: AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Bundle endpoint should return 404 when taxonomy is missing."""
        mock_settings = MagicMock()
        mock_settings.TAXONOMY_PATH = "/nonexistent/path"
        mock_settings.RUNS_ROOT = "/nonexistent/runs"
        mock_settings.INSIGHT_GRAPH_RUN = "test_graph/20250101"
        monkeypatch.setattr(_router_module, "settings", mock_settings)

        response = await client.get("/api/metadata/bundle")

        assert response.status_code == 404
        assert "Taxonomy directory not found" in response.json()["detail"]


class TestRunManifestEndpoint:
    """Tests for GET /metadata/run-manifest endpoint."""

    @pytest.fixture
    async def client(self) -> AsyncGenerator[AsyncClient, None]:
        """Create async test client."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

    @pytest.mark.asyncio
    async def test_run_manifest_basic(
        self,
        client: AsyncClient,
        sample_taxonomy_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run manifest endpoint should return valid response."""
        runs_dir = tmp_path / "runs"
        run_dir = runs_dir / "test_graph" / "20250101"
        run_dir.mkdir(parents=True)

        mock_settings = MagicMock()
        mock_settings.TAXONOMY_PATH = str(sample_taxonomy_dir)
        mock_settings.RUNS_ROOT = str(runs_dir)
        mock_settings.INSIGHT_GRAPH_RUN = "test_graph/20250101"
        monkeypatch.setattr(_router_module, "settings", mock_settings)

        response = await client.get("/api/metadata/run-manifest")

        assert response.status_code == 200
        data = response.json()

        assert data["run_id"] == "20250101"
        assert data["graph_name"] == "test_graph"
        assert data["metadata_bundle_version"] == "1.0.0"
        assert len(data["taxonomy_hash"]) == 16

    @pytest.mark.asyncio
    async def test_run_manifest_with_modeling(
        self,
        client: AsyncClient,
        sample_taxonomy_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run manifest should detect modeling artifacts."""
        runs_dir = tmp_path / "runs"
        run_dir = runs_dir / "test_graph" / "20250101"
        modeling_dir = run_dir / "modeling"
        modeling_dir.mkdir(parents=True)

        # Create modeling artifacts
        (modeling_dir / "run_summary.json").write_text("{}")
        (modeling_dir / "experiments.json").write_text("[]")

        mock_settings = MagicMock()
        mock_settings.TAXONOMY_PATH = str(sample_taxonomy_dir)
        mock_settings.RUNS_ROOT = str(runs_dir)
        mock_settings.INSIGHT_GRAPH_RUN = "test_graph/20250101"
        monkeypatch.setattr(_router_module, "settings", mock_settings)

        response = await client.get("/api/metadata/run-manifest")

        assert response.status_code == 200
        data = response.json()

        assert "run_summary_path" in data["modeling"]
        assert "experiments_manifest_path" in data["modeling"]

    @pytest.mark.asyncio
    async def test_run_manifest_missing_run_dir(
        self,
        client: AsyncClient,
        sample_taxonomy_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run manifest should handle missing run directory gracefully."""
        runs_dir = tmp_path / "runs"
        runs_dir.mkdir(parents=True)

        mock_settings = MagicMock()
        mock_settings.TAXONOMY_PATH = str(sample_taxonomy_dir)
        mock_settings.RUNS_ROOT = str(runs_dir)
        mock_settings.INSIGHT_GRAPH_RUN = "nonexistent/run"
        monkeypatch.setattr(_router_module, "settings", mock_settings)

        response = await client.get("/api/metadata/run-manifest")

        # Should still return 200 with "unknown" values
        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == "unknown"
        assert data["graph_name"] == "unknown"
