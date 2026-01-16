"""Unit tests for RunDiscoveryService."""

from pathlib import Path

import pytest

from src.runs.services.run_discovery import RunDiscoveryService


class TestRunDiscoveryService:
    """Tests for RunDiscoveryService."""

    @pytest.fixture
    def temp_runs_root(self, tmp_path: Path) -> Path:
        """Create temporary runs directory structure."""
        # Create graph with runs
        graph_dir = tmp_path / "test_graph"
        graph_dir.mkdir(parents=True)

        # Create run directories with results
        run1 = graph_dir / "20260101120000"
        run1.mkdir()
        (run1 / "results").mkdir()
        (run1 / "results" / "index.json").write_text('{"run_id": "20260101120000"}')

        run2 = graph_dir / "20260102120000"
        run2.mkdir()
        (run2 / "results").mkdir()
        (run2 / "results" / "index.json").write_text('{"run_id": "20260102120000"}')

        return tmp_path

    def test_discover_graphs(self, temp_runs_root: Path) -> None:
        """Test discovering graphs from filesystem."""
        service = RunDiscoveryService(runs_root=temp_runs_root)
        graphs = service.discover_graphs()

        assert len(graphs) == 1
        assert graphs[0].graph_name == "test_graph"
        assert graphs[0].run_count == 2
        assert graphs[0].latest_run_id == "20260102120000"

    def test_discover_graphs_empty(self, tmp_path: Path) -> None:
        """Test discovering graphs when runs_root exists but is empty."""
        # Empty runs root - no graphs yet
        service = RunDiscoveryService(runs_root=tmp_path)
        graphs = service.discover_graphs()

        assert len(graphs) == 0

    def test_list_runs_for_graph(self, temp_runs_root: Path) -> None:
        """Test listing runs for a specific graph."""
        service = RunDiscoveryService(runs_root=temp_runs_root)
        runs = service.list_runs_for_graph("test_graph")

        assert len(runs) == 2
        # Runs should be sorted newest first
        assert runs[0].run_id == "20260102120000"
        assert runs[1].run_id == "20260101120000"

    def test_list_runs_for_nonexistent_graph(self, temp_runs_root: Path) -> None:
        """Test listing runs for graph that doesn't exist."""
        service = RunDiscoveryService(runs_root=temp_runs_root)
        runs = service.list_runs_for_graph("nonexistent")

        assert len(runs) == 0

    def test_malformed_index_json(self, tmp_path: Path) -> None:
        """Test handling of malformed index.json."""
        graph_dir = tmp_path / "broken_graph"
        run_dir = graph_dir / "20260101120000"
        (run_dir / "results").mkdir(parents=True)
        (run_dir / "results" / "index.json").write_text("not valid json{")

        service = RunDiscoveryService(runs_root=tmp_path)
        runs = service.list_runs_for_graph("broken_graph")

        # Should return run with defaults, not crash
        assert len(runs) == 1
        assert runs[0].node_count == 0

    def test_node_count_populated(self, temp_runs_root: Path) -> None:
        """Test node_count is correctly read from index.json."""
        # Update fixture with nodes array
        index_path = temp_runs_root / "test_graph" / "20260101120000" / "results" / "index.json"
        index_path.write_text('{"nodes": ["a", "b", "c"]}')

        service = RunDiscoveryService(runs_root=temp_runs_root)
        runs = service.list_runs_for_graph("test_graph")

        run = next(r for r in runs if r.run_id == "20260101120000")
        assert run.node_count == 3

    def test_discover_graphs_nonexistent_root(self, tmp_path: Path) -> None:
        """Test discover_graphs with nonexistent runs_root directory."""
        nonexistent_path = tmp_path / "nonexistent"
        service = RunDiscoveryService(runs_root=nonexistent_path)
        # runs_root doesn't exist
        graphs = service.discover_graphs()
        assert len(graphs) == 0

    def test_path_traversal_rejected(self, temp_runs_root: Path) -> None:
        """Test that path traversal attempts are rejected."""
        service = RunDiscoveryService(runs_root=temp_runs_root)

        # These should all return empty, not traverse filesystem
        assert service.list_runs_for_graph("../../../etc") == []
        assert service.list_runs_for_graph("test_graph/../../../etc") == []
        assert service.list_runs_for_graph("..") == []
