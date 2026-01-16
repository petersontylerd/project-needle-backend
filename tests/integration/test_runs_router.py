"""Tests for runs router endpoints."""

import pytest
from httpx import AsyncClient


class TestRunsRouterBasic:
    """Basic connectivity tests for runs router."""

    @pytest.mark.asyncio
    async def test_graphs_endpoint_exists(self, client: AsyncClient) -> None:
        """Test that /api/runs/graphs endpoint responds."""
        response = await client.get("/api/runs/graphs")
        # Should not return 404 (route exists)
        assert response.status_code != 404


class TestListGraphs:
    """Tests for GET /api/runs/graphs endpoint."""

    @pytest.mark.asyncio
    async def test_list_graphs_returns_list(self, client: AsyncClient) -> None:
        """Test that endpoint returns a list of graphs."""
        response = await client.get("/api/runs/graphs")
        assert response.status_code == 200
        data = response.json()
        assert "graphs" in data
        assert isinstance(data["graphs"], list)

    @pytest.mark.asyncio
    async def test_list_graphs_schema(self, client: AsyncClient) -> None:
        """Test that graph items have required fields."""
        response = await client.get("/api/runs/graphs")
        assert response.status_code == 200
        data = response.json()
        # If there are graphs, check schema
        for graph in data["graphs"]:
            assert "graph_name" in graph
            assert "run_count" in graph
            assert "latest_run_id" in graph
            assert "latest_run_timestamp" in graph


class TestListRuns:
    """Tests for GET /api/runs/graphs/{graph_name}/runs endpoint."""

    @pytest.mark.asyncio
    async def test_list_runs_returns_list(self, client: AsyncClient) -> None:
        """Test that endpoint returns a list of runs."""
        response = await client.get("/api/runs/graphs/test_minimal/runs")
        assert response.status_code == 200
        data = response.json()
        assert "runs" in data
        assert isinstance(data["runs"], list)

    @pytest.mark.asyncio
    async def test_list_runs_nonexistent_graph(self, client: AsyncClient) -> None:
        """Test that nonexistent graph returns empty list."""
        response = await client.get("/api/runs/graphs/nonexistent_graph/runs")
        assert response.status_code == 200
        data = response.json()
        assert data["runs"] == []

    @pytest.mark.asyncio
    async def test_list_runs_schema(self, client: AsyncClient) -> None:
        """Test that run items have required fields."""
        response = await client.get("/api/runs/graphs/test_minimal/runs")
        assert response.status_code == 200
        data = response.json()
        for run in data["runs"]:
            assert "run_id" in run
            assert "created_at" in run
            assert "node_count" in run


class TestGetRunMetadata:
    """Tests for GET /api/runs/graphs/{graph_name}/runs/{run_id} endpoint."""

    @pytest.mark.asyncio
    async def test_get_run_metadata_not_found(self, client: AsyncClient) -> None:
        """Test that nonexistent run returns 404."""
        response = await client.get("/api/runs/graphs/test_minimal/runs/99999999999999")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_run_metadata_schema(self, client: AsyncClient) -> None:
        """Test that run metadata has required fields."""
        # First get a valid run ID
        runs_response = await client.get("/api/runs/graphs/test_minimal/runs")
        if runs_response.status_code != 200 or not runs_response.json()["runs"]:
            pytest.skip("No test runs available")

        run_id = runs_response.json()["runs"][0]["run_id"]
        response = await client.get(f"/api/runs/graphs/test_minimal/runs/{run_id}")

        assert response.status_code == 200
        data = response.json()
        assert "run_id" in data
        assert "created_at" in data
        assert "nodes" in data
        assert isinstance(data["nodes"], list)

    @pytest.mark.asyncio
    async def test_get_run_metadata_node_fields(self, client: AsyncClient) -> None:
        """Test that node metadata has required fields."""
        # First get a valid run ID
        runs_response = await client.get("/api/runs/graphs/test_minimal/runs")
        if runs_response.status_code != 200 or not runs_response.json()["runs"]:
            pytest.skip("No test runs available")

        run_id = runs_response.json()["runs"][0]["run_id"]
        response = await client.get(f"/api/runs/graphs/test_minimal/runs/{run_id}")

        assert response.status_code == 200
        data = response.json()

        # There should be nodes in test_minimal
        assert len(data["nodes"]) > 0

        for node in data["nodes"]:
            assert "canonical_node_id" in node
            assert "node_id" in node
            assert "metric_id" in node
            assert "result_path" in node
            assert "statistical_methods" in node
            assert isinstance(node["statistical_methods"], list)

    @pytest.mark.asyncio
    async def test_get_run_metadata_path_traversal_blocked(self, client: AsyncClient) -> None:
        """Test that path traversal attempts are blocked."""
        response = await client.get("/api/runs/graphs/test_minimal/runs/../../../etc/passwd")
        assert response.status_code == 404


class TestGetGraphStructure:
    """Tests for GET /api/runs/graphs/{graph_name}/runs/{run_id}/graph endpoint."""

    @pytest.mark.asyncio
    async def test_get_graph_structure_not_found(self, client: AsyncClient) -> None:
        """Test that nonexistent run returns 404."""
        response = await client.get("/api/runs/graphs/test_minimal/runs/99999999999999/graph")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_graph_structure_schema(self, client: AsyncClient) -> None:
        """Test that graph structure has required fields."""
        # First get a valid run ID
        runs_response = await client.get("/api/runs/graphs/test_minimal/runs")
        if runs_response.status_code != 200 or not runs_response.json()["runs"]:
            pytest.skip("No test runs available")

        run_id = runs_response.json()["runs"][0]["run_id"]
        response = await client.get(f"/api/runs/graphs/test_minimal/runs/{run_id}/graph")

        assert response.status_code == 200
        data = response.json()
        assert "nodes" in data
        assert "edges" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)

    @pytest.mark.asyncio
    async def test_get_graph_structure_node_fields(self, client: AsyncClient) -> None:
        """Test that nodes have required fields."""
        runs_response = await client.get("/api/runs/graphs/test_minimal/runs")
        if runs_response.status_code != 200 or not runs_response.json()["runs"]:
            pytest.skip("No test runs available")

        run_id = runs_response.json()["runs"][0]["run_id"]
        response = await client.get(f"/api/runs/graphs/test_minimal/runs/{run_id}/graph")

        if response.status_code == 200 and response.json()["nodes"]:
            node = response.json()["nodes"][0]
            assert "id" in node
            assert "label" in node
            assert "shape" in node
