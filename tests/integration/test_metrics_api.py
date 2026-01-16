"""Integration tests for metrics API router endpoints.

Tests the semantic layer metrics API endpoints including:
- GET /api/metrics/definitions
- GET /api/metrics/definitions/{metric_name}
- GET /api/metrics/categories
- POST /api/metrics/query/{metric_name}
- GET /api/metrics/semantic-models

Note: These tests run against the real app. Tests that require the
semantic manifest will return 503 if the manifest is not present.
Tests are designed to exercise validation and error paths.
"""

import pytest
from httpx import AsyncClient


class TestMetricDefinitionsEndpoint:
    """Tests for GET /api/metrics/definitions endpoint."""

    @pytest.mark.asyncio
    async def test_list_definitions_returns_list_or_503(self, client: AsyncClient) -> None:
        """Test that definitions endpoint returns list or 503 if manifest missing.

        If semantic manifest exists, returns list of metrics.
        If not, returns 503 Service Unavailable.
        """
        response = await client.get("/api/metrics/definitions")
        # Either successful with list or 503 if manifest missing
        assert response.status_code in [200, 503]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_list_definitions_with_category_filter(self, client: AsyncClient) -> None:
        """Test category filter parameter.

        Verifies that category query parameter is accepted.
        """
        response = await client.get("/api/metrics/definitions?category=Volume")
        # Either successful with filtered list or 503 if manifest missing
        assert response.status_code in [200, 503]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            # All returned metrics should have the requested category
            for metric in data:
                if metric.get("category"):
                    assert metric["category"] == "Volume"


class TestMetricDefinitionDetailEndpoint:
    """Tests for GET /api/metrics/definitions/{metric_name} endpoint."""

    @pytest.mark.asyncio
    async def test_get_definition_not_found_or_503(self, client: AsyncClient) -> None:
        """Test 404 for non-existent metric or 503 if manifest missing.

        Verifies proper error handling for unknown metric names.
        """
        response = await client.get("/api/metrics/definitions/non_existent_metric_xyz")
        # 404 if manifest exists but metric not found, 503 if no manifest
        assert response.status_code in [404, 503]

    @pytest.mark.asyncio
    async def test_get_definition_valid_name_format(self, client: AsyncClient) -> None:
        """Test that valid metric name is accepted.

        Uses a known metric name from the semantic manifest.
        """
        response = await client.get("/api/metrics/definitions/total_signals")
        # Either 200 if found, 404 if not found, or 503 if no manifest
        assert response.status_code in [200, 404, 503]

        if response.status_code == 200:
            data = response.json()
            assert data["name"] == "total_signals"
            assert "label" in data
            assert "description" in data
            assert "type" in data
            assert "available_dimensions" in data


class TestMetricCategoriesEndpoint:
    """Tests for GET /api/metrics/categories endpoint."""

    @pytest.mark.asyncio
    async def test_list_categories_returns_list_or_503(self, client: AsyncClient) -> None:
        """Test that categories endpoint returns sorted list or 503.

        Categories should be unique and sorted alphabetically.
        """
        response = await client.get("/api/metrics/categories")
        assert response.status_code in [200, 503]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            # Should be sorted
            assert data == sorted(data)
            # Should be unique
            assert len(data) == len(set(data))


class TestMetricQueryEndpoint:
    """Tests for POST /api/metrics/query/{metric_name} endpoint."""

    @pytest.mark.asyncio
    async def test_query_metric_empty_body(self, client: AsyncClient) -> None:
        """Test query with empty request body.

        Should accept empty body (all parameters optional).
        """
        response = await client.post("/api/metrics/query/total_signals", json={})
        # 200 if successful, 400 if metric not queryable, 503 if no manifest
        assert response.status_code in [200, 400, 503]

    @pytest.mark.asyncio
    async def test_query_metric_with_dimensions(self, client: AsyncClient) -> None:
        """Test query with group_by dimensions.

        Verifies that group_by parameter is accepted.
        """
        response = await client.post(
            "/api/metrics/query/total_signals",
            json={"group_by": ["severity"]},
        )
        assert response.status_code in [200, 400, 503]

    @pytest.mark.asyncio
    async def test_query_metric_with_date_range(self, client: AsyncClient) -> None:
        """Test query with date range filter.

        Verifies that date parameters are accepted.
        """
        response = await client.post(
            "/api/metrics/query/total_signals",
            json={
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            },
        )
        assert response.status_code in [200, 400, 503]

    @pytest.mark.asyncio
    async def test_query_metric_with_limit(self, client: AsyncClient) -> None:
        """Test query with limit parameter.

        Verifies that limit parameter is accepted.
        """
        response = await client.post("/api/metrics/query/total_signals", json={"limit": 100})
        assert response.status_code in [200, 400, 503]

    @pytest.mark.asyncio
    async def test_query_metric_invalid_limit_too_high(self, client: AsyncClient) -> None:
        """Test that limit > 10000 is rejected.

        Verifies the le=10000 constraint on limit parameter.
        """
        response = await client.post("/api/metrics/query/total_signals", json={"limit": 10001})
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_query_metric_invalid_limit_zero(self, client: AsyncClient) -> None:
        """Test that limit = 0 is rejected.

        Verifies the ge=1 constraint on limit parameter.
        """
        response = await client.post("/api/metrics/query/total_signals", json={"limit": 0})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_query_metric_invalid_limit_negative(self, client: AsyncClient) -> None:
        """Test that negative limit is rejected.

        Verifies the ge=1 constraint on limit parameter.
        """
        response = await client.post("/api/metrics/query/total_signals", json={"limit": -1})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_query_nonexistent_metric(self, client: AsyncClient) -> None:
        """Test query for non-existent metric.

        Should return 400 (metric not found) or 503 if no manifest.
        """
        response = await client.post("/api/metrics/query/non_existent_metric_xyz", json={})
        # 400 if metric not found in manifest, 503 if no manifest
        assert response.status_code in [400, 503]


class TestSemanticModelsEndpoint:
    """Tests for GET /api/metrics/semantic-models endpoint."""

    @pytest.mark.asyncio
    async def test_list_semantic_models_returns_list_or_503(self, client: AsyncClient) -> None:
        """Test that semantic models endpoint returns list or 503.

        Should return list of semantic model definitions.
        """
        response = await client.get("/api/metrics/semantic-models")
        assert response.status_code in [200, 503]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            # Each model should have required fields
            for model in data:
                assert "name" in model
                assert "description" in model
                assert "table_name" in model
                assert "dimensions" in model
                assert "measures" in model


class TestLegacyEndpoints:
    """Tests for legacy backward-compatible endpoints."""

    @pytest.mark.asyncio
    async def test_legacy_get_all_metrics(self, client: AsyncClient) -> None:
        """Test GET /api/metrics returns hardcoded metrics.

        This is the legacy endpoint for backward compatibility.
        """
        response = await client.get("/api/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "metrics" in data
        assert "count" in data
        assert isinstance(data["metrics"], list)
        assert data["count"] == len(data["metrics"])

    @pytest.mark.asyncio
    async def test_legacy_get_metric_by_id(self, client: AsyncClient) -> None:
        """Test GET /api/metrics/{metric_id} returns legacy metric.

        This is the legacy endpoint for backward compatibility.
        """
        response = await client.get("/api/metrics/losIndex")
        assert response.status_code == 200
        data = response.json()
        assert data["metric_id"] == "losIndex"
        assert "display_name" in data
        assert "description" in data
        assert "domain" in data

    @pytest.mark.asyncio
    async def test_legacy_get_metric_not_found(self, client: AsyncClient) -> None:
        """Test 404 for non-existent legacy metric.

        Verifies proper error handling for unknown metric IDs.
        """
        response = await client.get("/api/metrics/nonExistentMetric")
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
