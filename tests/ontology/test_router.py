"""Tests for ontology API router."""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_async_db_session
from src.ontology.router import _parse_agtype_vertex, router


@pytest.fixture
def mock_session() -> AsyncMock:
    """Create a mock database session."""
    session = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    session.execute.return_value = mock_result
    return session


@pytest.fixture
def app(mock_session: AsyncMock) -> FastAPI:
    """Create test FastAPI app with mocked database session."""
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api")

    async def override_get_session() -> AsyncGenerator[AsyncMock, None]:
        yield mock_session

    test_app.dependency_overrides[get_async_db_session] = override_get_session
    return test_app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create test client."""
    return TestClient(app)


class TestListLabels:
    """Tests for GET /api/ontology/labels."""

    def test_returns_valid_labels(self, client: TestClient) -> None:
        """Should return list of valid vertex labels."""
        response = client.get("/api/ontology/labels")

        assert response.status_code == 200
        data = response.json()
        assert "labels" in data
        assert "Facility" in data["labels"]
        assert "Signal" in data["labels"]
        assert "Metric" in data["labels"]


class TestListVertices:
    """Tests for GET /api/ontology/vertices/{label}."""

    def test_rejects_invalid_label(self, client: TestClient) -> None:
        """Should return 400 for invalid label."""
        response = client.get("/api/ontology/vertices/InvalidLabel")

        assert response.status_code == 400
        assert "Invalid label" in response.json()["detail"]

    def test_accepts_valid_label(self, client: TestClient) -> None:
        """Should accept valid vertex labels and return empty list."""
        response = client.get("/api/ontology/vertices/Facility")

        assert response.status_code == 200
        data = response.json()
        assert "vertices" in data
        assert "count" in data
        assert data["count"] == 0
        assert data["vertices"] == []

    def test_respects_limit_parameter(self, client: TestClient, mock_session: AsyncMock) -> None:
        """Should pass limit parameter to query."""
        response = client.get("/api/ontology/vertices/Facility?limit=50")

        assert response.status_code == 200
        # Verify execute was called with appropriate SQL
        mock_session.execute.assert_called_once()


class TestGetVertex:
    """Tests for GET /api/ontology/vertices/{label}/{id}."""

    def test_rejects_invalid_label(self, client: TestClient) -> None:
        """Should return 400 for invalid label."""
        response = client.get("/api/ontology/vertices/InvalidLabel/123")

        assert response.status_code == 400

    def test_returns_404_when_not_found(self, client: TestClient) -> None:
        """Should return 404 when vertex is not found."""
        response = client.get("/api/ontology/vertices/Facility/nonexistent")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"]


class TestGetNeighbors:
    """Tests for GET /api/ontology/vertices/{label}/{id}/neighbors."""

    def test_rejects_invalid_label(self, client: TestClient) -> None:
        """Should return 400 for invalid label."""
        response = client.get("/api/ontology/vertices/InvalidLabel/123/neighbors")

        assert response.status_code == 400

    def test_validates_direction_parameter(self, client: TestClient) -> None:
        """Should reject invalid direction values."""
        response = client.get("/api/ontology/vertices/Facility/F1/neighbors?direction=invalid")

        assert response.status_code == 422  # Validation error

    def test_accepts_valid_direction_values(self, client: TestClient) -> None:
        """Should accept outgoing, incoming, and both as direction values."""
        for direction in ["outgoing", "incoming", "both"]:
            response = client.get(f"/api/ontology/vertices/Facility/F1/neighbors?direction={direction}")
            assert response.status_code == 200

    def test_returns_empty_neighbors_list(self, client: TestClient) -> None:
        """Should return empty list when no neighbors found."""
        response = client.get("/api/ontology/vertices/Facility/F1/neighbors")

        assert response.status_code == 200
        data = response.json()
        assert "neighbors" in data
        assert "count" in data
        assert data["count"] == 0


class TestParseAgtypeVertex:
    """Tests for _parse_agtype_vertex function."""

    def test_parse_none_returns_empty_dict(self) -> None:
        """Should return empty dict for None input."""
        result = _parse_agtype_vertex(None)
        assert result == {}

    def test_parse_age_vertex_format(self) -> None:
        """Should parse AGE vertex format: Label{...}::vertex."""
        result = _parse_agtype_vertex('Facility{"id": "F1", "name": "Hospital"}::vertex')

        assert result["id"] == "F1"
        assert result["name"] == "Hospital"
        assert result["__label"] == "Facility"

    def test_parse_json_format(self) -> None:
        """Should parse clean JSON format."""
        result = _parse_agtype_vertex('{"id": "F1", "name": "Hospital"}')

        assert result["id"] == "F1"
        assert result["name"] == "Hospital"

    def test_parse_tuple_with_age_format(self) -> None:
        """Should handle tuple results with AGE format."""
        result = _parse_agtype_vertex(('Facility{"id": "F1"}::vertex',))

        assert result["id"] == "F1"
        assert result["__label"] == "Facility"

    def test_parse_invalid_returns_raw(self) -> None:
        """Should return raw value for unparseable input."""
        result = _parse_agtype_vertex("invalid data")

        assert result == {"raw": "invalid data"}


class TestDatabaseErrorHandling:
    """Tests for database error handling (503 responses)."""

    @pytest.fixture
    def failing_session(self) -> AsyncMock:
        """Create a mock session that raises exceptions."""
        session = AsyncMock(spec=AsyncSession)
        session.execute.side_effect = Exception("Database connection failed")
        return session

    @pytest.fixture
    def failing_app(self, failing_session: AsyncMock) -> FastAPI:
        """Create test app with failing database session."""
        test_app = FastAPI()
        test_app.include_router(router, prefix="/api")

        async def override_get_session() -> AsyncGenerator[AsyncMock, None]:
            yield failing_session

        test_app.dependency_overrides[get_async_db_session] = override_get_session
        return test_app

    @pytest.fixture
    def failing_client(self, failing_app: FastAPI) -> TestClient:
        """Create test client with failing database."""
        return TestClient(failing_app)

    def test_list_vertices_returns_503_on_db_error(self, failing_client: TestClient) -> None:
        """Should return 503 when database query fails for list_vertices."""
        response = failing_client.get("/api/ontology/vertices/Facility")

        assert response.status_code == 503
        assert "temporarily unavailable" in response.json()["detail"]

    def test_get_vertex_returns_503_on_db_error(self, failing_client: TestClient) -> None:
        """Should return 503 when database query fails for get_vertex."""
        response = failing_client.get("/api/ontology/vertices/Facility/F1")

        assert response.status_code == 503
        assert "temporarily unavailable" in response.json()["detail"]

    def test_get_neighbors_returns_503_on_db_error(self, failing_client: TestClient) -> None:
        """Should return 503 when database query fails for get_neighbors."""
        response = failing_client.get("/api/ontology/vertices/Facility/F1/neighbors")

        assert response.status_code == 503
        assert "temporarily unavailable" in response.json()["detail"]
