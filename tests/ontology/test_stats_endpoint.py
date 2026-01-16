"""Tests for GET /api/ontology/stats endpoint."""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_async_db_session
from src.ontology.router import router
from src.ontology.schema import EDGE_LABELS, VERTEX_LABELS


@pytest.fixture
def mock_session() -> AsyncMock:
    """Create a mock database session that returns counts."""
    session = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    # Return count of 10 for each query
    mock_result.fetchall.return_value = [(10,)]
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


class TestGetStats:
    """Tests for GET /api/ontology/stats."""

    def test_returns_vertex_counts(self, client: TestClient) -> None:
        """Should return counts for all vertex labels."""
        response = client.get("/api/ontology/stats")
        assert response.status_code == 200
        data = response.json()
        assert "vertex_counts" in data
        for vertex_class in VERTEX_LABELS:
            assert vertex_class.label in data["vertex_counts"]

    def test_returns_edge_counts(self, client: TestClient) -> None:
        """Should return counts for all edge labels."""
        response = client.get("/api/ontology/stats")
        assert response.status_code == 200
        data = response.json()
        assert "edge_counts" in data
        for edge_class in EDGE_LABELS:
            assert edge_class.label in data["edge_counts"]

    def test_returns_503_on_db_error(self) -> None:
        """Should return 503 when database query fails."""
        failing_session = AsyncMock(spec=AsyncSession)
        failing_session.execute.side_effect = Exception("Database error")

        test_app = FastAPI()
        test_app.include_router(router, prefix="/api")

        async def override() -> AsyncGenerator[AsyncMock, None]:
            yield failing_session

        test_app.dependency_overrides[get_async_db_session] = override
        failing_client = TestClient(test_app)

        response = failing_client.get("/api/ontology/stats")
        assert response.status_code == 503
        assert "temporarily unavailable" in response.json()["detail"]
