"""Tests for health check endpoint."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient) -> None:
    """Test that health endpoint returns healthy status.

    Args:
        client: Async test client fixture.
    """
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "version" in data


@pytest.mark.asyncio
async def test_health_check_version(client: AsyncClient) -> None:
    """Test that health endpoint returns version.

    Args:
        client: Async test client fixture.
    """
    response = await client.get("/health")
    data = response.json()
    assert data["version"] == "0.1.0"
