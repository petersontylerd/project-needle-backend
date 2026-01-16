"""Tests for users API endpoints."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_list_users_returns_list(client: AsyncClient) -> None:
    """GET /api/users should return a list (possibly empty in test env)."""
    response = await client.get("/api/users")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_list_users_response_shape(client: AsyncClient) -> None:
    """GET /api/users response items should have expected fields when users exist."""
    response = await client.get("/api/users")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    # If users exist, verify the shape
    if len(data) > 0:
        user = data[0]
        assert "id" in user
        assert "name" in user
        assert "email" in user
        assert "role" in user


@pytest.mark.asyncio
async def test_list_users_excludes_password_fields(client: AsyncClient) -> None:
    """GET /api/users should not expose sensitive fields."""
    response = await client.get("/api/users")
    assert response.status_code == 200

    data = response.json()
    for user in data:
        assert "password" not in user
        assert "password_hash" not in user
        assert "hashed_password" not in user
