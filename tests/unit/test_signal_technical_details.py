"""Tests for GET /api/signals/{id}/technical-details endpoint.

Tests focus on input validation since the endpoint requires database access
for 404 paths. Integration tests that verify 404 behavior should be added
when a test database with proper fixtures is available.
"""

import pytest
from httpx import AsyncClient


class TestGetTechnicalDetailsValidation:
    """Tests for technical details endpoint input validation."""

    @pytest.mark.asyncio
    async def test_get_technical_details_invalid_uuid(self, client: AsyncClient) -> None:
        """422 returned for invalid UUID format.

        Verifies that FastAPI's path parameter validation rejects
        malformed UUIDs before the handler is invoked.
        """
        response = await client.get("/api/signals/not-a-uuid/technical-details")

        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_get_technical_details_partial_uuid(self, client: AsyncClient) -> None:
        """422 returned for partial/truncated UUID.

        Verifies that an incomplete UUID is rejected.
        """
        response = await client.get("/api/signals/550e8400-e29b-41d4/technical-details")

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_technical_details_empty_uuid(self, client: AsyncClient) -> None:
        """Empty UUID path segment is handled gracefully.

        Verifies that empty path segment is handled, typically as a redirect
        or 404 since it won't match the expected route.
        """
        response = await client.get("/api/signals//technical-details")

        # Empty segment typically results in 404 or route mismatch
        assert response.status_code in (404, 422)
