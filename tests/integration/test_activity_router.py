"""Integration tests for activity feed router endpoints.

Tests the activity feed API endpoints including error paths for 404s,
cursor validation, and pagination edge cases.

Note: These tests run against the real app but without a database connection.
Tests are designed to exercise validation and error paths that don't require
database operations.
"""

import pytest
from httpx import AsyncClient


class TestGetFeedValidation:
    """Tests for GET /api/feed endpoint input validation."""

    @pytest.mark.asyncio
    async def test_get_feed_with_invalid_cursor(self, client: AsyncClient) -> None:
        """Test 400 response for invalid cursor format.

        Verifies that an invalid cursor format returns 400.
        """
        invalid_cursor = "not-a-valid-datetime"
        response = await client.get(f"/api/feed?cursor={invalid_cursor}")
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data
        assert "cursor" in data["detail"].lower()

    @pytest.mark.asyncio
    async def test_get_feed_invalid_limit_too_high(self, client: AsyncClient) -> None:
        """Test that limit > 100 is rejected.

        Verifies the input validation constraint on limit parameter.
        """
        response = await client.get("/api/feed?limit=101")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_feed_invalid_limit_zero(self, client: AsyncClient) -> None:
        """Test that limit = 0 is rejected.

        Verifies the ge=1 constraint.
        """
        response = await client.get("/api/feed?limit=0")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_feed_invalid_limit_negative(self, client: AsyncClient) -> None:
        """Test that negative limit is rejected.

        Verifies the ge=1 constraint.
        """
        response = await client.get("/api/feed?limit=-5")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_feed_invalid_event_type(self, client: AsyncClient) -> None:
        """Test that invalid event_type is rejected.

        Verifies enum validation on event_type parameter.
        """
        response = await client.get("/api/feed?event_type=invalid_type")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_feed_invalid_signal_id_format(self, client: AsyncClient) -> None:
        """Test that invalid UUID format for signal_id is rejected.

        Verifies UUID validation on signal_id parameter.
        """
        response = await client.get("/api/feed?signal_id=not-a-uuid")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_feed_cursor_with_special_characters(self, client: AsyncClient) -> None:
        """Test cursor with special characters is rejected.

        Verifies cursor parsing rejects invalid character sequences.
        """
        bad_cursor = "2025-<script>alert</script>"
        response = await client.get(f"/api/feed?cursor={bad_cursor}")
        # This should be rejected as invalid ISO format
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_get_feed_cursor_with_invalid_timezone(self, client: AsyncClient) -> None:
        """Test cursor with malformed timezone.

        Verifies cursor parsing handles timezone issues.
        """
        bad_tz_cursor = "2025-12-11T10:00:00+99:99"
        response = await client.get(f"/api/feed?cursor={bad_tz_cursor}")
        assert response.status_code == 400


class TestMarkEventReadValidation:
    """Tests for POST /api/feed/{event_id}/mark-read endpoint validation."""

    @pytest.mark.asyncio
    async def test_mark_read_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid event UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.post("/api/feed/not-a-valid-uuid/mark-read")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_mark_read_empty_uuid(self, client: AsyncClient) -> None:
        """Test validation error for empty event UUID.

        Verifies that an empty UUID returns 404 (route not matched).
        """
        response = await client.post("/api/feed//mark-read")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_mark_read_special_characters_in_uuid(self, client: AsyncClient) -> None:
        """Test validation error for special characters in UUID.

        Verifies that special characters are rejected (returns 404 or 422).
        """
        response = await client.post("/api/feed/abc-!@#-def/mark-read")
        # FastAPI may return 404 (route not matched) or 422 (validation error)
        assert response.status_code in [404, 422]

    @pytest.mark.asyncio
    async def test_mark_read_too_short_uuid(self, client: AsyncClient) -> None:
        """Test validation error for truncated UUID.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.post("/api/feed/550e8400-e29b-41d4/mark-read")
        assert response.status_code == 422
