"""Integration tests for signal temporal API endpoint.

Tests the GET /api/signals/{signal_id}/temporal endpoint including:
- Input validation (UUID format)
- 404 for non-existent signals
- Response schema validation
- Edge cases for temporal data presence

Note: These tests run against the real app but without a database connection.
Tests are designed to exercise validation and error paths that don't require
database operations.
"""

import pytest
from httpx import AsyncClient


class TestGetSignalTemporalValidation:
    """Tests for GET /api/signals/{signal_id}/temporal endpoint validation."""

    @pytest.mark.asyncio
    async def test_get_temporal_invalid_uuid_format(self, client: AsyncClient) -> None:
        """Test validation error for invalid UUID format.

        Verifies that an invalid UUID format returns 422 Unprocessable Entity.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If response status code is not 422.
        """
        response = await client.get("/api/signals/not-a-valid-uuid/temporal")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_get_temporal_partial_uuid(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID.

        Verifies that incomplete UUIDs are rejected with 422.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If response status code is not 422.
        """
        response = await client.get("/api/signals/550e8400-e29b/temporal")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_get_temporal_special_chars_in_uuid(self, client: AsyncClient) -> None:
        """Test validation error for special characters in UUID.

        Verifies that special characters in UUID are rejected.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If response status code is not 422.
        """
        response = await client.get("/api/signals/test!@#$%^&*()/temporal")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_temporal_empty_uuid(self, client: AsyncClient) -> None:
        """Test behavior when UUID path segment is empty.

        Verifies that empty path segment is handled appropriately.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If response is not a valid error status.
        """
        response = await client.get("/api/signals//temporal")
        # Empty UUID segment should result in 404 (route not found) or 422
        assert response.status_code in [404, 422]


class TestGetSignalTemporalResponseSchema:
    """Tests for temporal endpoint response schema validation.

    Note: These tests verify routing and validation only. Database-dependent
    tests would require a test database with migrations applied.
    """

    @pytest.mark.asyncio
    async def test_response_schema_validation_error_is_json(self, client: AsyncClient) -> None:
        """Test that validation error responses are proper JSON.

        Verifies that 422 responses from validation errors include
        proper JSON structure with detail field.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If validation error response is malformed.
        """
        response = await client.get("/api/signals/bad-uuid/temporal")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data
        # FastAPI validation errors return a list of error objects
        assert isinstance(data["detail"], list)

    @pytest.mark.asyncio
    async def test_validation_error_includes_location(self, client: AsyncClient) -> None:
        """Test that validation errors include location information.

        Verifies that validation error details specify where the error occurred.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If error location is missing.
        """
        response = await client.get("/api/signals/invalid-uuid-format/temporal")
        assert response.status_code == 422
        data = response.json()
        assert len(data["detail"]) > 0
        error = data["detail"][0]
        # Error should include 'loc' (location) and 'msg' (message)
        assert "loc" in error
        assert "msg" in error


class TestGetSignalTemporalEdgeCases:
    """Tests for temporal endpoint edge cases."""

    @pytest.mark.asyncio
    async def test_uuid_with_invalid_hex_characters(self, client: AsyncClient) -> None:
        """Test that UUIDs with invalid hex characters are rejected.

        Valid UUIDs only contain 0-9 and a-f/A-F characters.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If invalid hex UUID does not cause validation error.
        """
        # UUID-like format but with invalid hex characters (g, h, etc.)
        invalid_hex_uuid = "550g8400-e29b-41d4-a716-446655440000"
        response = await client.get(f"/api/signals/{invalid_hex_uuid}/temporal")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_uuid_with_leading_trailing_spaces(self, client: AsyncClient) -> None:
        """Test handling of UUID with surrounding whitespace.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If response is unexpected.
        """
        # Spaces in URL should be handled by URL encoding
        response = await client.get("/api/signals/ 550e8400-e29b-41d4-a716-446655440000 /temporal")
        # URL with spaces in path should either be 422 or 404
        assert response.status_code in [404, 422]

    @pytest.mark.asyncio
    async def test_null_uuid_value(self, client: AsyncClient) -> None:
        """Test handling of 'null' as UUID string.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If response status is not 422.
        """
        response = await client.get("/api/signals/null/temporal")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_numeric_uuid_value(self, client: AsyncClient) -> None:
        """Test handling of numeric value as UUID.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If response status is not 422.
        """
        response = await client.get("/api/signals/12345/temporal")
        assert response.status_code == 422


class TestGetSignalTemporalContentType:
    """Tests for temporal endpoint content type handling."""

    @pytest.mark.asyncio
    async def test_validation_error_content_type_is_json(self, client: AsyncClient) -> None:
        """Test that validation error content type is application/json.

        Verifies that 422 validation errors return proper JSON content type.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If content type is not JSON.
        """
        # Use invalid UUID to trigger validation error (doesn't hit database)
        response = await client.get("/api/signals/not-a-uuid-format/temporal")
        assert response.status_code == 422
        # Validation error responses should be JSON
        assert "application/json" in response.headers.get("content-type", "")

    @pytest.mark.asyncio
    async def test_error_response_has_detail(self, client: AsyncClient) -> None:
        """Test that error responses include detail field.

        Args:
            client: Async HTTP client fixture.

        Returns:
            None

        Raises:
            AssertionError: If error response lacks detail.
        """
        response = await client.get("/api/signals/invalid-uuid/temporal")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data
        assert isinstance(data["detail"], list)
