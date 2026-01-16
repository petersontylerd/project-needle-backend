"""Integration tests for workflow router endpoints.

Tests the workflow API endpoints including error paths for 404s,
validation errors, invalid status transitions, and edge cases.

Note: These tests run against the real app but without a database connection.
Tests are designed to exercise validation and error paths that don't require
database operations.
"""

from uuid import uuid4

import pytest
from httpx import AsyncClient


class TestAssignSignalValidation:
    """Tests for POST /api/signals/{signal_id}/assign endpoint validation."""

    @pytest.mark.asyncio
    async def test_assign_signal_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid signal UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.post(
            "/api/signals/not-a-valid-uuid/assign",
            json={
                "assignee_id": str(uuid4()),
                "role_type": "clinical_leadership",
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_assign_signal_invalid_assignee_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid assignee UUID format.

        Verifies that an invalid assignee_id UUID returns 422.
        """
        response = await client.post(
            f"/api/signals/{uuid4()}/assign",
            json={
                "assignee_id": "not-a-valid-uuid",
                "role_type": "clinical_leadership",
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_assign_signal_invalid_role_type(self, client: AsyncClient) -> None:
        """Test validation error for invalid role_type.

        Verifies that an invalid role_type value returns 422.
        """
        response = await client.post(
            f"/api/signals/{uuid4()}/assign",
            json={
                "assignee_id": str(uuid4()),
                "role_type": "invalid_role",
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_assign_signal_missing_required_fields(self, client: AsyncClient) -> None:
        """Test validation error for missing required fields.

        Verifies that missing required fields return 422.
        """
        response = await client.post(
            f"/api/signals/{uuid4()}/assign",
            json={},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_assign_signal_missing_assignee_id(self, client: AsyncClient) -> None:
        """Test validation error for missing assignee_id.

        Verifies that missing assignee_id returns 422.
        """
        response = await client.post(
            f"/api/signals/{uuid4()}/assign",
            json={
                "role_type": "clinical_leadership",
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_assign_signal_missing_role_type(self, client: AsyncClient) -> None:
        """Test validation error for missing role_type.

        Verifies that missing role_type returns 422.
        """
        response = await client.post(
            f"/api/signals/{uuid4()}/assign",
            json={
                "assignee_id": str(uuid4()),
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_assign_signal_partial_uuid_in_path(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID in path.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.post(
            "/api/signals/550e8400-e29b/assign",
            json={
                "assignee_id": str(uuid4()),
                "role_type": "clinical_leadership",
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_assign_signal_empty_body(self, client: AsyncClient) -> None:
        """Test validation error for empty request body.

        Verifies that empty body returns 422.
        """
        response = await client.post(
            f"/api/signals/{uuid4()}/assign",
            content="",
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422


class TestUpdateSignalStatusValidation:
    """Tests for PATCH /api/signals/{signal_id}/status endpoint validation."""

    @pytest.mark.asyncio
    async def test_update_status_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid signal UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.patch(
            "/api/signals/not-a-valid-uuid/status",
            json={"status": "in_progress"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_update_status_invalid_status_value(self, client: AsyncClient) -> None:
        """Test validation error for invalid status value.

        Verifies that an invalid status value returns 422.
        """
        response = await client.patch(
            f"/api/signals/{uuid4()}/status",
            json={"status": "invalid_status"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_update_status_missing_status_field(self, client: AsyncClient) -> None:
        """Test validation error for missing status field.

        Verifies that missing status field returns 422.
        """
        response = await client.patch(
            f"/api/signals/{uuid4()}/status",
            json={"notes": "Missing status"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_update_status_empty_body(self, client: AsyncClient) -> None:
        """Test validation error for empty request body.

        Verifies that empty body returns 422.
        """
        response = await client.patch(
            f"/api/signals/{uuid4()}/status",
            json={},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_update_status_partial_uuid_in_path(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID in path.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.patch(
            "/api/signals/550e8400/status",
            json={"status": "in_progress"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_update_status_numeric_status(self, client: AsyncClient) -> None:
        """Test validation error for numeric status value.

        Verifies that non-string status values are rejected.
        """
        response = await client.patch(
            f"/api/signals/{uuid4()}/status",
            json={"status": 123},
        )
        assert response.status_code == 422


class TestGetSignalAssignmentValidation:
    """Tests for GET /api/signals/{signal_id}/assignment endpoint validation."""

    @pytest.mark.asyncio
    async def test_get_assignment_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid signal UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.get("/api/signals/not-a-valid-uuid/assignment")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_assignment_partial_uuid(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.get("/api/signals/550e8400-e29b/assignment")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_assignment_special_chars(self, client: AsyncClient) -> None:
        """Test validation error for special characters in UUID.

        Verifies that special characters are rejected.
        """
        response = await client.get("/api/signals/abc!@#/assignment")
        assert response.status_code == 422
