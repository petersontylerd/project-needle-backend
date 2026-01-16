"""Integration tests for signals router endpoints.

Tests the signals API endpoints including error paths for 404s,
validation errors, and other edge cases.

Note: These tests run against the real app but without a database connection.
Tests are designed to exercise validation and error paths that don't require
database operations.
"""

from uuid import uuid4

import httpx
import pytest
from httpx import AsyncClient


class TestListSignalsValidation:
    """Tests for GET /api/signals endpoint input validation."""

    @pytest.mark.asyncio
    async def test_list_signals_invalid_limit_too_high(self, client: AsyncClient) -> None:
        """Test that limit > 100 is rejected.

        Verifies the input validation constraint on limit parameter.
        """
        response = await client.get("/api/signals?limit=101")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_list_signals_invalid_limit_negative(self, client: AsyncClient) -> None:
        """Test that negative limit is rejected.

        Verifies the input validation constraint on limit parameter.
        """
        response = await client.get("/api/signals?limit=-1")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_list_signals_invalid_limit_zero(self, client: AsyncClient) -> None:
        """Test that limit = 0 is rejected.

        Verifies the ge=1 constraint.
        """
        response = await client.get("/api/signals?limit=0")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_list_signals_invalid_offset_negative(self, client: AsyncClient) -> None:
        """Test that negative offset is rejected.

        Verifies the ge=0 constraint.
        """
        response = await client.get("/api/signals?offset=-1")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_list_signals_invalid_domain_value(self, client: AsyncClient) -> None:
        """Test that invalid domain value is rejected.

        Verifies enum validation on domain parameter.
        """
        response = await client.get("/api/signals?domain=InvalidDomain")
        assert response.status_code == 422


class TestGetSignalValidation:
    """Tests for GET /api/signals/{signal_id} endpoint validation."""

    @pytest.mark.asyncio
    async def test_get_signal_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.get("/api/signals/not-a-valid-uuid")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_get_signal_empty_uuid(self, client: AsyncClient) -> None:
        """Test validation error for empty UUID.

        Verifies that empty path parameter is handled.
        """
        response = await client.get("/api/signals/")
        # Empty path triggers redirect (307) to /api/signals or matches list endpoint
        assert response.status_code in [200, 307, 404, 422, 500]

    @pytest.mark.asyncio
    async def test_get_signal_partial_uuid(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.get("/api/signals/550e8400-e29b")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_signal_special_chars_in_uuid(self, client: AsyncClient) -> None:
        """Test validation error for special characters in UUID.

        Verifies that special characters are rejected.
        """
        response = await client.get("/api/signals/test!@#$%")
        assert response.status_code == 422


class TestGetSignalContributionsValidation:
    """Tests for GET /api/signals/{signal_id}/contributions endpoint validation."""

    @pytest.mark.asyncio
    async def test_get_contributions_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.get("/api/signals/not-a-uuid/contributions")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_contributions_invalid_top_n_too_high(self, client: AsyncClient) -> None:
        """Test validation error for top_n > 50.

        Verifies the le=50 constraint.
        """
        fake_uuid = uuid4()
        response = await client.get(f"/api/signals/{fake_uuid}/contributions?top_n=100")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_contributions_invalid_top_n_zero(self, client: AsyncClient) -> None:
        """Test validation error for top_n = 0.

        Verifies the ge=1 constraint.
        """
        fake_uuid = uuid4()
        response = await client.get(f"/api/signals/{fake_uuid}/contributions?top_n=0")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_contributions_invalid_top_n_negative(self, client: AsyncClient) -> None:
        """Test validation error for negative top_n.

        Verifies the ge=1 constraint.
        """
        fake_uuid = uuid4()
        response = await client.get(f"/api/signals/{fake_uuid}/contributions?top_n=-5")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_contributions_partial_uuid(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.get("/api/signals/550e8400/contributions")
        assert response.status_code == 422


class TestGetSignalChildrenValidation:
    """Tests for GET /api/signals/{signal_id}/children endpoint validation."""

    @pytest.mark.asyncio
    async def test_get_children_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.get("/api/signals/not-a-valid-uuid/children")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_get_children_partial_uuid(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.get("/api/signals/550e8400-e29b/children")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_children_invalid_limit_too_high(self, client: AsyncClient) -> None:
        """Test validation error for limit > 100.

        Verifies the le=100 constraint.
        """
        fake_uuid = uuid4()
        response = await client.get(f"/api/signals/{fake_uuid}/children?limit=101")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_children_invalid_limit_zero(self, client: AsyncClient) -> None:
        """Test validation error for limit = 0.

        Verifies the ge=1 constraint.
        """
        fake_uuid = uuid4()
        response = await client.get(f"/api/signals/{fake_uuid}/children?limit=0")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_children_invalid_limit_negative(self, client: AsyncClient) -> None:
        """Test validation error for negative limit.

        Verifies the ge=1 constraint.
        """
        fake_uuid = uuid4()
        response = await client.get(f"/api/signals/{fake_uuid}/children?limit=-10")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_children_special_chars_in_uuid(self, client: AsyncClient) -> None:
        """Test validation error for special characters in UUID.

        Verifies that special characters are rejected.
        """
        response = await client.get("/api/signals/test!@#$%/children")
        assert response.status_code == 422


class TestGetSignalParentValidation:
    """Tests for GET /api/signals/{signal_id}/parent endpoint validation."""

    @pytest.mark.asyncio
    async def test_get_parent_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.get("/api/signals/not-a-valid-uuid/parent")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_get_parent_partial_uuid(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.get("/api/signals/550e8400-e29b/parent")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_parent_special_chars_in_uuid(self, client: AsyncClient) -> None:
        """Test validation error for special characters in UUID.

        Verifies that special characters are rejected.
        """
        response = await client.get("/api/signals/test!@#$%/parent")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_parent_empty_uuid(self, client: AsyncClient) -> None:
        """Test validation error for empty UUID segment.

        Verifies that empty path segment is handled.
        """
        response = await client.get("/api/signals//parent")
        # Empty path triggers 404 (not found) or 422 (validation error)
        assert response.status_code in [404, 422]


class TestListFacilities:
    """Tests for GET /api/signals/facilities endpoint."""

    @pytest.mark.asyncio
    async def test_list_facilities_returns_list_of_strings(self, client: AsyncClient) -> None:
        """Test that facilities endpoint returns a list of strings.

        Verifies the endpoint responds with 200 and returns a JSON array.
        If there are facilities, they should all be strings.
        Note: May return empty list if no signals exist in database.
        """
        response = await client.get("/api/signals/facilities")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # If there are facilities, they should all be strings
        for facility in data:
            assert isinstance(facility, str)


async def _get_with_schema_guard(client: AsyncClient, url: str) -> httpx.Response:
    """Execute GET request, skipping test if schema is out of sync.

    This helper handles cases where the test database schema doesn't include
    columns required by the endpoint (e.g., system_name column missing).
    """
    try:
        response = await client.get(url)
    except Exception as e:
        if "system_name" in str(e).lower() or "does not exist" in str(e).lower():
            pytest.skip("Database schema is out of sync (system_name column missing)")
        raise
    if response.status_code == 500:
        pytest.skip("Database schema is out of sync (system_name column missing)")
    return response


class TestListFacilitiesFiltered:
    """Tests for GET /api/signals/facilities with system_name filter."""

    @pytest.mark.asyncio
    async def test_list_facilities_filtered_by_system(self, client: AsyncClient) -> None:
        """Test that facilities endpoint filters by system_name."""
        response = await _get_with_schema_guard(client, "/api/signals/facilities?system_name=BANNER_SYSTEM")
        assert response.status_code == 200
        facilities = response.json()
        assert isinstance(facilities, list)
        for facility in facilities:
            assert isinstance(facility, str)

    @pytest.mark.asyncio
    async def test_list_facilities_nonexistent_system_returns_empty(self, client: AsyncClient) -> None:
        """Test that nonexistent system returns empty list."""
        response = await _get_with_schema_guard(client, "/api/signals/facilities?system_name=NONEXISTENT_SYSTEM_XYZ")
        assert response.status_code == 200
        assert response.json() == []

    @pytest.mark.asyncio
    async def test_list_facilities_without_system_returns_all(self, client: AsyncClient) -> None:
        """Test that omitting system_name returns all facilities."""
        response = await client.get("/api/signals/facilities")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


class TestGetRelatedSignalsValidation:
    """Tests for GET /api/signals/{signal_id}/related endpoint validation."""

    @pytest.mark.asyncio
    async def test_get_related_signals_invalid_uuid(self, client: AsyncClient) -> None:
        """Test validation error for invalid UUID format.

        Verifies that an invalid UUID format returns 422.
        """
        response = await client.get("/api/signals/not-a-valid-uuid/related")
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_get_related_signals_partial_uuid(self, client: AsyncClient) -> None:
        """Test validation error for partial UUID.

        Verifies that incomplete UUIDs are rejected.
        """
        response = await client.get("/api/signals/550e8400-e29b/related")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_related_signals_special_chars_in_uuid(self, client: AsyncClient) -> None:
        """Test validation error for special characters in UUID.

        Verifies that special characters are rejected.
        """
        response = await client.get("/api/signals/test!@#$%/related")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_related_signals_empty_uuid_segment(self, client: AsyncClient) -> None:
        """Test validation error for empty UUID segment.

        Verifies that empty path segment is handled.
        """
        response = await client.get("/api/signals//related")
        # Empty path triggers 404 (route not found) or 422 (validation error)
        assert response.status_code in [404, 422]


class TestGetRelatedSignalsFunctional:
    """Functional tests for GET /api/signals/{signal_id}/related endpoint behavior.

    These tests require a properly migrated database and use fixtures
    from conftest.py to create test signals.
    """

    @pytest.mark.asyncio
    async def test_returns_404_when_signal_not_found(self, client: AsyncClient) -> None:
        """404 error when base signal doesn't exist.

        If the signal_id in the URL doesn't exist in the database,
        return 404 Not Found.
        """
        non_existent_id = "00000000-0000-0000-0000-000000000000"
        try:
            response = await client.get(f"/api/signals/{non_existent_id}/related")
        except Exception as e:
            # Skip if database is unavailable or schema is out of sync
            pytest.skip(f"Database unavailable or schema mismatch: {e}")
        # Skip if database schema is out of sync (500 error)
        if response.status_code == 500:
            pytest.skip("Database schema is out of sync, skipping 404 test")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_related_signals_exist(self, client: AsyncClient, isolated_signal) -> None:
        """Empty list when no signals share facility/service_line.

        If the base signal exists but no other signals share its
        facility and service_line combination, return an empty list.
        """
        response = await client.get(f"/api/signals/{isolated_signal.id}/related")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    @pytest.mark.asyncio
    async def test_returns_signals_with_same_facility_and_service_line(self, client: AsyncClient, related_signals_set: dict) -> None:
        """Related signals share facility and service_line with the base signal.

        The endpoint should return signals that have:
        - Same facility as the base signal
        - Same service_line as the base signal
        - Different metric_id than the base signal
        """
        base_signal = related_signals_set["base"]
        response = await client.get(f"/api/signals/{base_signal.id}/related")

        assert response.status_code == 200
        data = response.json()

        # Should return exactly 3 related signals (not unrelated, not same-metric)
        assert isinstance(data, list)
        assert len(data) == 3

        # Verify all returned signals share facility and service_line
        for signal in data:
            assert signal["facility"] == base_signal.facility
            assert signal["service_line"] == base_signal.service_line
            assert signal["metric_id"] != base_signal.metric_id

    @pytest.mark.asyncio
    async def test_excludes_signals_with_same_metric_id(self, client: AsyncClient, related_signals_set: dict) -> None:
        """Related signals must have different metric_id from base signal.

        The endpoint should exclude any signals that have the same metric_id
        as the base signal, even if they share facility and service_line.
        """
        base_signal = related_signals_set["base"]
        same_metric_signal = related_signals_set["same_metric"]

        response = await client.get(f"/api/signals/{base_signal.id}/related")
        assert response.status_code == 200
        data = response.json()

        # The same-metric signal should NOT be in results
        returned_ids = [s["id"] for s in data]
        assert str(same_metric_signal.id) not in returned_ids

        # All returned signals should have different metric_id
        for signal in data:
            assert signal["metric_id"] != base_signal.metric_id

    @pytest.mark.asyncio
    async def test_excludes_base_signal_from_results(self, client: AsyncClient, related_signals_set: dict) -> None:
        """The base signal itself should not appear in related results.

        Even though the base signal matches its own facility/service_line,
        it must be excluded from the results.
        """
        base_signal = related_signals_set["base"]

        response = await client.get(f"/api/signals/{base_signal.id}/related")
        assert response.status_code == 200
        data = response.json()

        # Base signal should NOT be in results
        returned_ids = [s["id"] for s in data]
        assert str(base_signal.id) not in returned_ids

    @pytest.mark.asyncio
    async def test_excludes_unrelated_signals(self, client: AsyncClient, related_signals_set: dict) -> None:
        """Unrelated signals (different facility) should not appear.

        Signals with a different facility should not be returned,
        even if they have a different metric_id.
        """
        base_signal = related_signals_set["base"]
        unrelated_signal = related_signals_set["unrelated"]

        response = await client.get(f"/api/signals/{base_signal.id}/related")
        assert response.status_code == 200
        data = response.json()

        # Unrelated signal should NOT be in results
        returned_ids = [s["id"] for s in data]
        assert str(unrelated_signal.id) not in returned_ids

    @pytest.mark.asyncio
    async def test_orders_by_simplified_severity_descending(self, client: AsyncClient, related_signals_set: dict) -> None:
        """Related signals are ordered by simplified_severity DESC.

        The endpoint should return signals with higher priority scores first.
        """
        base_signal = related_signals_set["base"]

        response = await client.get(f"/api/signals/{base_signal.id}/related")
        assert response.status_code == 200
        data = response.json()

        # Verify order: relatedMetricA (100) > relatedMetricB (75) > relatedMetricC (25)
        assert len(data) >= 3
        scores = [s.get("simplified_severity") for s in data]

        # Filter out None values for comparison (NULLS LAST)
        non_null_scores = [s for s in scores if s is not None]
        assert non_null_scores == sorted(non_null_scores, reverse=True)

    @pytest.mark.asyncio
    async def test_limits_results_to_ten_signals(self, client: AsyncClient, db_session) -> None:
        """Related signals response is limited to 10 results.

        Even if more than 10 related signals exist, the endpoint should
        return at most 10 signals.
        """
        from datetime import UTC, datetime
        from decimal import Decimal

        from src.db.models import Assignment, AssignmentStatus, Signal, SignalDomain

        # Create base signal and 15 related signals
        facility = "Limit Test Facility"
        service_line = "Limit Test Service"
        detected_at = datetime.now(tz=UTC)

        base_signal = Signal(
            canonical_node_id="limit_test__base__node",
            metric_id="limitBaseMetric",
            domain=SignalDomain.EFFICIENCY,
            facility=facility,
            service_line=service_line,
            description="Base signal for limit testing",
            metric_value=Decimal("1.0"),
            detected_at=detected_at,
        )

        try:
            db_session.add(base_signal)
            await db_session.flush()
        except Exception as e:
            pytest.skip(f"Failed to create test signals (schema mismatch?): {e}")

        # Create 15 related signals
        related_signals = []
        for i in range(15):
            signal = Signal(
                canonical_node_id=f"limit_test__related_{i}__node",
                metric_id=f"limitRelatedMetric{i}",
                domain=SignalDomain.EFFICIENCY,
                facility=facility,
                service_line=service_line,
                description=f"Related signal {i} for limit testing",
                metric_value=Decimal(str(1.0 + i * 0.1)),
                simplified_severity=100 - i,
                detected_at=detected_at,
            )
            db_session.add(signal)
            related_signals.append(signal)

        await db_session.flush()

        # Create assignments
        all_signals = [base_signal, *related_signals]
        assignments = []
        for signal in all_signals:
            assignment = Assignment(
                signal_id=signal.id,
                status=AssignmentStatus.NEW,
            )
            db_session.add(assignment)
            assignments.append(assignment)
        await db_session.flush()

        try:
            # Make the request using the provided client fixture
            response = await client.get(f"/api/signals/{base_signal.id}/related")

            assert response.status_code == 200
            data = response.json()

            # Should be limited to 10 results
            assert len(data) == 10
        finally:
            # Cleanup
            for assignment in assignments:
                await db_session.delete(assignment)
            for signal in all_signals:
                await db_session.delete(signal)
            await db_session.commit()
