"""Tests for signals router."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from src.db.models import Assignment, AssignmentStatus, Signal, SignalDomain
from src.signals.router import _signal_to_response


class TestSignalToResponse:
    """Tests for the _signal_to_response helper function."""

    def _create_test_signal(
        self,
        *,
        assignment: Assignment | None = None,
    ) -> Signal:
        """Create a test Signal instance with required fields."""
        signal = Signal(
            id=uuid4(),
            canonical_node_id="losIndex__medicareId__aggregate_time_period",
            metric_id="losIndex",
            domain=SignalDomain.EFFICIENCY,
            facility="Test Hospital",
            facility_id="010033",
            service_line="Cardiology",
            description="Test signal description",
            metric_value=Decimal("1.25"),
            detected_at=datetime.now(tz=UTC),
            created_at=datetime.now(tz=UTC),
        )
        # Set assignment relationship
        signal.assignment = assignment
        return signal

    def test_workflow_status_defaults_to_new_when_no_assignment(self) -> None:
        """workflow_status should be 'new' when signal has no assignment."""
        signal = self._create_test_signal(assignment=None)

        response = _signal_to_response(signal)

        assert response.workflow_status == "new"

    def test_workflow_status_reflects_assignment_status(self) -> None:
        """workflow_status should reflect the assignment's status."""
        # Test each assignment status
        test_cases = [
            (AssignmentStatus.NEW, "new"),
            (AssignmentStatus.ASSIGNED, "assigned"),
            (AssignmentStatus.IN_PROGRESS, "in_progress"),
            (AssignmentStatus.RESOLVED, "resolved"),
            (AssignmentStatus.CLOSED, "closed"),
        ]

        for assignment_status, expected_workflow_status in test_cases:
            assignment = Assignment(
                id=uuid4(),
                signal_id=uuid4(),
                status=assignment_status,
            )
            signal = self._create_test_signal(assignment=assignment)

            response = _signal_to_response(signal)

            assert response.workflow_status == expected_workflow_status, (
                f"Expected workflow_status '{expected_workflow_status}' for assignment status {assignment_status}, got '{response.workflow_status}'"
            )

    def test_response_includes_workflow_status_field(self) -> None:
        """SignalResponse should include workflow_status in its fields."""
        signal = self._create_test_signal()

        response = _signal_to_response(signal)

        # Verify the field exists in the response model
        assert hasattr(response, "workflow_status")
        # Verify it's included in model dump
        response_dict = response.model_dump()
        assert "workflow_status" in response_dict
