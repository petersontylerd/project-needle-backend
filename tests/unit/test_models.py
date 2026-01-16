"""Unit tests for SQLAlchemy models.

Tests model instantiation, enum values, and relationships without database.
"""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from src.db.models import (
    ActivityEvent,
    Assignment,
    AssignmentRoleType,
    AssignmentStatus,
    EventType,
    Signal,
    SignalDomain,
    User,
    UserRole,
)

pytestmark = pytest.mark.tier1


class TestUserRole:
    """Tests for UserRole enum."""

    def test_all_roles_defined(self) -> None:
        """Verify all expected roles are defined."""
        expected_roles = {"admin", "clinical_leadership", "nursing_leadership", "administration", "viewer"}
        actual_roles = {role.value for role in UserRole}
        assert actual_roles == expected_roles

    def test_role_is_string_enum(self) -> None:
        """Verify UserRole inherits from str."""
        assert isinstance(UserRole.ADMIN.value, str)
        assert UserRole.ADMIN == "admin"


class TestSignalDomain:
    """Tests for SignalDomain enum."""

    def test_all_domains_defined(self) -> None:
        """Verify all expected domains are defined."""
        expected = {"Efficiency", "Safety", "Effectiveness"}
        actual = {domain.value for domain in SignalDomain}
        assert actual == expected


class TestAssignmentStatus:
    """Tests for AssignmentStatus enum."""

    def test_all_statuses_defined(self) -> None:
        """Verify all expected statuses are defined."""
        expected = {"new", "assigned", "in_progress", "resolved", "closed"}
        actual = {status.value for status in AssignmentStatus}
        assert actual == expected

    def test_workflow_states(self) -> None:
        """Verify workflow state values."""
        assert AssignmentStatus.NEW.value == "new"
        assert AssignmentStatus.ASSIGNED.value == "assigned"
        assert AssignmentStatus.IN_PROGRESS.value == "in_progress"
        assert AssignmentStatus.RESOLVED.value == "resolved"
        assert AssignmentStatus.CLOSED.value == "closed"


class TestAssignmentRoleType:
    """Tests for AssignmentRoleType enum."""

    def test_all_role_types_defined(self) -> None:
        """Verify all expected role types are defined."""
        expected = {"clinical_leadership", "nursing_leadership", "administration"}
        actual = {role_type.value for role_type in AssignmentRoleType}
        assert actual == expected


class TestEventType:
    """Tests for EventType enum."""

    def test_all_event_types_defined(self) -> None:
        """Verify all expected event types are defined."""
        expected = {
            "new_signal",
            "regression",
            "improvement",
            "insight",
            "technical_error",
            "assignment",
            "status_change",
            "comment",
            "intervention_activated",
            "intervention_outcome",
        }
        actual = {event_type.value for event_type in EventType}
        assert actual == expected


class TestUserModel:
    """Tests for User model."""

    def test_user_table_name(self) -> None:
        """Verify User model table name."""
        assert User.__tablename__ == "users"

    def test_user_instantiation(self) -> None:
        """Test User model can be instantiated with required fields."""
        user = User(
            id=uuid4(),
            email="test@example.com",
            name="Test User",
            hashed_password="$2b$12$test_hash",
            role=UserRole.VIEWER,
            is_active=True,
            created_at=datetime.now(tz=UTC),
            updated_at=datetime.now(tz=UTC),
        )
        assert user.email == "test@example.com"
        assert user.name == "Test User"
        assert user.role == UserRole.VIEWER
        assert user.is_active is True


class TestSignalModel:
    """Tests for Signal model."""

    def test_signal_table_name(self) -> None:
        """Verify Signal model table name."""
        assert Signal.__tablename__ == "signals"

    def test_signal_instantiation(self) -> None:
        """Test Signal model can be instantiated with required fields."""
        now = datetime.now(tz=UTC)
        signal = Signal(
            id=uuid4(),
            canonical_node_id="SoC:facility_001",
            metric_id="losIndex",
            domain=SignalDomain.EFFICIENCY,
            facility="General Hospital",
            service_line="Inpatient",
            description="LOS Index above benchmark",
            metric_value=Decimal("1.25"),
            detected_at=now,
            created_at=now,
            updated_at=now,
        )
        assert signal.canonical_node_id == "SoC:facility_001"
        assert signal.domain == SignalDomain.EFFICIENCY
        assert signal.metric_value == Decimal("1.25")

    def test_signal_optional_fields(self) -> None:
        """Test Signal model with optional fields."""
        now = datetime.now(tz=UTC)
        signal = Signal(
            id=uuid4(),
            canonical_node_id="SoC:facility_001",
            metric_id="losIndex",
            domain=SignalDomain.SAFETY,
            facility="General Hospital",
            facility_id="123456",
            service_line="Inpatient",
            sub_service_line="Medical",
            description="Safety metric with optional fields",
            metric_value=Decimal("2.50"),
            peer_mean=Decimal("1.00"),
            peer_std=Decimal("0.15"),
            percentile_rank=Decimal("95.00"),
            encounters=500,
            detected_at=now,
            created_at=now,
            updated_at=now,
            groupby_label="Facility-wide",
            group_value="Facility-wide",
        )
        assert signal.facility_id == "123456"
        assert signal.peer_mean == Decimal("1.00")
        assert signal.encounters == 500


class TestAssignmentModel:
    """Tests for Assignment model."""

    def test_assignment_table_name(self) -> None:
        """Verify Assignment model table name."""
        assert Assignment.__tablename__ == "assignments"

    def test_assignment_instantiation(self) -> None:
        """Test Assignment model can be instantiated."""
        now = datetime.now(tz=UTC)
        assignment = Assignment(
            id=uuid4(),
            signal_id=uuid4(),
            status=AssignmentStatus.NEW,
            created_at=now,
            updated_at=now,
        )
        assert assignment.status == AssignmentStatus.NEW
        assert assignment.assignee_id is None

    def test_assignment_with_assignee(self) -> None:
        """Test Assignment model with assignee."""
        now = datetime.now(tz=UTC)
        user_id = uuid4()
        assigner_id = uuid4()
        assignment = Assignment(
            id=uuid4(),
            signal_id=uuid4(),
            assignee_id=user_id,
            assigner_id=assigner_id,
            status=AssignmentStatus.ASSIGNED,
            role_type=AssignmentRoleType.CLINICAL_LEADERSHIP,
            notes="Initial assignment",
            assigned_at=now,
            created_at=now,
            updated_at=now,
        )
        assert assignment.assignee_id == user_id
        assert assignment.assigner_id == assigner_id
        assert assignment.status == AssignmentStatus.ASSIGNED
        assert assignment.role_type == AssignmentRoleType.CLINICAL_LEADERSHIP


class TestActivityEventModel:
    """Tests for ActivityEvent model."""

    def test_activity_event_table_name(self) -> None:
        """Verify ActivityEvent model table name."""
        assert ActivityEvent.__tablename__ == "activity_events"

    def test_activity_event_instantiation(self) -> None:
        """Test ActivityEvent model can be instantiated."""
        now = datetime.now(tz=UTC)
        event = ActivityEvent(
            id=uuid4(),
            event_type=EventType.NEW_SIGNAL,
            signal_id=uuid4(),
            payload={"classification": "critical", "sub_classification": "crisis_acute", "metric": "losIndex"},
            read=False,
            created_at=now,
        )
        assert event.event_type == EventType.NEW_SIGNAL
        assert event.read is False
        assert event.payload["classification"] == "critical"

    def test_activity_event_user_event(self) -> None:
        """Test ActivityEvent model for user-triggered event."""
        now = datetime.now(tz=UTC)
        user_id = uuid4()
        event = ActivityEvent(
            id=uuid4(),
            event_type=EventType.COMMENT,
            signal_id=uuid4(),
            user_id=user_id,
            payload={"comment": "Looking into this issue"},
            read=False,
            created_at=now,
        )
        assert event.event_type == EventType.COMMENT
        assert event.user_id == user_id


@pytest.mark.parametrize(
    ("domain", "expected_value"),
    [
        (SignalDomain.EFFICIENCY, "Efficiency"),
        (SignalDomain.SAFETY, "Safety"),
        (SignalDomain.EFFECTIVENESS, "Effectiveness"),
    ],
)
def test_domain_values(domain: SignalDomain, expected_value: str) -> None:
    """Test each domain enum value."""
    assert domain.value == expected_value
