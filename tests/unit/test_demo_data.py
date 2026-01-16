"""Tests for the demo seed data generation functions.

This module tests the pure functions in demo_data.py that create
demo users, signals, assignments, and activity events.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from src.db.models import (
    AssignmentRoleType,
    AssignmentStatus,
    EventType,
    SignalDomain,
    UserRole,
)
from src.db.seeds.demo_data import (
    _days_ago,
    _hours_ago,
    _now,
    create_demo_activity_events,
    create_demo_assignments,
    create_demo_signals,
    create_demo_users,
)

pytestmark = pytest.mark.tier1


class TestHelperFunctions:
    """Tests for time helper functions."""

    def test_now_returns_utc_datetime(self) -> None:
        """Test _now returns a UTC datetime."""
        result = _now()
        assert result.tzinfo == UTC
        # Should be very close to now
        diff = abs((datetime.now(tz=UTC) - result).total_seconds())
        assert diff < 1

    def test_days_ago_returns_past_date(self) -> None:
        """Test _days_ago returns correct past date."""
        result = _days_ago(5)
        expected = datetime.now(tz=UTC) - timedelta(days=5)
        diff = abs((expected - result).total_seconds())
        assert diff < 1

    def test_days_ago_zero(self) -> None:
        """Test _days_ago(0) returns approximately now."""
        result = _days_ago(0)
        diff = abs((datetime.now(tz=UTC) - result).total_seconds())
        assert diff < 1

    def test_hours_ago_returns_past_time(self) -> None:
        """Test _hours_ago returns correct past time."""
        result = _hours_ago(3)
        expected = datetime.now(tz=UTC) - timedelta(hours=3)
        diff = abs((expected - result).total_seconds())
        assert diff < 1


class TestCreateDemoUsers:
    """Tests for create_demo_users function."""

    def test_creates_three_users(self) -> None:
        """Test that create_demo_users returns 3 users."""
        users = create_demo_users()
        assert len(users) == 3

    def test_users_have_unique_ids(self) -> None:
        """Test that users have unique UUIDs."""
        users = create_demo_users()
        ids = [user.id for user in users]
        assert len(ids) == len(set(ids))

    def test_users_have_preset_uuids(self) -> None:
        """Test that users have the expected preset UUIDs."""
        users = create_demo_users()
        expected_ids = [
            UUID("00000000-0000-0000-0000-000000000001"),
            UUID("00000000-0000-0000-0000-000000000002"),
            UUID("00000000-0000-0000-0000-000000000003"),
        ]
        actual_ids = [user.id for user in users]
        assert actual_ids == expected_ids

    def test_users_have_different_roles(self) -> None:
        """Test that users have different roles."""
        users = create_demo_users()
        roles = [user.role for user in users]
        assert UserRole.CLINICAL_LEADERSHIP in roles
        assert UserRole.NURSING_LEADERSHIP in roles
        assert UserRole.ADMINISTRATION in roles

    def test_users_have_valid_emails(self) -> None:
        """Test that users have valid email addresses."""
        users = create_demo_users()
        for user in users:
            assert "@" in user.email
            assert ".example" in user.email

    def test_users_are_active(self) -> None:
        """Test that all users are active."""
        users = create_demo_users()
        for user in users:
            assert user.is_active is True


class TestCreateDemoSignals:
    """Tests for create_demo_signals function."""

    def test_creates_ten_signals(self) -> None:
        """Test that create_demo_signals returns 10 signals."""
        signals = create_demo_signals()
        assert len(signals) == 10

    def test_signals_have_unique_ids(self) -> None:
        """Test that signals have unique UUIDs."""
        signals = create_demo_signals()
        ids = [signal.id for signal in signals]
        assert len(ids) == len(set(ids))

    def test_signals_have_all_domains(self) -> None:
        """Test signals cover all domains."""
        signals = create_demo_signals()
        domains = {signal.domain for signal in signals}
        assert SignalDomain.EFFICIENCY in domains
        assert SignalDomain.SAFETY in domains
        assert SignalDomain.EFFECTIVENESS in domains

    def test_signals_have_descriptions(self) -> None:
        """Test all signals have descriptions."""
        signals = create_demo_signals()
        for signal in signals:
            assert signal.description is not None
            assert len(signal.description) > 10

    def test_signals_have_numeric_values(self) -> None:
        """Test signals have metric and peer_mean values."""
        signals = create_demo_signals()
        for signal in signals:
            assert signal.metric_value is not None
            assert signal.peer_mean is not None


class TestCreateDemoAssignments:
    """Tests for create_demo_assignments function."""

    def test_creates_ten_assignments(self) -> None:
        """Test that create_demo_assignments returns 10 assignments."""
        users = create_demo_users()
        signals = create_demo_signals()
        assignments = create_demo_assignments(signals, users)
        assert len(assignments) == 10

    def test_assignments_have_unique_ids(self) -> None:
        """Test that assignments have unique UUIDs."""
        users = create_demo_users()
        signals = create_demo_signals()
        assignments = create_demo_assignments(signals, users)
        ids = [a.id for a in assignments]
        assert len(ids) == len(set(ids))

    def test_assignments_have_expected_status_distribution(self) -> None:
        """Test assignments have expected status distribution."""
        users = create_demo_users()
        signals = create_demo_signals()
        assignments = create_demo_assignments(signals, users)
        statuses = [a.status for a in assignments]
        assert statuses.count(AssignmentStatus.NEW) == 2
        assert statuses.count(AssignmentStatus.ASSIGNED) == 3
        assert statuses.count(AssignmentStatus.IN_PROGRESS) == 2
        assert statuses.count(AssignmentStatus.RESOLVED) == 2
        assert statuses.count(AssignmentStatus.CLOSED) == 1

    def test_assignments_reference_signals(self) -> None:
        """Test assignments reference valid signal IDs."""
        users = create_demo_users()
        signals = create_demo_signals()
        assignments = create_demo_assignments(signals, users)
        signal_ids = {signal.id for signal in signals}
        for assignment in assignments:
            assert assignment.signal_id in signal_ids

    def test_non_new_assignments_have_role_types(self) -> None:
        """Test non-NEW assignments have valid role types."""
        users = create_demo_users()
        signals = create_demo_signals()
        assignments = create_demo_assignments(signals, users)
        for assignment in assignments:
            # NEW assignments may not have role_type yet
            if assignment.status != AssignmentStatus.NEW:
                assert assignment.role_type in AssignmentRoleType

    def test_resolved_assignments_have_resolution_notes(self) -> None:
        """Test resolved/closed assignments have resolution notes."""
        users = create_demo_users()
        signals = create_demo_signals()
        assignments = create_demo_assignments(signals, users)
        for assignment in assignments:
            if assignment.status in (AssignmentStatus.RESOLVED, AssignmentStatus.CLOSED):
                assert assignment.resolution_notes is not None


class TestCreateDemoActivityEvents:
    """Tests for create_demo_activity_events function."""

    def test_creates_twenty_events(self) -> None:
        """Test that create_demo_activity_events returns 20 events."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        assert len(events) == 20

    def test_events_have_unique_ids(self) -> None:
        """Test that events have unique UUIDs."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        ids = [e.id for e in events]
        assert len(ids) == len(set(ids))

    def test_events_cover_multiple_types(self) -> None:
        """Test events cover multiple event types."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        event_types = {e.event_type for e in events}
        # Should have at least these event types
        assert EventType.NEW_SIGNAL in event_types
        assert EventType.ASSIGNMENT in event_types
        assert EventType.STATUS_CHANGE in event_types

    def test_events_reference_valid_signals(self) -> None:
        """Test events with signal_id reference valid signal IDs."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        signal_ids = {signal.id for signal in signals}
        for event in events:
            # Some events (like technical_error) may not have signal_id
            if event.signal_id is not None:
                assert event.signal_id in signal_ids

    def test_events_reference_valid_users(self) -> None:
        """Test events with user_id reference valid user IDs."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        user_ids = {user.id for user in users}
        for event in events:
            # Some events may not have user_id
            if event.user_id is not None:
                assert event.user_id in user_ids

    def test_events_have_payloads(self) -> None:
        """Test all events have payload dictionaries."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        for event in events:
            assert event.payload is not None
            assert isinstance(event.payload, dict)

    def test_events_have_created_at(self) -> None:
        """Test all events have created_at timestamps."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        for event in events:
            assert event.created_at is not None
            assert event.created_at.tzinfo == UTC

    def test_some_events_are_unread(self) -> None:
        """Test some events are marked as unread."""
        users = create_demo_users()
        signals = create_demo_signals()
        events = create_demo_activity_events(signals, users)
        read_values = [e.read for e in events]
        assert False in read_values  # At least some unread
