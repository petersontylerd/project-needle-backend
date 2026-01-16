"""Unit tests for activity feed router.

These tests verify the feed router logic without making actual database calls.
"""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from src.db.models import ActivityEvent, EventType
from src.schemas.activity import ActivityEventResponse, FeedResponse

pytestmark = pytest.mark.tier1

# =============================================================================
# Schema Tests
# =============================================================================


class TestActivityEventResponseSchema:
    """Tests for ActivityEventResponse Pydantic schema."""

    def test_schema_from_orm_object(self) -> None:
        """Test schema can be created from ORM-like attributes."""
        event_id = str(uuid4())
        signal_id = str(uuid4())
        created_at = datetime.now(tz=UTC)

        response = ActivityEventResponse(
            id=event_id,
            event_type=EventType.NEW_SIGNAL,
            signal_id=signal_id,
            user_id=None,
            payload={"classification": "critical", "sub_classification": "crisis_acute"},
            read=False,
            created_at=created_at,
        )

        assert response.id == event_id
        assert response.event_type == EventType.NEW_SIGNAL
        assert response.signal_id == signal_id
        assert response.user_id is None
        assert response.payload == {"classification": "critical", "sub_classification": "crisis_acute"}
        assert response.read is False
        assert response.created_at == created_at

    def test_schema_with_user_id(self) -> None:
        """Test schema with user_id for user-triggered events."""
        user_id = str(uuid4())

        response = ActivityEventResponse(
            id=str(uuid4()),
            event_type=EventType.ASSIGNMENT,
            signal_id=str(uuid4()),
            user_id=user_id,
            payload={"assignee_name": "Dr. Chen"},
            read=True,
            created_at=datetime.now(tz=UTC),
        )

        assert response.user_id == user_id
        assert response.event_type == EventType.ASSIGNMENT

    def test_schema_serialization(self) -> None:
        """Test schema serializes to dict correctly."""
        response = ActivityEventResponse(
            id=str(uuid4()),
            event_type=EventType.STATUS_CHANGE,
            signal_id=str(uuid4()),
            user_id=None,
            payload={"from_status": "assigned", "to_status": "in_progress"},
            read=False,
            created_at=datetime.now(tz=UTC),
        )

        data = response.model_dump()
        assert "id" in data
        assert "event_type" in data
        assert "signal_id" in data
        assert "user_id" in data
        assert "payload" in data
        assert "read" in data
        assert "created_at" in data

    @pytest.mark.parametrize(
        "event_type",
        [
            EventType.NEW_SIGNAL,
            EventType.REGRESSION,
            EventType.IMPROVEMENT,
            EventType.INSIGHT,
            EventType.TECHNICAL_ERROR,
            EventType.ASSIGNMENT,
            EventType.STATUS_CHANGE,
            EventType.COMMENT,
            EventType.INTERVENTION_ACTIVATED,
            EventType.INTERVENTION_OUTCOME,
        ],
    )
    def test_all_event_types_valid(self, event_type: EventType) -> None:
        """Test all event types are valid in schema.

        Args:
            event_type: Event type to test.
        """
        response = ActivityEventResponse(
            id=str(uuid4()),
            event_type=event_type,
            signal_id=str(uuid4()),
            user_id=None,
            payload={},
            read=False,
            created_at=datetime.now(tz=UTC),
        )

        assert response.event_type == event_type


class TestFeedResponseSchema:
    """Tests for FeedResponse Pydantic schema."""

    def test_feed_response_empty(self) -> None:
        """Test feed response with no events."""
        response = FeedResponse(
            events=[],
            cursor=None,
            has_more=False,
            total_count=0,
        )

        assert response.events == []
        assert response.cursor is None
        assert response.has_more is False
        assert response.total_count == 0

    def test_feed_response_with_events(self) -> None:
        """Test feed response with events."""
        event = ActivityEventResponse(
            id=str(uuid4()),
            event_type=EventType.NEW_SIGNAL,
            signal_id=str(uuid4()),
            user_id=None,
            payload={},
            read=False,
            created_at=datetime.now(tz=UTC),
        )

        response = FeedResponse(
            events=[event],
            cursor="2025-12-11T10:00:00Z",
            has_more=True,
            total_count=100,
        )

        assert len(response.events) == 1
        assert response.cursor == "2025-12-11T10:00:00Z"
        assert response.has_more is True
        assert response.total_count == 100

    def test_feed_response_serialization(self) -> None:
        """Test feed response serializes correctly."""
        response = FeedResponse(
            events=[],
            cursor=None,
            has_more=False,
            total_count=0,
        )

        data = response.model_dump()
        assert "events" in data
        assert "cursor" in data
        assert "has_more" in data
        assert "total_count" in data


# =============================================================================
# Router Helper Function Tests
# =============================================================================


class TestEventToResponseHelper:
    """Tests for _event_to_response helper function."""

    def test_converts_event_to_response(self) -> None:
        """Test conversion from ORM model to response schema."""
        from src.activity.router import _event_to_response

        # Create a mock event with all attributes
        event = ActivityEvent(
            id=uuid4(),
            event_type=EventType.ASSIGNMENT,
            signal_id=uuid4(),
            user_id=uuid4(),
            payload={"test": "data"},
            read=True,
            created_at=datetime.now(tz=UTC),
        )

        response = _event_to_response(event)

        assert response.id == str(event.id)
        assert response.event_type == event.event_type
        assert response.signal_id == str(event.signal_id)
        assert response.user_id == str(event.user_id)
        assert response.payload == event.payload
        assert response.read == event.read
        assert response.created_at == event.created_at

    def test_handles_null_signal_id(self) -> None:
        """Test conversion when signal_id is None."""
        from src.activity.router import _event_to_response

        event = ActivityEvent(
            id=uuid4(),
            event_type=EventType.TECHNICAL_ERROR,
            signal_id=None,
            user_id=None,
            payload={"error": "system failure"},
            read=False,
            created_at=datetime.now(tz=UTC),
        )

        response = _event_to_response(event)

        assert response.signal_id is None
        assert response.user_id is None

    def test_handles_null_user_id(self) -> None:
        """Test conversion for system events (no user)."""
        from src.activity.router import _event_to_response

        event = ActivityEvent(
            id=uuid4(),
            event_type=EventType.NEW_SIGNAL,
            signal_id=uuid4(),
            user_id=None,
            payload={},
            read=False,
            created_at=datetime.now(tz=UTC),
        )

        response = _event_to_response(event)

        assert response.user_id is None


# =============================================================================
# Event Type Enum Tests
# =============================================================================


class TestEventTypeEnum:
    """Tests for EventType enum values."""

    def test_event_type_values(self) -> None:
        """Test all expected event type values exist."""
        assert EventType.NEW_SIGNAL.value == "new_signal"
        assert EventType.REGRESSION.value == "regression"
        assert EventType.IMPROVEMENT.value == "improvement"
        assert EventType.INSIGHT.value == "insight"
        assert EventType.TECHNICAL_ERROR.value == "technical_error"
        assert EventType.ASSIGNMENT.value == "assignment"
        assert EventType.STATUS_CHANGE.value == "status_change"
        assert EventType.COMMENT.value == "comment"
        assert EventType.INTERVENTION_ACTIVATED.value == "intervention_activated"
        assert EventType.INTERVENTION_OUTCOME.value == "intervention_outcome"

    def test_event_type_count(self) -> None:
        """Test expected number of event types."""
        assert len(EventType) == 10


# =============================================================================
# Pagination Logic Tests
# =============================================================================


class TestPaginationLogic:
    """Tests for pagination cursor handling logic."""

    def test_cursor_datetime_parsing(self) -> None:
        """Test cursor datetime string parsing."""
        cursor = "2025-12-11T10:00:00+00:00"
        cursor_dt = datetime.fromisoformat(cursor)

        assert cursor_dt.year == 2025
        assert cursor_dt.month == 12
        assert cursor_dt.day == 11
        assert cursor_dt.hour == 10

    def test_cursor_with_z_suffix(self) -> None:
        """Test cursor with Z suffix parses correctly."""
        cursor = "2025-12-11T10:00:00Z"
        cursor_dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))

        assert cursor_dt.tzinfo is not None

    def test_invalid_cursor_raises_error(self) -> None:
        """Test invalid cursor format raises ValueError."""
        cursor = "invalid-cursor"

        with pytest.raises(ValueError):
            datetime.fromisoformat(cursor)
