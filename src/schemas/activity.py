"""Activity event schemas for API responses.

This module defines Pydantic models for activity feed responses,
following the feed events specification.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.db.models import EventType


class ActivityEventResponse(BaseModel):
    """Response schema for activity events.

    Attributes:
        id: Unique identifier for the event.
        event_type: Type of event (new_signal, assignment, status_change, etc.).
        signal_id: UUID of the related signal, if applicable.
        user_id: UUID of the user who triggered the event (null for system events).
        payload: Event-specific data as a JSON object.
        read: Whether the event has been read by the user.
        created_at: Timestamp when the event was created.

    Example:
        >>> response = ActivityEventResponse(
        ...     id="550e8400-e29b-41d4-a716-446655440000",
        ...     event_type=EventType.NEW_SIGNAL,
        ...     signal_id="660e8400-e29b-41d4-a716-446655440001",
        ...     user_id=None,
        ...     payload={"signal_type": "critical_trajectory", "severity": 85, "metric_id": "losIndex"},
        ...     read=False,
        ...     created_at=datetime.now(),
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(description="Event UUID")
    event_type: EventType = Field(description="Type of activity event")
    signal_id: str | None = Field(default=None, description="Related signal UUID")
    user_id: str | None = Field(default=None, description="Actor user UUID (null for system)")
    payload: dict[str, Any] = Field(default_factory=dict, description="Event-specific data")
    read: bool = Field(default=False, description="Whether event has been read")
    created_at: datetime = Field(description="Event timestamp")


class FeedResponse(BaseModel):
    """Paginated feed response with cursor-based pagination.

    Attributes:
        events: List of activity events in the current page.
        cursor: Opaque cursor for fetching the next page (null if no more).
        has_more: Whether more events exist beyond this page.
        total_count: Total number of events matching the query filters.

    Example:
        >>> response = FeedResponse(
        ...     events=[event1, event2],
        ...     cursor="2025-12-11T10:00:00Z",
        ...     has_more=True,
        ...     total_count=100,
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    events: list[ActivityEventResponse] = Field(description="Activity events")
    cursor: str | None = Field(default=None, description="Cursor for next page")
    has_more: bool = Field(default=False, description="Whether more events exist")
    total_count: int = Field(description="Total matching events")
