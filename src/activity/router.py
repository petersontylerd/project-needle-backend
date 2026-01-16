"""Activity feed API router - Endpoints for activity feed events.

This module provides REST endpoints for retrieving activity feed events
with filtering and cursor-based pagination support.
"""

import logging
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import ActivityEvent, EventType
from src.db.session import get_async_db_session
from src.schemas.activity import ActivityEventResponse, FeedResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/feed", tags=["activity"])


# =============================================================================
# Type Aliases for Dependencies
# =============================================================================

DbSession = Annotated[AsyncSession, Depends(get_async_db_session)]

# Query parameter annotations
SignalIdFilter = Annotated[UUID | None, Query(description="Filter events by signal UUID")]
EventTypeFilter = Annotated[EventType | None, Query(description="Filter by event type")]
LimitQuery = Annotated[int, Query(ge=1, le=100, description="Number of events per page")]
CursorQuery = Annotated[str | None, Query(description="Cursor for pagination (ISO datetime string)")]


# =============================================================================
# Response Helpers
# =============================================================================


def _event_to_response(event: ActivityEvent) -> ActivityEventResponse:
    """Convert an ActivityEvent model to ActivityEventResponse schema.

    Args:
        event: ActivityEvent ORM model instance.

    Returns:
        ActivityEventResponse: Pydantic response model.
    """
    return ActivityEventResponse(
        id=str(event.id),
        event_type=event.event_type,
        signal_id=str(event.signal_id) if event.signal_id else None,
        user_id=str(event.user_id) if event.user_id else None,
        payload=event.payload,
        read=event.read,
        created_at=event.created_at,
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.get("", response_model=FeedResponse)
async def get_feed(
    session: DbSession,
    signal_id: SignalIdFilter = None,
    event_type: EventTypeFilter = None,
    limit: LimitQuery = 20,
    cursor: CursorQuery = None,
) -> FeedResponse:
    """Get activity feed events with optional filtering.

    Retrieves activity events with support for filtering by signal_id
    and event_type. Uses cursor-based pagination for efficient scrolling.

    Args:
        session: Database session (injected).
        signal_id: Filter events related to a specific signal.
        event_type: Filter by event type.
        limit: Number of events per page (default 20, max 100).
        cursor: Pagination cursor (ISO datetime string from previous response).

    Returns:
        FeedResponse: Paginated list of activity events.

    Example:
        >>> GET /api/feed?limit=20
        >>> GET /api/feed?signal_id=550e8400-e29b-41d4-a716-446655440000
        >>> GET /api/feed?event_type=assignment&limit=10
        >>> GET /api/feed?cursor=2025-12-11T10:00:00Z
    """
    # Build base query
    query = select(ActivityEvent)

    # Apply filters
    if signal_id:
        query = query.where(ActivityEvent.signal_id == signal_id)
    if event_type:
        query = query.where(ActivityEvent.event_type == event_type)

    # Apply cursor for pagination (events before cursor timestamp)
    if cursor:
        try:
            cursor_dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
            query = query.where(ActivityEvent.created_at < cursor_dt)
        except ValueError as e:
            # Invalid cursor format - return error instead of silently ignoring
            logger.warning("Invalid cursor format received: %s", cursor)
            raise HTTPException(
                status_code=400,
                detail=f"Invalid cursor format. Expected ISO 8601 datetime, got: {cursor}",
            ) from e

    # Get total count (without pagination/cursor)
    count_query = select(func.count()).select_from(ActivityEvent)
    if signal_id:
        count_query = count_query.where(ActivityEvent.signal_id == signal_id)
    if event_type:
        count_query = count_query.where(ActivityEvent.event_type == event_type)

    total_count = await session.scalar(count_query) or 0

    # Order by created_at descending (newest first) and apply limit
    query = query.order_by(ActivityEvent.created_at.desc()).limit(limit + 1)

    result = await session.execute(query)
    events = list(result.scalars().all())

    # Check if there are more results
    has_more = len(events) > limit
    if has_more:
        events = events[:limit]  # Remove the extra event

    # Build cursor for next page
    next_cursor: str | None = None
    if events and has_more:
        last_event = events[-1]
        next_cursor = last_event.created_at.isoformat()

    return FeedResponse(
        events=[_event_to_response(event) for event in events],
        cursor=next_cursor,
        has_more=has_more,
        total_count=total_count,
    )


@router.get("/unread-count", response_model=dict[str, int])
async def get_unread_count(
    session: DbSession,
    signal_id: SignalIdFilter = None,
) -> dict[str, int]:
    """Get count of unread activity events.

    Returns the number of unread events, optionally filtered by signal.

    Args:
        session: Database session (injected).
        signal_id: Filter to count unread events for a specific signal.

    Returns:
        dict: Object with "count" key containing unread event count.

    Example:
        >>> GET /api/feed/unread-count
        >>> {"count": 5}
    """
    query = (
        select(func.count())
        .select_from(ActivityEvent)
        .where(
            ActivityEvent.read == False  # noqa: E712 - SQLAlchemy requires == False
        )
    )

    if signal_id:
        query = query.where(ActivityEvent.signal_id == signal_id)

    count = await session.scalar(query) or 0
    return {"count": count}


@router.post("/{event_id}/mark-read", response_model=ActivityEventResponse)
async def mark_event_read(
    event_id: UUID,
    session: DbSession,
) -> ActivityEventResponse:
    """Mark an activity event as read.

    Updates the read status of an event to True.

    Args:
        event_id: UUID of the event to mark as read.
        session: Database session (injected).

    Returns:
        ActivityEventResponse: Updated event.

    Raises:
        HTTPException: 404 if event not found.

    Example:
        >>> POST /api/feed/550e8400-e29b-41d4-a716-446655440000/mark-read
    """
    from fastapi import HTTPException

    query = select(ActivityEvent).where(ActivityEvent.id == event_id)
    result = await session.execute(query)
    event = result.scalars().first()

    if not event:
        raise HTTPException(status_code=404, detail=f"Event not found: {event_id}")

    event.read = True
    await session.flush()
    await session.refresh(event)

    return _event_to_response(event)
