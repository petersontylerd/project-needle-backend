"""Workflow API router - Assignment and status transition endpoints.

This module provides REST endpoints for managing signal assignments
and workflow status transitions.
"""

import logging
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.db.models import (
    ActivityEvent,
    Assignment,
    AssignmentRoleType,
    AssignmentStatus,
    EventType,
    Signal,
    User,
)
from src.db.session import get_async_db_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/signals", tags=["workflow"])


# =============================================================================
# Type Aliases for Dependencies
# =============================================================================

DbSession = Annotated[AsyncSession, Depends(get_async_db_session)]


# =============================================================================
# Request/Response Schemas
# =============================================================================


class AssignmentRequest(BaseModel):
    """Request body for assigning a signal.

    Attributes:
        assignee_id: UUID of the user to assign the signal to.
        role_type: Role type for the assignment.
        notes: Optional notes about the assignment.

    Example:
        >>> request = AssignmentRequest(
        ...     assignee_id=uuid4(),
        ...     role_type=AssignmentRoleType.CLINICAL_LEADERSHIP,
        ...     notes="Please review this signal",
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    assignee_id: UUID = Field(description="User ID to assign to")
    role_type: AssignmentRoleType = Field(description="Role type for assignment")
    notes: str | None = Field(default=None, description="Assignment notes")


class StatusUpdateRequest(BaseModel):
    """Request body for updating signal status.

    Attributes:
        status: New status for the signal.
        notes: Optional notes about the status change.

    Example:
        >>> request = StatusUpdateRequest(
        ...     status=AssignmentStatus.IN_PROGRESS,
        ...     notes="Starting investigation",
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    status: AssignmentStatus = Field(description="New status")
    notes: str | None = Field(default=None, description="Status change notes")


class AssignmentResponse(BaseModel):
    """Response for assignment operations.

    Attributes:
        id: Assignment UUID.
        signal_id: UUID of the assigned signal.
        assignee_id: UUID of the assigned user.
        assigner_id: UUID of the user who made the assignment.
        status: Current workflow status.
        role_type: Role type for the assignment.
        notes: Assignment notes.
        assigned_at: When the assignment was made.
        created_at: Record creation timestamp.

    Example:
        >>> response = AssignmentResponse(
        ...     id=uuid4(),
        ...     signal_id=uuid4(),
        ...     assignee_id=uuid4(),
        ...     status=AssignmentStatus.ASSIGNED,
        ...     role_type=AssignmentRoleType.CLINICAL_LEADERSHIP,
        ...     assigned_at=datetime.now(tz=UTC),
        ...     created_at=datetime.now(tz=UTC),
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    signal_id: str
    assignee_id: str | None
    assigner_id: str | None
    status: AssignmentStatus
    role_type: AssignmentRoleType | None
    notes: str | None
    assigned_at: datetime | None
    started_at: datetime | None
    resolved_at: datetime | None
    closed_at: datetime | None
    created_at: datetime


def _assignment_to_response(assignment: Assignment) -> AssignmentResponse:
    """Convert an Assignment model to AssignmentResponse schema.

    Args:
        assignment: Assignment ORM model instance.

    Returns:
        AssignmentResponse: Pydantic response model.
    """
    return AssignmentResponse(
        id=str(assignment.id),
        signal_id=str(assignment.signal_id),
        assignee_id=str(assignment.assignee_id) if assignment.assignee_id else None,
        assigner_id=str(assignment.assigner_id) if assignment.assigner_id else None,
        status=assignment.status,
        role_type=assignment.role_type,
        notes=assignment.notes,
        assigned_at=assignment.assigned_at,
        started_at=assignment.started_at,
        resolved_at=assignment.resolved_at,
        closed_at=assignment.closed_at,
        created_at=assignment.created_at,
    )


# =============================================================================
# Status Transition Validation
# =============================================================================

# Valid status transitions: from_status -> [allowed_to_statuses]
VALID_TRANSITIONS: dict[AssignmentStatus, list[AssignmentStatus]] = {
    AssignmentStatus.NEW: [AssignmentStatus.ASSIGNED],
    AssignmentStatus.ASSIGNED: [AssignmentStatus.IN_PROGRESS, AssignmentStatus.NEW],
    AssignmentStatus.IN_PROGRESS: [AssignmentStatus.RESOLVED, AssignmentStatus.ASSIGNED],
    AssignmentStatus.RESOLVED: [AssignmentStatus.CLOSED, AssignmentStatus.IN_PROGRESS],
    AssignmentStatus.CLOSED: [],  # Closed is final
}


def _validate_status_transition(current_status: AssignmentStatus, new_status: AssignmentStatus) -> None:
    """Validate that a status transition is allowed.

    Args:
        current_status: Current assignment status.
        new_status: Requested new status.

    Raises:
        HTTPException: 400 if the transition is not allowed.
    """
    allowed = VALID_TRANSITIONS.get(current_status, [])
    if new_status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot transition from {current_status.value} to {new_status.value}. Allowed transitions: {[s.value for s in allowed]}",
        )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/{signal_id}/assign", response_model=AssignmentResponse)
async def assign_signal(
    signal_id: UUID,
    request: AssignmentRequest,
    session: DbSession,
) -> AssignmentResponse:
    """Assign a signal to a user.

    Creates a new assignment or updates an existing one. Creates an
    assignment activity event.

    Args:
        signal_id: UUID of the signal to assign.
        request: Assignment request with assignee and role type.
        session: Database session (injected).

    Returns:
        AssignmentResponse: The created or updated assignment.

    Raises:
        HTTPException: 404 if signal or assignee not found.

    Example:
        >>> POST /api/signals/{signal_id}/assign
        >>> Body: {"assignee_id": "...", "role_type": "clinical_leadership"}
    """
    # Verify signal exists
    signal_query = select(Signal).options(joinedload(Signal.assignment)).where(Signal.id == signal_id)
    signal_result = await session.execute(signal_query)
    signal = signal_result.scalars().first()

    if not signal:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    # Verify assignee exists
    user_query = select(User).where(User.id == request.assignee_id)
    user_result = await session.execute(user_query)
    assignee = user_result.scalars().first()

    if not assignee:
        raise HTTPException(status_code=404, detail=f"User not found: {request.assignee_id}")

    now = datetime.now(tz=UTC)

    # Create or update assignment
    if signal.assignment:
        # Update existing assignment
        assignment = signal.assignment
        assignment.assignee_id = request.assignee_id
        assignment.role_type = request.role_type
        assignment.notes = request.notes
        assignment.status = AssignmentStatus.ASSIGNED
        assignment.assigned_at = now
        assignment.updated_at = now
    else:
        # Create new assignment
        assignment = Assignment(
            signal_id=signal_id,
            assignee_id=request.assignee_id,
            role_type=request.role_type,
            notes=request.notes,
            status=AssignmentStatus.ASSIGNED,
            assigned_at=now,
        )
        session.add(assignment)

    # Create activity event
    event = ActivityEvent(
        event_type=EventType.ASSIGNMENT,
        signal_id=signal_id,
        payload={
            "assignee_id": str(request.assignee_id),
            "assignee_name": assignee.name,
            "role_type": request.role_type.value,
            "notes": request.notes,
        },
    )
    session.add(event)

    await session.flush()
    await session.refresh(assignment)

    # Audit log for security/compliance
    logger.info(
        "Signal assigned: signal_id=%s, assignee_id=%s, role_type=%s, timestamp=%s",
        signal_id,
        request.assignee_id,
        request.role_type.value,
        now.isoformat(),
    )

    return _assignment_to_response(assignment)


@router.patch("/{signal_id}/status", response_model=AssignmentResponse)
async def update_signal_status(
    signal_id: UUID,
    request: StatusUpdateRequest,
    session: DbSession,
) -> AssignmentResponse:
    """Update the workflow status of a signal.

    Validates the status transition and updates the assignment.
    Creates a status change activity event.

    Args:
        signal_id: UUID of the signal to update.
        request: Status update request with new status.
        session: Database session (injected).

    Returns:
        AssignmentResponse: The updated assignment.

    Raises:
        HTTPException: 404 if signal or assignment not found.
        HTTPException: 400 if status transition is not valid.

    Example:
        >>> PATCH /api/signals/{signal_id}/status
        >>> Body: {"status": "in_progress", "notes": "Starting investigation"}
    """
    # Verify signal exists with assignment
    signal_query = select(Signal).options(joinedload(Signal.assignment)).where(Signal.id == signal_id)
    signal_result = await session.execute(signal_query)
    signal = signal_result.scalars().first()

    if not signal:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    if not signal.assignment:
        raise HTTPException(
            status_code=404,
            detail=f"No assignment found for signal: {signal_id}. Assign the signal first.",
        )

    assignment = signal.assignment

    # Validate transition
    _validate_status_transition(assignment.status, request.status)

    old_status = assignment.status
    now = datetime.now(tz=UTC)

    # Update assignment
    assignment.status = request.status
    assignment.updated_at = now

    # Update status-specific timestamps
    if request.status == AssignmentStatus.IN_PROGRESS:
        assignment.started_at = now
    elif request.status == AssignmentStatus.RESOLVED:
        assignment.resolved_at = now
        assignment.resolution_notes = request.notes
    elif request.status == AssignmentStatus.CLOSED:
        assignment.closed_at = now

    # Create activity event
    event = ActivityEvent(
        event_type=EventType.STATUS_CHANGE,
        signal_id=signal_id,
        payload={
            "old_status": old_status.value,
            "new_status": request.status.value,
            "notes": request.notes,
        },
    )
    session.add(event)

    await session.flush()
    await session.refresh(assignment)

    # Audit log for security/compliance
    logger.info(
        "Signal status updated: signal_id=%s, old_status=%s, new_status=%s, timestamp=%s",
        signal_id,
        old_status.value,
        request.status.value,
        now.isoformat(),
    )

    return _assignment_to_response(assignment)


@router.get("/{signal_id}/assignment", response_model=AssignmentResponse)
async def get_signal_assignment(
    signal_id: UUID,
    session: DbSession,
) -> AssignmentResponse:
    """Get the current assignment for a signal.

    Args:
        signal_id: UUID of the signal.
        session: Database session (injected).

    Returns:
        AssignmentResponse: The current assignment.

    Raises:
        HTTPException: 404 if signal or assignment not found.

    Example:
        >>> GET /api/signals/{signal_id}/assignment
    """
    signal_query = select(Signal).options(joinedload(Signal.assignment)).where(Signal.id == signal_id)
    signal_result = await session.execute(signal_query)
    signal = signal_result.scalars().first()

    if not signal:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    if not signal.assignment:
        raise HTTPException(
            status_code=404,
            detail=f"No assignment found for signal: {signal_id}",
        )

    return _assignment_to_response(signal.assignment)
