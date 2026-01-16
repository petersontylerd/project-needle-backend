"""Users API router - User listing for assignment workflows.

This module provides REST endpoints for listing users available
for signal assignment.
"""

from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import User
from src.db.session import get_async_db_session

router = APIRouter(prefix="/users", tags=["users"])

DbSession = Annotated[AsyncSession, Depends(get_async_db_session)]


class UserResponse(BaseModel):
    """Public user information for assignment UI.

    Attributes:
        id: User UUID as string.
        name: Display name.
        email: Email address.
        role: User role/title.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    email: str
    role: str | None


@router.get("", response_model=list[UserResponse])
async def list_users(session: DbSession) -> list[UserResponse]:
    """List all users available for assignment.

    Returns:
        List of users with public information only.

    Example:
        >>> GET /api/users
        >>> [{"id": "...", "name": "Sarah Chen", "email": "...", "role": "Clinical Director"}]
    """
    query = select(User).order_by(User.name)
    result = await session.execute(query)
    users = result.scalars().all()

    return [
        UserResponse(
            id=str(user.id),
            name=user.name,
            email=user.email,
            role=user.role.value if user.role else None,
        )
        for user in users
    ]
