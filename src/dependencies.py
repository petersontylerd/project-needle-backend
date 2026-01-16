"""Shared FastAPI dependencies.

Provides type aliases and reusable dependencies for route handlers.
"""

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_async_db_session

# Type alias for cleaner route signatures
DbSession = Annotated[AsyncSession, Depends(get_async_db_session)]
"""Database session dependency type alias.

Use this in route handlers for automatic session injection:

    @router.get("/items")
    async def list_items(db: DbSession) -> list[Item]:
        ...
"""
