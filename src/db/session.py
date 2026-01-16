"""Async SQLAlchemy session management.

Provides the async engine, session factory, and FastAPI dependency
for database session injection.
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.config import settings

# Create async engine with connection pooling
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,  # Log SQL statements in debug mode
    pool_pre_ping=True,  # Verify connections before use
)

# Session factory for creating new sessions
async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,  # Prevent lazy loading issues after commit
)


async def get_async_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide database session for FastAPI dependency injection.

    Creates a new async session for each request and handles
    commit/rollback automatically.

    Yields:
        AsyncSession: Database session that auto-commits on success
            and rolls back on exception.

    Example:
        >>> @router.get("/items")
        ... async def list_items(
        ...     db: Annotated[AsyncSession, Depends(get_async_db_session)]
        ... ):
        ...     result = await db.execute(select(Item))
        ...     return result.scalars().all()

    Raises:
        None: Exceptions are re-raised after rollback.
    """
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
