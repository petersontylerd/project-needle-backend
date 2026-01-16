"""SQLAlchemy 2.0 declarative base with async support.

Provides the Base class for all SQLAlchemy models with:
- AsyncAttrs for async attribute access
- Consistent naming conventions for indexes and constraints
- Timezone-aware datetime mapping
"""

import datetime

from sqlalchemy import DateTime, MetaData
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase


class Base(AsyncAttrs, DeclarativeBase):
    """Base class for all SQLAlchemy models.

    Includes:
        - AsyncAttrs for async attribute access on relationships
        - Consistent naming conventions for database objects
        - Timezone-aware datetime mapping

    Example:
        >>> class User(Base):
        ...     __tablename__ = "users"
        ...     id: Mapped[UUID] = mapped_column(primary_key=True)
        ...     name: Mapped[str]
    """

    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )
    type_annotation_map = {
        datetime.datetime: DateTime(timezone=True),
    }
