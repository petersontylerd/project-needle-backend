"""Tests for SQLAlchemy database models."""

from sqlalchemy.dialects.postgresql import JSONB


def test_signal_has_metadata_field() -> None:
    """Signal model includes metadata JSONB field."""
    from src.db.models import Signal

    assert hasattr(Signal, "metadata_")


def test_signal_metadata_field_uses_jsonb_type() -> None:
    """Signal.metadata_ column uses PostgreSQL JSONB type."""
    from src.db.models import Signal

    # Access the column through the model's table
    metadata_column = Signal.__table__.columns.get("metadata")
    assert metadata_column is not None, "metadata column should exist in Signal table"
    assert isinstance(metadata_column.type, JSONB), f"metadata should be JSONB, got {type(metadata_column.type)}"


def test_signal_metadata_field_is_nullable() -> None:
    """Signal.metadata_ column should be nullable."""
    from src.db.models import Signal

    metadata_column = Signal.__table__.columns.get("metadata")
    assert metadata_column is not None, "metadata column should exist in Signal table"
    assert metadata_column.nullable is True, "metadata column should be nullable"
