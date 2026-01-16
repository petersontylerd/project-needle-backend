"""Fix signal_sub_classification storage type.

This migration converts the signal_sub_classification column from a native
PostgreSQL enum to VARCHAR, matching the SQLAlchemy model's native_enum=False
setting. This ensures compatibility between the database schema and the
SQLAlchemy ORM.

Background:
- Migration 011 created a native PostgreSQL enum with lowercase values
- The SQLAlchemy model uses native_enum=False which stores enum names (uppercase)
- This caused LookupError at runtime when reading existing data

Changes:
- Convert signal_sub_classification from native enum to VARCHAR(50)
- Transform existing lowercase values to uppercase
- Drop the signalsubclassification enum type

Revision ID: 012_fix_signal_sub_classification_storage
Revises: 011_add_signal_sub_classification
Create Date: 2024-12-18

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "012_fix_signal_sub_classification_storage"
down_revision: str | None = "011_add_signal_sub_classification"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Convert native enum to VARCHAR for SQLAlchemy compatibility."""
    # Step 1: Drop the index (required before altering column type)
    op.drop_index("ix_signals_sub_classification", table_name="signals")

    # Step 2: Convert column from native enum to VARCHAR
    op.execute("""
        ALTER TABLE signals
        ALTER COLUMN signal_sub_classification TYPE VARCHAR(50)
        USING signal_sub_classification::text
    """)

    # Step 3: Transform existing values to uppercase (SQLAlchemy native_enum=False stores names)
    op.execute("""
        UPDATE signals
        SET signal_sub_classification = UPPER(signal_sub_classification)
        WHERE signal_sub_classification IS NOT NULL
    """)

    # Step 4: Drop the now-unused native enum type
    op.execute("DROP TYPE IF EXISTS signalsubclassification")

    # Step 5: Recreate the index
    op.create_index(
        "ix_signals_sub_classification",
        "signals",
        ["signal_sub_classification"],
        unique=False,
    )


def downgrade() -> None:
    """Revert to native PostgreSQL enum (not recommended)."""
    from sqlalchemy.dialects import postgresql

    # Step 1: Drop the index
    op.drop_index("ix_signals_sub_classification", table_name="signals")

    # Step 2: Transform values back to lowercase
    op.execute("""
        UPDATE signals
        SET signal_sub_classification = LOWER(signal_sub_classification)
        WHERE signal_sub_classification IS NOT NULL
    """)

    # Step 3: Recreate the enum type
    signal_sub_classification_values = [
        "concentrated_driver",
        "systemic_poor",
        "rapid_decline",
        "gradual_decline",
        "sustained_recovery",
        "early_improvement",
        "data_quality_suspect",
        "transient_spike",
        "stable_performer",
        "watch_list",
    ]
    signal_sub_classification_enum = postgresql.ENUM(
        *signal_sub_classification_values,
        name="signalsubclassification",
    )
    signal_sub_classification_enum.create(op.get_bind(), checkfirst=True)

    # Step 4: Convert column back to native enum
    op.execute("""
        ALTER TABLE signals
        ALTER COLUMN signal_sub_classification TYPE signalsubclassification
        USING signal_sub_classification::signalsubclassification
    """)

    # Step 5: Recreate the index
    op.create_index(
        "ix_signals_sub_classification",
        "signals",
        ["signal_sub_classification"],
        unique=False,
    )
