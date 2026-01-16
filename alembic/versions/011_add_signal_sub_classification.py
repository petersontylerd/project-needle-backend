"""Add signal sub-classification columns.

This migration adds columns for hierarchical signal sub-classification,
enabling more granular categorization within the 5 parent classification
classes for improved triage and action guidance.

Changes:
- Add signal_sub_classification enum type and column
- Add classification_priority_score for sorting within sub-class
- Add classification_confidence for data quality indicator
- Add classification_factors JSON for contributing factor details
- Add indexes for efficient querying

Revision ID: 011_add_signal_sub_classification
Revises: 010_consolidate_signal_schema
Create Date: 2024-12-18

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "011_add_signal_sub_classification"
down_revision: str | None = "010_consolidate_signal_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Define the enum values for signal sub-classification
SIGNAL_SUB_CLASSIFICATION_VALUES = [
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


def upgrade() -> None:
    """Add signal sub-classification columns."""
    # Step 1: Create the enum type
    signal_sub_classification_enum = postgresql.ENUM(
        *SIGNAL_SUB_CLASSIFICATION_VALUES,
        name="signalsubclassification",
    )
    signal_sub_classification_enum.create(op.get_bind(), checkfirst=True)

    # Step 2: Add the columns
    op.add_column(
        "signals",
        sa.Column(
            "signal_sub_classification",
            sa.Enum(
                *SIGNAL_SUB_CLASSIFICATION_VALUES,
                name="signalsubclassification",
            ),
            nullable=True,
        ),
    )

    op.add_column(
        "signals",
        sa.Column("classification_priority_score", sa.Integer(), nullable=True),
    )

    op.add_column(
        "signals",
        sa.Column("classification_confidence", sa.String(20), nullable=True),
    )

    op.add_column(
        "signals",
        sa.Column("classification_factors", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    # Step 3: Create indexes for efficient querying
    op.create_index(
        "ix_signals_sub_classification",
        "signals",
        ["signal_sub_classification"],
        unique=False,
    )

    op.create_index(
        "ix_signals_priority_score",
        "signals",
        ["classification_priority_score"],
        unique=False,
    )


def downgrade() -> None:
    """Remove signal sub-classification columns."""
    # Step 1: Drop indexes
    op.drop_index("ix_signals_priority_score", table_name="signals")
    op.drop_index("ix_signals_sub_classification", table_name="signals")

    # Step 2: Drop columns
    op.drop_column("signals", "classification_factors")
    op.drop_column("signals", "classification_confidence")
    op.drop_column("signals", "classification_priority_score")
    op.drop_column("signals", "signal_sub_classification")

    # Step 3: Drop the enum type
    signal_sub_classification_enum = postgresql.ENUM(
        *SIGNAL_SUB_CLASSIFICATION_VALUES,
        name="signalsubclassification",
    )
    signal_sub_classification_enum.drop(op.get_bind(), checkfirst=True)
