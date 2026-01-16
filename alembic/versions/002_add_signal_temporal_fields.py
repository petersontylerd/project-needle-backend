"""Add temporal classification fields to signals table.

Adds fields for signal sophistication feature:
- signal_classification: Temporal classification enum
- temporal_node_id: Reference to temporal node
- trend_slope_percentile: Trend slope percentile (0-100)
- monthly_z_scores: JSON array of 12 monthly z-scores

Revision ID: 002
Revises: 001
Create Date: 2025-12-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic
revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Signal classification enum - matches SignalClassification in models.py
signal_classification = postgresql.ENUM(
    "CONSISTENTLY_POOR",
    "DETERIORATING",
    "IMPROVING",
    "OUTLIER_DRIVEN",
    "NORMAL",
    name="signalclassification",
    create_type=False,
)


def upgrade() -> None:
    """Add temporal classification fields to signals table."""
    # Create the enum type first
    signal_classification.create(op.get_bind(), checkfirst=True)

    # Add new columns
    op.add_column(
        "signals",
        sa.Column(
            "signal_classification",
            signal_classification,
            nullable=True,
        ),
    )
    op.add_column(
        "signals",
        sa.Column(
            "temporal_node_id",
            sa.String(255),
            nullable=True,
        ),
    )
    op.add_column(
        "signals",
        sa.Column(
            "trend_slope_percentile",
            sa.Numeric(5, 2),
            nullable=True,
        ),
    )
    op.add_column(
        "signals",
        sa.Column(
            "monthly_z_scores",
            postgresql.JSONB,
            nullable=True,
        ),
    )

    # Add index for signal_classification
    op.create_index(
        "ix_signals_classification",
        "signals",
        ["signal_classification"],
    )


def downgrade() -> None:
    """Remove temporal classification fields from signals table."""
    # Drop index first
    op.drop_index("ix_signals_classification", table_name="signals")

    # Drop columns
    op.drop_column("signals", "monthly_z_scores")
    op.drop_column("signals", "trend_slope_percentile")
    op.drop_column("signals", "temporal_node_id")
    op.drop_column("signals", "signal_classification")

    # Drop enum type
    signal_classification.drop(op.get_bind(), checkfirst=True)
