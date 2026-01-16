"""Add statistical_method field to signals table.

This migration adds a statistical_method column to track which statistical
method (simple_zscore, robust_zscore, etc.) produced each signal. This enables:
1. Separate signals for each statistical method on the same entity/metric
2. Filtering and analysis by statistical method
3. Proper uniqueness when multiple methods generate signals for the same data

Changes:
1. Adds statistical_method column (VARCHAR(50), nullable)
2. Creates index on statistical_method
3. Updates unique constraint to include statistical_method

Revision ID: 007
Revises: 006
Create Date: 2025-12-16
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "007"
down_revision: str | None = "006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add statistical_method field and update unique constraint."""
    # Add the new column
    op.add_column(
        "signals",
        sa.Column("statistical_method", sa.String(50), nullable=True),
    )

    # Create index for efficient filtering by statistical method
    op.create_index(
        "ix_signals_statistical_method",
        "signals",
        ["statistical_method"],
    )

    # Drop the old unique constraint
    op.drop_constraint(
        "uq_signals_canonical_metric_facility_detected",
        "signals",
        type_="unique",
    )

    # Create new unique constraint including statistical_method
    op.create_unique_constraint(
        "uq_signals_canonical_metric_facility_method_detected",
        "signals",
        ["canonical_node_id", "metric_id", "facility_id", "statistical_method", "detected_at"],
    )


def downgrade() -> None:
    """Remove statistical_method field and revert unique constraint."""
    # Drop the new unique constraint
    op.drop_constraint(
        "uq_signals_canonical_metric_facility_method_detected",
        "signals",
        type_="unique",
    )

    # Recreate the old unique constraint
    op.create_unique_constraint(
        "uq_signals_canonical_metric_facility_detected",
        "signals",
        ["canonical_node_id", "metric_id", "facility_id", "detected_at"],
    )

    # Drop the index
    op.drop_index("ix_signals_statistical_method", table_name="signals")

    # Remove the column
    op.drop_column("signals", "statistical_method")
