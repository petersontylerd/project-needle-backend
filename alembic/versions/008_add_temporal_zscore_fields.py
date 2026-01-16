"""Add temporal z-score fields to signals table.

This migration adds columns for temporal node z-score statistics:
- latest_simple_zscore: Most recent z-score from trending_simple_zscore method
- mean_simple_zscore: Average z-score over time from trending_simple_zscore method
- latest_robust_zscore: Most recent z-score from trending_robust_zscore method
- mean_robust_zscore: Average z-score over time from trending_robust_zscore method

These fields enable temporal trend analysis in the dashboard.

Revision ID: 008
Revises: 007
Create Date: 2025-12-16
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "008"
down_revision: str | None = "007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add temporal z-score fields."""
    op.add_column(
        "signals",
        sa.Column("latest_simple_zscore", sa.Numeric(8, 4), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("mean_simple_zscore", sa.Numeric(8, 4), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("latest_robust_zscore", sa.Numeric(8, 4), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("mean_robust_zscore", sa.Numeric(8, 4), nullable=True),
    )


def downgrade() -> None:
    """Remove temporal z-score fields."""
    op.drop_column("signals", "mean_robust_zscore")
    op.drop_column("signals", "latest_robust_zscore")
    op.drop_column("signals", "mean_simple_zscore")
    op.drop_column("signals", "latest_simple_zscore")
