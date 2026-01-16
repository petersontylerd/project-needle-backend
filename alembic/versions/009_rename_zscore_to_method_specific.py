"""Rename z_score to simple_zscore and add robust_zscore column.

This migration fixes the many-to-one mapping issue where multiple statistical
methods were being collapsed into a single z_score field. Each statistical
method now has its own output field:
- simple_zscore: Z-score from simple_zscore statistical method
- robust_zscore: Z-score from robust_zscore statistical method

Revision ID: 009
Revises: 008
Create Date: 2025-12-16
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "009"
down_revision: str | None = "008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Rename z_score to simple_zscore and add robust_zscore column."""
    # Rename z_score to simple_zscore
    op.alter_column("signals", "z_score", new_column_name="simple_zscore")

    # Add robust_zscore column
    op.add_column(
        "signals",
        sa.Column("robust_zscore", sa.Numeric(6, 3), nullable=True),
    )


def downgrade() -> None:
    """Restore z_score column and remove robust_zscore."""
    # Remove robust_zscore column
    op.drop_column("signals", "robust_zscore")

    # Rename simple_zscore back to z_score
    op.alter_column("signals", "simple_zscore", new_column_name="z_score")
