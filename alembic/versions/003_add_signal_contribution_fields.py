"""Add contribution weighting fields to signals table.

Adds fields for contribution-weighted signal prioritization:
- contribution_weight: Impact magnitude (|excess_over_parent| * weight_share)
- contribution_direction: Direction enum (positive, negative, neutral)

Revision ID: 003
Revises: 002
Create Date: 2025-12-14

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic
revision: str = "003"
down_revision: str | None = "002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Contribution direction enum - matches ContributionDirection in models.py
contribution_direction = postgresql.ENUM(
    "positive",
    "negative",
    "neutral",
    name="contributiondirection",
    create_type=False,
)


def upgrade() -> None:
    """Add contribution weighting fields to signals table."""
    # Create the enum type first
    contribution_direction.create(op.get_bind(), checkfirst=True)

    # Add new columns
    op.add_column(
        "signals",
        sa.Column(
            "contribution_weight",
            sa.Numeric(8, 6),
            nullable=True,
        ),
    )
    op.add_column(
        "signals",
        sa.Column(
            "contribution_direction",
            contribution_direction,
            nullable=True,
        ),
    )

    # Add index for contribution_weight to support sorting by impact
    op.create_index(
        "ix_signals_contribution_weight",
        "signals",
        ["contribution_weight"],
    )


def downgrade() -> None:
    """Remove contribution weighting fields from signals table."""
    # Drop index first
    op.drop_index("ix_signals_contribution_weight", table_name="signals")

    # Drop columns
    op.drop_column("signals", "contribution_direction")
    op.drop_column("signals", "contribution_weight")

    # Drop enum type
    contribution_direction.drop(op.get_bind(), checkfirst=True)
