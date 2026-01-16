"""Remove vestigial contribution fields from signals table.

These fields were pre-fetched during hydration but never used by the frontend.
The ContributionService provides runtime queries for driver analysis which is
the only path used by the UI.

Revision ID: 017_remove_contribution_fields
Revises: 016_convert_enums_to_varchar
Create Date: 2025-12-26

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "017_remove_contribution_fields"
down_revision: str | None = "016_convert_enums_to_varchar"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Remove contribution_weight and contribution_direction columns."""
    # Drop the index first
    op.drop_index("ix_signals_contribution_weight", table_name="signals")

    # Drop the columns
    op.drop_column("signals", "contribution_weight")
    op.drop_column("signals", "contribution_direction")


def downgrade() -> None:
    """Restore contribution_weight and contribution_direction columns."""
    op.add_column(
        "signals",
        sa.Column("contribution_weight", sa.Numeric(8, 6), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("contribution_direction", sa.VARCHAR(length=50), nullable=True),
    )
    op.create_index(
        "ix_signals_contribution_weight",
        "signals",
        ["contribution_weight"],
    )
