"""Add system_name column to signals table.

Revision ID: 022_add_system_name
Revises: 021_drop_significance_direction
Create Date: 2026-01-06

Adds system_name column to support health system-level grouping and filtering.
This new entity dimension enables multi-facility organization filtering in the
Quality Compass dashboard.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "022_add_system_name"
down_revision: str | None = "021_drop_significance_direction"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add system_name column and index to signals table."""
    op.add_column(
        "signals",
        sa.Column("system_name", sa.String(255), nullable=True),
    )
    op.create_index("ix_signals_system_name", "signals", ["system_name"])


def downgrade() -> None:
    """Remove system_name column and index from signals table."""
    op.drop_index("ix_signals_system_name", table_name="signals")
    op.drop_column("signals", "system_name")
