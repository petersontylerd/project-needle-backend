"""add_metadata_per_period_to_signals

Revision ID: 5c3ce8c4550e
Revises: 13bf50a1dd61
Create Date: 2026-01-12 08:18:46.100090

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5c3ce8c4550e"
down_revision: str | Sequence[str] | None = "13bf50a1dd61"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "signals",
        sa.Column(
            "metadata_per_period",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Per-period breakdown of metadata values (encounters per period, etc.)",
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("signals", "metadata_per_period")
