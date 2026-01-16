"""Add metadata JSONB column to signals table.

Revision ID: 34008bf60430
Revises: 024_drop_old_classification_columns
Create Date: 2026-01-10

Adds a flexible metadata column for extensible signal attributes:
- metadata: JSONB column for key-value pairs from entity payload
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "34008bf60430"
down_revision: str | Sequence[str] | None = "024_drop_old_classification_columns"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "signals",
        sa.Column("metadata", JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("signals", "metadata")
