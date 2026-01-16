"""Drop old 3D Matrix classification columns from signals table.

Revision ID: 024_drop_old_classification_columns
Revises: 5f3c10d06091
Create Date: 2026-01-09

Removes the old 18-subclass classification system columns:
- signal_classification (parent class)
- signal_sub_classification (18 sub-classes)
- classification_priority_score
- classification_confidence
- classification_factors

The simplified 9 Signal Type system is now the ONLY classification system.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "024_drop_old_classification_columns"
down_revision: str | Sequence[str] | None = "5f3c10d06091"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Drop indexes first
    op.drop_index("ix_signals_classification", table_name="signals")
    op.drop_index("ix_signals_sub_classification", table_name="signals")
    op.drop_index("ix_signals_priority_score", table_name="signals")

    # Drop columns
    op.drop_column("signals", "signal_classification")
    op.drop_column("signals", "signal_sub_classification")
    op.drop_column("signals", "classification_priority_score")
    op.drop_column("signals", "classification_confidence")
    op.drop_column("signals", "classification_factors")


def downgrade() -> None:
    # Recreate columns
    op.add_column(
        "signals",
        sa.Column("signal_classification", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("signal_sub_classification", sa.String(50), nullable=True),
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
        sa.Column("classification_factors", JSONB(), nullable=True),
    )

    # Recreate indexes
    op.create_index(
        "ix_signals_classification",
        "signals",
        ["signal_classification"],
    )
    op.create_index(
        "ix_signals_sub_classification",
        "signals",
        ["signal_sub_classification"],
    )
    op.create_index(
        "ix_signals_priority_score",
        "signals",
        ["classification_priority_score"],
    )
