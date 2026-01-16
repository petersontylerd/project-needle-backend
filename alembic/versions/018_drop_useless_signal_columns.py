"""Drop useless signal columns.

Removes columns that are always NULL or developer-only:
- node_result_path: Legacy, always NULL
- contribution_path: Legacy, always NULL
- coefficient_of_variation: Technical input to consistency, not user-facing
- priority_score_breakdown: Developer debugging, not user-facing

Revision ID: 018_drop_useless_signal_columns
Revises: 017_remove_contribution_fields
Create Date: 2025-12-27

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic
revision: str = "018_drop_useless_signal_columns"
down_revision: str | None = "017_remove_contribution_fields"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop useless columns from signals table."""
    op.drop_column("signals", "node_result_path")
    op.drop_column("signals", "contribution_path")
    op.drop_column("signals", "coefficient_of_variation")
    op.drop_column("signals", "priority_score_breakdown")


def downgrade() -> None:
    """Re-add dropped columns."""
    op.add_column(
        "signals",
        sa.Column("node_result_path", sa.String(500), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("contribution_path", sa.String(500), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("coefficient_of_variation", sa.Numeric(6, 4), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column(
            "priority_score_breakdown",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
