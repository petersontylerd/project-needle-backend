"""Add simplified classification fields to signals table.

Revision ID: 5f3c10d06091
Revises: 023_add_impact_metrics
Create Date: 2026-01-09

Adds 7 new columns for the 9 Signal Type classification system:
- simplified_signal_type: One of 9 signal types (VARCHAR)
- simplified_severity: Severity score 0-100 (INTEGER)
- simplified_severity_range: [min, max] range for type (JSONB)
- simplified_inputs: 5 input values (JSONB)
- simplified_indicators: Categorical interpretations (JSONB)
- simplified_reasoning: Classification explanation (TEXT)
- simplified_severity_calculation: Severity breakdown (JSONB)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5f3c10d06091"
down_revision: str | Sequence[str] | None = "023_add_impact_metrics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Add simplified classification columns
    op.add_column(
        "signals",
        sa.Column("simplified_signal_type", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("simplified_severity", sa.Integer(), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("simplified_severity_range", JSONB(), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("simplified_inputs", JSONB(), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("simplified_indicators", JSONB(), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("simplified_reasoning", sa.Text(), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("simplified_severity_calculation", JSONB(), nullable=True),
    )

    # Add indexes for common filter/sort operations
    op.create_index(
        "ix_signals_simplified_signal_type",
        "signals",
        ["simplified_signal_type"],
    )
    op.create_index(
        "ix_signals_simplified_severity",
        "signals",
        ["simplified_severity"],
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index("ix_signals_simplified_severity", table_name="signals")
    op.drop_index("ix_signals_simplified_signal_type", table_name="signals")

    # Drop columns
    op.drop_column("signals", "simplified_severity_calculation")
    op.drop_column("signals", "simplified_reasoning")
    op.drop_column("signals", "simplified_indicators")
    op.drop_column("signals", "simplified_inputs")
    op.drop_column("signals", "simplified_severity_range")
    op.drop_column("signals", "simplified_severity")
    op.drop_column("signals", "simplified_signal_type")
