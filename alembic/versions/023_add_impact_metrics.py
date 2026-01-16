"""Add impact metrics columns to signals table.

Revision ID: 023_add_impact_metrics
Revises: 022_add_system_name
Create Date: 2026-01-08

Adds columns for business impact metrics per wireframe specification:
- annual_excess_cost: Computed excess cost in dollars
- excess_los_days: Total excess length of stay days
- capacity_impact_bed_days: Bed-days per day impact
- expected_metric_value: Expected/benchmark metric value
- why_matters_narrative: User-editable business impact narrative
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "023_add_impact_metrics"
down_revision: str | None = "022_add_system_name"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add impact metrics columns to signals table."""
    op.add_column(
        "signals",
        sa.Column("annual_excess_cost", sa.Numeric(12, 2), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("excess_los_days", sa.Numeric(10, 2), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("capacity_impact_bed_days", sa.Numeric(10, 4), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("expected_metric_value", sa.Numeric(10, 4), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("why_matters_narrative", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    """Remove impact metrics columns from signals table."""
    op.drop_column("signals", "why_matters_narrative")
    op.drop_column("signals", "expected_metric_value")
    op.drop_column("signals", "capacity_impact_bed_days")
    op.drop_column("signals", "excess_los_days")
    op.drop_column("signals", "annual_excess_cost")
