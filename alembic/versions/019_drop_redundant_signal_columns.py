"""Drop redundant signal columns (Phase 2).

Revision ID: 019
Revises: 018
Create Date: 2025-12-27

Removes 20 technical columns that are now served via on-demand API:
- Z-scores: simple_zscore, robust_zscore, latest_*, mean_*
- Anomaly labels: *_anomaly
- Trend: slope, slope_percentile, monthly_z_scores, acceleration, momentum
- Tiers: magnitude_tier, trajectory_tier, consistency_tier
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision = "019_drop_redundant_signal_columns"
down_revision = "018_drop_useless_signal_columns"
branch_labels = None
depends_on = None


COLUMNS_TO_DROP = [
    "simple_zscore",
    "robust_zscore",
    "latest_simple_zscore",
    "mean_simple_zscore",
    "latest_robust_zscore",
    "mean_robust_zscore",
    "simple_zscore_anomaly",
    "robust_zscore_anomaly",
    "latest_simple_zscore_anomaly",
    "mean_simple_zscore_anomaly",
    "latest_robust_zscore_anomaly",
    "mean_robust_zscore_anomaly",
    "slope",
    "slope_percentile",
    "monthly_z_scores",
    "acceleration",
    "momentum",
    "magnitude_tier",
    "trajectory_tier",
    "consistency_tier",
]


def upgrade() -> None:
    """Drop redundant columns from signals table."""
    for col in COLUMNS_TO_DROP:
        op.drop_column("signals", col)


def downgrade() -> None:
    """Re-add dropped columns."""
    # Z-scores (Numeric)
    op.add_column("signals", sa.Column("simple_zscore", sa.Numeric(10, 4), nullable=True))
    op.add_column("signals", sa.Column("robust_zscore", sa.Numeric(10, 4), nullable=True))
    op.add_column("signals", sa.Column("latest_simple_zscore", sa.Numeric(10, 4), nullable=True))
    op.add_column("signals", sa.Column("mean_simple_zscore", sa.Numeric(10, 4), nullable=True))
    op.add_column("signals", sa.Column("latest_robust_zscore", sa.Numeric(10, 4), nullable=True))
    op.add_column("signals", sa.Column("mean_robust_zscore", sa.Numeric(10, 4), nullable=True))

    # Anomaly labels (String)
    op.add_column("signals", sa.Column("simple_zscore_anomaly", sa.String(50), nullable=True))
    op.add_column("signals", sa.Column("robust_zscore_anomaly", sa.String(50), nullable=True))
    op.add_column("signals", sa.Column("latest_simple_zscore_anomaly", sa.String(50), nullable=True))
    op.add_column("signals", sa.Column("mean_simple_zscore_anomaly", sa.String(50), nullable=True))
    op.add_column("signals", sa.Column("latest_robust_zscore_anomaly", sa.String(50), nullable=True))
    op.add_column("signals", sa.Column("mean_robust_zscore_anomaly", sa.String(50), nullable=True))

    # Trend (Numeric/String/JSONB)
    op.add_column("signals", sa.Column("slope", sa.Numeric(10, 6), nullable=True))
    op.add_column("signals", sa.Column("slope_percentile", sa.Numeric(10, 4), nullable=True))
    op.add_column("signals", sa.Column("monthly_z_scores", JSONB, nullable=True))
    op.add_column("signals", sa.Column("acceleration", sa.Numeric(10, 6), nullable=True))
    op.add_column("signals", sa.Column("momentum", sa.String(20), nullable=True))

    # Tiers (String)
    op.add_column("signals", sa.Column("magnitude_tier", sa.String(20), nullable=True))
    op.add_column("signals", sa.Column("trajectory_tier", sa.String(20), nullable=True))
    op.add_column("signals", sa.Column("consistency_tier", sa.String(20), nullable=True))
