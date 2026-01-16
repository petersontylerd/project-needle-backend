"""Consolidate signal schema for entity/metric pair grouping.

This migration restructures the signals table to support consolidated signals
where one signal record represents all statistical methods for a unique
entity/metric pair combination.

Changes:
- Add entity_dimensions, groupby_label, group_value columns
- Add metric_trend_timeline for temporal sparkline data
- Add anomaly label columns for each statistical method
- Add slope, acceleration, trend_direction, momentum columns
- Add peer_std column
- Rename benchmark_value to peer_mean
- Rename trend_slope_percentile to slope_percentile
- Remove statistical_method column
- Remove variance_percent column
- Change unique constraint from per-method to per-entity/metric

Revision ID: 010_consolidate_signal_schema
Revises: 009_rename_zscore_to_method_specific
Create Date: 2024-12-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "010_consolidate_signal_schema"
down_revision: str | None = "009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply schema changes for signal consolidation."""
    # Step 1: Add new columns for entity identification and grouping
    op.add_column(
        "signals",
        sa.Column("entity_dimensions", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("groupby_label", sa.String(255), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("group_value", sa.String(500), nullable=True),
    )

    # Step 2: Add metric trend timeline for sparkline visualization
    op.add_column(
        "signals",
        sa.Column("metric_trend_timeline", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    # Step 3: Add anomaly label columns for each statistical method
    op.add_column(
        "signals",
        sa.Column("simple_zscore_anomaly", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("robust_zscore_anomaly", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("latest_simple_zscore_anomaly", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("mean_simple_zscore_anomaly", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("latest_robust_zscore_anomaly", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("mean_robust_zscore_anomaly", sa.String(50), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("slope_anomaly", sa.String(50), nullable=True),
    )

    # Step 4: Add temporal statistics columns
    op.add_column(
        "signals",
        sa.Column("slope", sa.Numeric(10, 6), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("acceleration", sa.Numeric(10, 6), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("trend_direction", sa.String(20), nullable=True),
    )
    op.add_column(
        "signals",
        sa.Column("momentum", sa.String(20), nullable=True),
    )

    # Step 5: Add peer_std column
    op.add_column(
        "signals",
        sa.Column("peer_std", sa.Numeric(10, 4), nullable=True),
    )

    # Step 6: Rename benchmark_value to peer_mean
    op.alter_column("signals", "benchmark_value", new_column_name="peer_mean")

    # Step 7: Rename trend_slope_percentile to slope_percentile
    op.alter_column("signals", "trend_slope_percentile", new_column_name="slope_percentile")

    # Step 8: Drop old unique constraint
    op.drop_constraint(
        "uq_signals_canonical_metric_facility_method_detected",
        "signals",
        type_="unique",
    )

    # Step 9: Drop statistical_method index before dropping column
    op.drop_index("ix_signals_statistical_method", table_name="signals")

    # Step 10: Drop statistical_method column
    op.drop_column("signals", "statistical_method")

    # Step 11: Drop variance_percent column
    op.drop_column("signals", "variance_percent")

    # Step 12: Create new unique constraint on entity/metric pair
    # Note: Using a hash of entity_dimensions for the constraint since JSONB
    # cannot be directly used in a unique constraint. We use a generated column approach.
    # For simplicity, we'll use a functional approach with a computed MD5 hash.
    op.execute(
        """
        ALTER TABLE signals
        ADD COLUMN entity_dimensions_hash TEXT GENERATED ALWAYS AS (
            CASE
                WHEN entity_dimensions IS NULL THEN ''
                ELSE MD5(entity_dimensions::text)
            END
        ) STORED;
        """
    )

    op.create_unique_constraint(
        "uq_signals_entity_metric_detected",
        "signals",
        ["canonical_node_id", "metric_id", "facility_id", "entity_dimensions_hash", "detected_at"],
    )

    # Step 13: Add indexes for new columns
    op.create_index("ix_signals_groupby_label", "signals", ["groupby_label"])
    op.create_index("ix_signals_entity_dimensions", "signals", ["entity_dimensions"], postgresql_using="gin")


def downgrade() -> None:
    """Revert schema changes for signal consolidation."""
    # Step 1: Drop new indexes
    op.drop_index("ix_signals_entity_dimensions", table_name="signals")
    op.drop_index("ix_signals_groupby_label", table_name="signals")

    # Step 2: Drop new unique constraint
    op.drop_constraint("uq_signals_entity_metric_detected", "signals", type_="unique")

    # Step 3: Drop entity_dimensions_hash generated column
    op.drop_column("signals", "entity_dimensions_hash")

    # Step 4: Re-add variance_percent column
    op.add_column(
        "signals",
        sa.Column("variance_percent", sa.Numeric(6, 2), nullable=True),
    )

    # Step 5: Re-add statistical_method column
    op.add_column(
        "signals",
        sa.Column("statistical_method", sa.String(50), nullable=True),
    )

    # Step 6: Re-create statistical_method index
    op.create_index("ix_signals_statistical_method", "signals", ["statistical_method"])

    # Step 7: Re-create old unique constraint
    op.create_unique_constraint(
        "uq_signals_canonical_metric_facility_method_detected",
        "signals",
        ["canonical_node_id", "metric_id", "facility_id", "statistical_method", "detected_at"],
    )

    # Step 8: Rename slope_percentile back to trend_slope_percentile
    op.alter_column("signals", "slope_percentile", new_column_name="trend_slope_percentile")

    # Step 9: Rename peer_mean back to benchmark_value
    op.alter_column("signals", "peer_mean", new_column_name="benchmark_value")

    # Step 10: Drop new columns in reverse order
    op.drop_column("signals", "peer_std")
    op.drop_column("signals", "momentum")
    op.drop_column("signals", "trend_direction")
    op.drop_column("signals", "acceleration")
    op.drop_column("signals", "slope")
    op.drop_column("signals", "slope_anomaly")
    op.drop_column("signals", "mean_robust_zscore_anomaly")
    op.drop_column("signals", "latest_robust_zscore_anomaly")
    op.drop_column("signals", "mean_simple_zscore_anomaly")
    op.drop_column("signals", "latest_simple_zscore_anomaly")
    op.drop_column("signals", "robust_zscore_anomaly")
    op.drop_column("signals", "simple_zscore_anomaly")
    op.drop_column("signals", "metric_trend_timeline")
    op.drop_column("signals", "group_value")
    op.drop_column("signals", "groupby_label")
    op.drop_column("signals", "entity_dimensions")
