"""Add 3D matrix classification tier columns.

This migration adds columns for the enhanced 3D matrix classification system
(Magnitude × Trajectory × Consistency) with fully explainable priority scores.

Changes:
- Add MagnitudeTier, TrajectoryTier, ConsistencyTier enum types
- Add magnitude_tier, trajectory_tier, consistency_tier columns
- Add coefficient_of_variation column for consistency calculation
- Add priority_score_breakdown JSONB column for explainability
- Update SignalClassification enum with new parent classes
- Update SignalSubClassification enum with expanded sub-classes
- Add indexes for efficient querying

Revision ID: 015_add_classification_tiers
Revises: 014_remove_shap_tables
Create Date: 2024-12-21

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "015_add_classification_tiers"
down_revision: str | None = "014_remove_shap_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Define enum values for new tier types
MAGNITUDE_TIER_VALUES = [
    "critical",
    "severe",
    "elevated",
    "marginal",
    "expected",
    "favorable",
    "excellent",
]

TRAJECTORY_TIER_VALUES = [
    "rapidly_deteriorating",
    "deteriorating",
    "stable",
    "improving",
    "rapidly_improving",
]

CONSISTENCY_TIER_VALUES = [
    "persistent",
    "variable",
    "transient",
]

# New parent classification values (replacing old ones)
NEW_SIGNAL_CLASSIFICATION_VALUES = [
    "critical",
    "struggling",
    "at_risk",
    "achieving",
    "excelling",
    "uncertain",
]

# Old parent classification values (for downgrade)
OLD_SIGNAL_CLASSIFICATION_VALUES = [
    "consistently_poor",
    "deteriorating",
    "recently_elevated",
    "improving",
    "stable",
]

# New sub-classification values (replacing old ones)
NEW_SIGNAL_SUB_CLASSIFICATION_VALUES = [
    "crisis_escalating",
    "crisis_acute",
    "systemic_failure",
    "concentrated_driver",
    "broad_underperformance",
    "accelerating_decline",
    "gradual_erosion",
    "approaching_threshold",
    "volatile_pattern",
    "early_warning",
    "stable_performer",
    "maintaining_position",
    "recovering",
    "top_performer",
    "sustained_excellence",
    "emerging_leader",
    "data_quality_suspect",
    "transient_anomaly",
    "insufficient_history",
]

# Old sub-classification values (for downgrade)
OLD_SIGNAL_SUB_CLASSIFICATION_VALUES = [
    "concentrated_driver",
    "systemic_poor",
    "rapid_decline",
    "gradual_decline",
    "sustained_recovery",
    "early_improvement",
    "data_quality_suspect",
    "transient_spike",
    "stable_performer",
    "watch_list",
]


def upgrade() -> None:
    """Add 3D matrix classification tier columns and update enums."""
    conn = op.get_bind()

    # Step 1: Create new enum types for tiers
    magnitude_tier_enum = postgresql.ENUM(
        *MAGNITUDE_TIER_VALUES,
        name="magnitudetier",
    )
    magnitude_tier_enum.create(conn, checkfirst=True)

    trajectory_tier_enum = postgresql.ENUM(
        *TRAJECTORY_TIER_VALUES,
        name="trajectorytier",
    )
    trajectory_tier_enum.create(conn, checkfirst=True)

    consistency_tier_enum = postgresql.ENUM(
        *CONSISTENCY_TIER_VALUES,
        name="consistencytier",
    )
    consistency_tier_enum.create(conn, checkfirst=True)

    # Step 2: Add new tier columns
    op.add_column(
        "signals",
        sa.Column(
            "magnitude_tier",
            sa.Enum(*MAGNITUDE_TIER_VALUES, name="magnitudetier"),
            nullable=True,
        ),
    )

    op.add_column(
        "signals",
        sa.Column(
            "trajectory_tier",
            sa.Enum(*TRAJECTORY_TIER_VALUES, name="trajectorytier"),
            nullable=True,
        ),
    )

    op.add_column(
        "signals",
        sa.Column(
            "consistency_tier",
            sa.Enum(*CONSISTENCY_TIER_VALUES, name="consistencytier"),
            nullable=True,
        ),
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

    # Step 3: Create indexes for tier columns
    op.create_index(
        "ix_signals_magnitude_tier",
        "signals",
        ["magnitude_tier"],
        unique=False,
    )

    op.create_index(
        "ix_signals_trajectory_tier",
        "signals",
        ["trajectory_tier"],
        unique=False,
    )

    op.create_index(
        "ix_signals_consistency_tier",
        "signals",
        ["consistency_tier"],
        unique=False,
    )

    # Step 4: Update SignalClassification enum
    # First, clear existing data that uses old enum values
    op.execute("UPDATE signals SET signal_classification = NULL")

    # Drop and recreate the enum type with new values
    # First need to alter column to use text temporarily
    op.execute("ALTER TABLE signals ALTER COLUMN signal_classification TYPE VARCHAR(50) USING signal_classification::VARCHAR(50)")

    # Drop old enum type
    op.execute("DROP TYPE IF EXISTS signalclassification")

    # Create new enum type
    new_classification_enum = postgresql.ENUM(
        *NEW_SIGNAL_CLASSIFICATION_VALUES,
        name="signalclassification",
    )
    new_classification_enum.create(conn, checkfirst=True)

    # Alter column back to use enum
    op.execute("ALTER TABLE signals ALTER COLUMN signal_classification TYPE signalclassification USING signal_classification::signalclassification")

    # Step 5: Update SignalSubClassification enum
    # First, clear existing data
    op.execute("UPDATE signals SET signal_sub_classification = NULL")

    # Alter column to text temporarily
    op.execute("ALTER TABLE signals ALTER COLUMN signal_sub_classification TYPE VARCHAR(50) USING signal_sub_classification::VARCHAR(50)")

    # Drop old enum type
    op.execute("DROP TYPE IF EXISTS signalsubclassification")

    # Create new enum type
    new_sub_classification_enum = postgresql.ENUM(
        *NEW_SIGNAL_SUB_CLASSIFICATION_VALUES,
        name="signalsubclassification",
    )
    new_sub_classification_enum.create(conn, checkfirst=True)

    # Alter column back to use enum
    op.execute(
        "ALTER TABLE signals ALTER COLUMN signal_sub_classification TYPE signalsubclassification USING signal_sub_classification::signalsubclassification"
    )


def downgrade() -> None:
    """Remove 3D matrix classification tier columns and restore old enums."""
    conn = op.get_bind()

    # Step 1: Drop indexes
    op.drop_index("ix_signals_consistency_tier", table_name="signals")
    op.drop_index("ix_signals_trajectory_tier", table_name="signals")
    op.drop_index("ix_signals_magnitude_tier", table_name="signals")

    # Step 2: Drop new columns
    op.drop_column("signals", "priority_score_breakdown")
    op.drop_column("signals", "coefficient_of_variation")
    op.drop_column("signals", "consistency_tier")
    op.drop_column("signals", "trajectory_tier")
    op.drop_column("signals", "magnitude_tier")

    # Step 3: Drop new enum types
    magnitude_tier_enum = postgresql.ENUM(
        *MAGNITUDE_TIER_VALUES,
        name="magnitudetier",
    )
    magnitude_tier_enum.drop(conn, checkfirst=True)

    trajectory_tier_enum = postgresql.ENUM(
        *TRAJECTORY_TIER_VALUES,
        name="trajectorytier",
    )
    trajectory_tier_enum.drop(conn, checkfirst=True)

    consistency_tier_enum = postgresql.ENUM(
        *CONSISTENCY_TIER_VALUES,
        name="consistencytier",
    )
    consistency_tier_enum.drop(conn, checkfirst=True)

    # Step 4: Restore old SignalClassification enum
    op.execute("UPDATE signals SET signal_classification = NULL")
    op.execute("ALTER TABLE signals ALTER COLUMN signal_classification TYPE VARCHAR(50) USING signal_classification::VARCHAR(50)")
    op.execute("DROP TYPE IF EXISTS signalclassification")

    old_classification_enum = postgresql.ENUM(
        *OLD_SIGNAL_CLASSIFICATION_VALUES,
        name="signalclassification",
    )
    old_classification_enum.create(conn, checkfirst=True)

    op.execute("ALTER TABLE signals ALTER COLUMN signal_classification TYPE signalclassification USING signal_classification::signalclassification")

    # Step 5: Restore old SignalSubClassification enum
    op.execute("UPDATE signals SET signal_sub_classification = NULL")
    op.execute("ALTER TABLE signals ALTER COLUMN signal_sub_classification TYPE VARCHAR(50) USING signal_sub_classification::VARCHAR(50)")
    op.execute("DROP TYPE IF EXISTS signalsubclassification")

    old_sub_classification_enum = postgresql.ENUM(
        *OLD_SIGNAL_SUB_CLASSIFICATION_VALUES,
        name="signalsubclassification",
    )
    old_sub_classification_enum.create(conn, checkfirst=True)

    op.execute(
        "ALTER TABLE signals ALTER COLUMN signal_sub_classification TYPE signalsubclassification USING signal_sub_classification::signalsubclassification"
    )
