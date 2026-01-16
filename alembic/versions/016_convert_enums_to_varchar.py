"""Convert enum columns to VARCHAR for SQLAlchemy native_enum=False compatibility.

The model uses native_enum=False which tells SQLAlchemy to serialize enums as
VARCHAR, but the database has native PostgreSQL enum columns. This migration
converts the columns to VARCHAR to match the model.

Revision ID: 016_convert_enums_to_varchar
Revises: 015_add_classification_tiers
Create Date: 2025-12-21

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "016_convert_enums_to_varchar"
down_revision: str | None = "015_add_classification_tiers"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Convert enum columns to VARCHAR for SQLAlchemy compatibility."""
    # Convert signal_classification to VARCHAR
    op.alter_column(
        "signals",
        "signal_classification",
        type_=sa.VARCHAR(50),
        existing_type=sa.Enum(name="signalclassification"),
        existing_nullable=True,
        postgresql_using="signal_classification::text",
    )

    # Convert signal_sub_classification to VARCHAR
    op.alter_column(
        "signals",
        "signal_sub_classification",
        type_=sa.VARCHAR(50),
        existing_type=sa.Enum(name="signalsubclassification"),
        existing_nullable=True,
        postgresql_using="signal_sub_classification::text",
    )

    # Convert magnitude_tier to VARCHAR
    op.alter_column(
        "signals",
        "magnitude_tier",
        type_=sa.VARCHAR(50),
        existing_type=sa.Enum(name="magnitudetier"),
        existing_nullable=True,
        postgresql_using="magnitude_tier::text",
    )

    # Convert trajectory_tier to VARCHAR
    op.alter_column(
        "signals",
        "trajectory_tier",
        type_=sa.VARCHAR(50),
        existing_type=sa.Enum(name="trajectorytier"),
        existing_nullable=True,
        postgresql_using="trajectory_tier::text",
    )

    # Convert consistency_tier to VARCHAR
    op.alter_column(
        "signals",
        "consistency_tier",
        type_=sa.VARCHAR(50),
        existing_type=sa.Enum(name="consistencytier"),
        existing_nullable=True,
        postgresql_using="consistency_tier::text",
    )

    # Convert contribution_direction to VARCHAR
    op.alter_column(
        "signals",
        "contribution_direction",
        type_=sa.VARCHAR(50),
        existing_type=sa.Enum(name="contributiondirection"),
        existing_nullable=True,
        postgresql_using="contribution_direction::text",
    )

    # Convert direction to VARCHAR
    op.alter_column(
        "signals",
        "direction",
        type_=sa.VARCHAR(50),
        existing_type=sa.Enum(name="signaldirection"),
        existing_nullable=True,
        postgresql_using="direction::text",
    )


def downgrade() -> None:
    """Convert VARCHAR columns back to native PostgreSQL enums.

    Note: This will fail if any values don't match the enum definitions.
    """
    # Convert direction back to enum
    op.alter_column(
        "signals",
        "direction",
        type_=sa.Enum(
            "FAVORABLE",
            "UNFAVORABLE",
            "NEUTRAL",
            name="signaldirection",
            create_type=False,
        ),
        existing_type=sa.VARCHAR(50),
        existing_nullable=True,
        postgresql_using="direction::signaldirection",
    )

    # Convert contribution_direction back to enum
    op.alter_column(
        "signals",
        "contribution_direction",
        type_=sa.Enum(
            "POSITIVE",
            "NEGATIVE",
            "NEUTRAL",
            name="contributiondirection",
            create_type=False,
        ),
        existing_type=sa.VARCHAR(50),
        existing_nullable=True,
        postgresql_using="contribution_direction::contributiondirection",
    )

    # Convert consistency_tier back to enum
    op.alter_column(
        "signals",
        "consistency_tier",
        type_=sa.Enum(
            "persistent",
            "variable",
            "transient",
            name="consistencytier",
            create_type=False,
        ),
        existing_type=sa.VARCHAR(50),
        existing_nullable=True,
        postgresql_using="consistency_tier::consistencytier",
    )

    # Convert trajectory_tier back to enum
    op.alter_column(
        "signals",
        "trajectory_tier",
        type_=sa.Enum(
            "rapidly_deteriorating",
            "deteriorating",
            "stable",
            "improving",
            "rapidly_improving",
            name="trajectorytier",
            create_type=False,
        ),
        existing_type=sa.VARCHAR(50),
        existing_nullable=True,
        postgresql_using="trajectory_tier::trajectorytier",
    )

    # Convert magnitude_tier back to enum
    op.alter_column(
        "signals",
        "magnitude_tier",
        type_=sa.Enum(
            "critical",
            "severe",
            "elevated",
            "marginal",
            "expected",
            "favorable",
            "excellent",
            name="magnitudetier",
            create_type=False,
        ),
        existing_type=sa.VARCHAR(50),
        existing_nullable=True,
        postgresql_using="magnitude_tier::magnitudetier",
    )

    # Convert signal_sub_classification back to enum
    op.alter_column(
        "signals",
        "signal_sub_classification",
        type_=sa.Enum(
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
            name="signalsubclassification",
            create_type=False,
        ),
        existing_type=sa.VARCHAR(50),
        existing_nullable=True,
        postgresql_using="signal_sub_classification::signalsubclassification",
    )

    # Convert signal_classification back to enum
    op.alter_column(
        "signals",
        "signal_classification",
        type_=sa.Enum(
            "critical",
            "struggling",
            "at_risk",
            "achieving",
            "excelling",
            "uncertain",
            name="signalclassification",
            create_type=False,
        ),
        existing_type=sa.VARCHAR(50),
        existing_nullable=True,
        postgresql_using="signal_classification::signalclassification",
    )
