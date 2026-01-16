"""Drop significance, direction, and slope_anomaly columns.

Revision ID: 021_drop_significance_direction
Revises: 020_add_age_extension
Create Date: 2026-01-05

Removes deprecated signal columns that have been superseded by the 3D
classification system (signal_classification, signal_sub_classification,
classification_priority_score).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic
revision: str = "021_drop_significance_direction"
down_revision: str | None = "020_add_age_extension"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop significance, direction, slope_anomaly columns and related indexes."""
    conn = op.get_bind()

    # Drop indexes first using IF EXISTS for robustness (indexes may not exist in all environments)
    # Order: composite indexes first, then single-column indexes
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_signals_significance_direction"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_signals_composite"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_signals_significance"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_signals_direction"))

    # Drop columns
    op.drop_column("signals", "significance")
    op.drop_column("signals", "direction")
    op.drop_column("signals", "slope_anomaly")

    # Drop enum types using IF EXISTS for robustness
    # Note: signalsignificance is a native PostgreSQL enum; signaldirection was converted to
    # VARCHAR in migration 016 but the enum type may still exist in the database
    conn.execute(sa.text("DROP TYPE IF EXISTS signalsignificance"))
    conn.execute(sa.text("DROP TYPE IF EXISTS signaldirection"))


def downgrade() -> None:
    """Re-add significance, direction, slope_anomaly columns."""
    conn = op.get_bind()

    # Recreate only signalsignificance enum - the significance column uses this native enum.
    # Note: signaldirection enum is NOT recreated because the direction column uses String(50),
    # having been converted from enum to VARCHAR in migration 016.
    conn.execute(
        sa.text("""
        CREATE TYPE signalsignificance AS ENUM (
            'EXTREME', 'HIGH', 'MODERATE', 'MINOR'
        )
    """)
    )

    # Add columns back
    # Note: significance was originally NOT NULL, but downgrade leaves nullable
    # since we cannot populate historical data
    op.add_column(
        "signals",
        sa.Column(
            "significance",
            postgresql.ENUM("EXTREME", "HIGH", "MODERATE", "MINOR", name="signalsignificance", create_type=False),
            nullable=True,
        ),
    )
    op.add_column(
        "signals",
        sa.Column(
            "direction",
            sa.String(50),  # Was converted to VARCHAR in migration 016
            nullable=True,
        ),
    )
    op.add_column(
        "signals",
        sa.Column(
            "slope_anomaly",
            sa.String(50),
            nullable=True,
        ),
    )

    # Recreate indexes
    op.create_index("ix_signals_significance", "signals", ["significance"])
    op.create_index("ix_signals_direction", "signals", ["direction"])
    op.create_index("ix_signals_significance_direction", "signals", ["significance", "direction"])
    op.create_index("ix_signals_composite", "signals", ["significance", "domain", "detected_at"])
