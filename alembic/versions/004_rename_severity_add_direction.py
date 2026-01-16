"""Rename severity to significance and add direction field.

This migration:
1. Renames SignalSeverity enum to SignalSignificance
2. Renames enum values: CRITICAL→EXTREME, WATCH→MINOR
3. Renames the severity column to significance
4. Adds SignalDirection enum (FAVORABLE, UNFAVORABLE, NEUTRAL)
5. Adds direction column (nullable)
6. Updates indexes

Revision ID: 004
Revises: 003
Create Date: 2025-12-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic
revision: str = "004"
down_revision: str | None = "003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Rename severity to significance and add direction field."""
    conn = op.get_bind()

    # Step 1: Create new signalsignificance ENUM type with new values
    conn.execute(
        sa.text(
            """
            CREATE TYPE signalsignificance AS ENUM (
                'EXTREME',
                'HIGH',
                'MODERATE',
                'MINOR'
            )
            """
        )
    )

    # Step 2: Create signaldirection ENUM type
    conn.execute(
        sa.text(
            """
            CREATE TYPE signaldirection AS ENUM (
                'FAVORABLE',
                'UNFAVORABLE',
                'NEUTRAL'
            )
            """
        )
    )

    # Step 3: Add temporary significance column
    op.add_column(
        "signals",
        sa.Column(
            "significance_temp",
            postgresql.ENUM(
                "EXTREME",
                "HIGH",
                "MODERATE",
                "MINOR",
                name="signalsignificance",
                create_type=False,
            ),
            nullable=True,
        ),
    )

    # Step 4: Migrate data from severity to significance with value mapping
    # CRITICAL → EXTREME, HIGH → HIGH, MODERATE → MODERATE, WATCH → MINOR
    conn.execute(
        sa.text(
            """
            UPDATE signals SET significance_temp =
                CASE severity::text
                    WHEN 'CRITICAL' THEN 'EXTREME'::signalsignificance
                    WHEN 'HIGH' THEN 'HIGH'::signalsignificance
                    WHEN 'MODERATE' THEN 'MODERATE'::signalsignificance
                    WHEN 'WATCH' THEN 'MINOR'::signalsignificance
                END
            """
        )
    )

    # Step 5: Drop old indexes that reference severity
    op.drop_index("ix_signals_severity", table_name="signals")
    op.drop_index("ix_signals_composite", table_name="signals")

    # Step 6: Drop old severity column and rename significance_temp to significance
    op.drop_column("signals", "severity")
    op.alter_column("signals", "significance_temp", new_column_name="significance")

    # Step 7: Make significance NOT NULL (all data should be migrated)
    op.alter_column("signals", "significance", nullable=False)

    # Step 8: Add direction column (nullable)
    op.add_column(
        "signals",
        sa.Column(
            "direction",
            postgresql.ENUM(
                "FAVORABLE",
                "UNFAVORABLE",
                "NEUTRAL",
                name="signaldirection",
                create_type=False,
            ),
            nullable=True,
        ),
    )

    # Step 9: Create new indexes
    op.create_index("ix_signals_significance", "signals", ["significance"])
    op.create_index("ix_signals_direction", "signals", ["direction"])
    op.create_index(
        "ix_signals_composite",
        "signals",
        ["significance", "domain", "detected_at"],
    )
    op.create_index(
        "ix_signals_significance_direction",
        "signals",
        ["significance", "direction"],
    )

    # Step 10: Drop old signalseverity ENUM type
    conn.execute(sa.text("DROP TYPE signalseverity"))


def downgrade() -> None:
    """Revert significance back to severity and remove direction field."""
    conn = op.get_bind()

    # Step 1: Recreate old signalseverity ENUM type
    conn.execute(
        sa.text(
            """
            CREATE TYPE signalseverity AS ENUM (
                'CRITICAL',
                'HIGH',
                'MODERATE',
                'WATCH'
            )
            """
        )
    )

    # Step 2: Add temporary severity column
    op.add_column(
        "signals",
        sa.Column(
            "severity_temp",
            postgresql.ENUM(
                "CRITICAL",
                "HIGH",
                "MODERATE",
                "WATCH",
                name="signalseverity",
                create_type=False,
            ),
            nullable=True,
        ),
    )

    # Step 3: Migrate data back: EXTREME → CRITICAL, MINOR → WATCH
    conn.execute(
        sa.text(
            """
            UPDATE signals SET severity_temp =
                CASE significance::text
                    WHEN 'EXTREME' THEN 'CRITICAL'::signalseverity
                    WHEN 'HIGH' THEN 'HIGH'::signalseverity
                    WHEN 'MODERATE' THEN 'MODERATE'::signalseverity
                    WHEN 'MINOR' THEN 'WATCH'::signalseverity
                END
            """
        )
    )

    # Step 4: Drop new indexes
    op.drop_index("ix_signals_significance_direction", table_name="signals")
    op.drop_index("ix_signals_composite", table_name="signals")
    op.drop_index("ix_signals_direction", table_name="signals")
    op.drop_index("ix_signals_significance", table_name="signals")

    # Step 5: Drop direction column
    op.drop_column("signals", "direction")

    # Step 6: Drop significance column and rename severity_temp to severity
    op.drop_column("signals", "significance")
    op.alter_column("signals", "severity_temp", new_column_name="severity")

    # Step 7: Make severity NOT NULL
    op.alter_column("signals", "severity", nullable=False)

    # Step 8: Recreate old indexes
    op.create_index("ix_signals_severity", "signals", ["severity"])
    op.create_index(
        "ix_signals_composite",
        "signals",
        ["severity", "domain", "detected_at"],
    )

    # Step 9: Drop new ENUM types
    conn.execute(sa.text("DROP TYPE signaldirection"))
    conn.execute(sa.text("DROP TYPE signalsignificance"))
