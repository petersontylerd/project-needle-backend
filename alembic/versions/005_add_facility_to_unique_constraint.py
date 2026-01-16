"""Add facility_id to signals unique constraint.

This migration fixes a data integrity bug where signals from different
facilities with the same (canonical_node_id, metric_id, detected_at) would
overwrite each other during hydration.

The fix:
1. Drops the old unique constraint on (canonical_node_id, metric_id, detected_at)
2. Creates new unique constraint on (canonical_node_id, metric_id, facility_id, detected_at)

Revision ID: 005
Revises: 004
Create Date: 2025-12-16
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic
revision: str = "005"
down_revision: str | None = "004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add facility_id to signals unique constraint."""
    # Drop the old constraint that was missing facility_id
    op.drop_constraint(
        "uq_signals_canonical_metric_detected",
        "signals",
        type_="unique",
    )

    # Create new constraint that includes facility_id
    op.create_unique_constraint(
        "uq_signals_canonical_metric_facility_detected",
        "signals",
        ["canonical_node_id", "metric_id", "facility_id", "detected_at"],
    )


def downgrade() -> None:
    """Revert to old unique constraint without facility_id."""
    # Drop the new constraint
    op.drop_constraint(
        "uq_signals_canonical_metric_facility_detected",
        "signals",
        type_="unique",
    )

    # Recreate the old constraint
    op.create_unique_constraint(
        "uq_signals_canonical_metric_detected",
        "signals",
        ["canonical_node_id", "metric_id", "detected_at"],
    )
