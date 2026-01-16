"""add_peer_percentile_trends

Revision ID: 13bf50a1dd61
Revises: 34008bf60430
Create Date: 2026-01-10 15:01:15.924246

Adds peer_percentile_trends JSONB column to signals table for storing
peer distribution percentile trends (p10, p25, p50, p75, p90) at each time period.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "13bf50a1dd61"
down_revision: str | Sequence[str] | None = "34008bf60430"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "signals",
        sa.Column(
            "peer_percentile_trends",
            JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Peer distribution percentile trends (p10, p25, p50, p75, p90) at each time period",
        ),
    )


def downgrade() -> None:
    op.drop_column("signals", "peer_percentile_trends")
