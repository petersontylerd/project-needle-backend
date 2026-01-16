"""Fix contributiondirection enum case to match SQLAlchemy default.

SQLAlchemy 2.0 uses enum member names (UPPERCASE) for serialization by default.
The contributiondirection PostgreSQL enum was created with lowercase values,
causing a mismatch. This migration updates it to uppercase.

Revision ID: 006
Revises: 005
Create Date: 2025-12-16
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Change contributiondirection enum values from lowercase to uppercase."""
    conn = op.get_bind()

    # PostgreSQL requires a multi-step process to change enum values:
    # 1. Add new values
    # 2. Update existing data
    # 3. Rename old values (remove them)

    # Since we can't easily remove enum values in PostgreSQL, we'll use a
    # workaround: create a new enum, migrate the column, drop the old enum.

    # Step 1: Create new enum type with uppercase values
    conn.execute(
        sa.text(
            """
            CREATE TYPE contributiondirection_new AS ENUM (
                'POSITIVE',
                'NEGATIVE',
                'NEUTRAL'
            )
            """
        )
    )

    # Step 2: Add temporary column with new enum type
    op.add_column(
        "signals",
        sa.Column(
            "contribution_direction_new",
            sa.Enum(
                "POSITIVE",
                "NEGATIVE",
                "NEUTRAL",
                name="contributiondirection_new",
                create_type=False,
            ),
            nullable=True,
        ),
    )

    # Step 3: Migrate data from old to new column
    conn.execute(
        sa.text(
            """
            UPDATE signals SET contribution_direction_new =
                CASE contribution_direction::text
                    WHEN 'positive' THEN 'POSITIVE'::contributiondirection_new
                    WHEN 'negative' THEN 'NEGATIVE'::contributiondirection_new
                    WHEN 'neutral' THEN 'NEUTRAL'::contributiondirection_new
                END
            WHERE contribution_direction IS NOT NULL
            """
        )
    )

    # Step 4: Drop old column
    op.drop_column("signals", "contribution_direction")

    # Step 5: Rename new column
    op.alter_column("signals", "contribution_direction_new", new_column_name="contribution_direction")

    # Step 6: Drop old enum type
    conn.execute(sa.text("DROP TYPE contributiondirection"))

    # Step 7: Rename new enum type to original name
    conn.execute(sa.text("ALTER TYPE contributiondirection_new RENAME TO contributiondirection"))


def downgrade() -> None:
    """Revert contributiondirection enum values back to lowercase."""
    conn = op.get_bind()

    # Step 1: Create old enum type with lowercase values
    conn.execute(
        sa.text(
            """
            CREATE TYPE contributiondirection_old AS ENUM (
                'positive',
                'negative',
                'neutral'
            )
            """
        )
    )

    # Step 2: Add temporary column with old enum type
    op.add_column(
        "signals",
        sa.Column(
            "contribution_direction_old",
            sa.Enum(
                "positive",
                "negative",
                "neutral",
                name="contributiondirection_old",
                create_type=False,
            ),
            nullable=True,
        ),
    )

    # Step 3: Migrate data from new to old column
    conn.execute(
        sa.text(
            """
            UPDATE signals SET contribution_direction_old =
                CASE contribution_direction::text
                    WHEN 'POSITIVE' THEN 'positive'::contributiondirection_old
                    WHEN 'NEGATIVE' THEN 'negative'::contributiondirection_old
                    WHEN 'NEUTRAL' THEN 'neutral'::contributiondirection_old
                END
            WHERE contribution_direction IS NOT NULL
            """
        )
    )

    # Step 4: Drop new column
    op.drop_column("signals", "contribution_direction")

    # Step 5: Rename old column
    op.alter_column("signals", "contribution_direction_old", new_column_name="contribution_direction")

    # Step 6: Drop new enum type
    conn.execute(sa.text("DROP TYPE contributiondirection"))

    # Step 7: Rename old enum type to original name
    conn.execute(sa.text("ALTER TYPE contributiondirection_old RENAME TO contributiondirection"))
