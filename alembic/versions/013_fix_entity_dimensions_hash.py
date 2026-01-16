"""Fix entity_dimensions_hash to allow external values.

The previous migration created entity_dimensions_hash as a GENERATED ALWAYS column,
but the dbt pipeline provides pre-computed hash values. This migration removes
the GENERATED ALWAYS expression to allow inserting values from dbt.

Revision ID: 013_fix_entity_dimensions_hash
Revises: 012_fix_signal_sub_classification_storage
Create Date: 2024-12-20
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "013_fix_entity_dimensions_hash"
down_revision: str | None = "012_fix_signal_sub_classification_storage"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Remove GENERATED ALWAYS from entity_dimensions_hash column.

    PostgreSQL 13+ supports ALTER COLUMN ... DROP EXPRESSION to remove
    the generated column expression while keeping the column and data.
    """
    op.execute(
        """
        ALTER TABLE signals
        ALTER COLUMN entity_dimensions_hash DROP EXPRESSION IF EXISTS;
        """
    )


def downgrade() -> None:
    """Restore GENERATED ALWAYS expression for entity_dimensions_hash.

    Note: This will clear any manually-set values and replace them with
    computed hashes based on entity_dimensions.
    """
    # First, drop the column
    op.drop_column("signals", "entity_dimensions_hash")

    # Then re-add it as a generated column
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
