"""Add Apache AGE extension verification.

Revision ID: 020_add_age_extension
Revises: 019_drop_redundant_signal_columns
Create Date: 2026-01-03

This migration verifies that the Apache AGE extension and healthcare_ontology
graph are properly installed. The actual installation is performed by the
Docker entrypoint script (01-enable-age.sh).
"""

from sqlalchemy import text

from alembic import op

# revision identifiers, used by Alembic.
revision = "020_add_age_extension"
down_revision = "019_drop_redundant_signal_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Verify AGE extension and graph are installed (soft check for dev environments)."""
    import os
    import warnings

    connection = op.get_bind()

    # Allow skipping AGE check in development environments
    skip_age_check = os.environ.get("SKIP_AGE_CHECK", "false").lower() == "true"

    # Verify AGE extension exists
    result = connection.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'age'")).fetchone()
    if not result:
        if skip_age_check:
            warnings.warn(
                "Apache AGE extension not found. Graph features will not be available. "
                "Set SKIP_AGE_CHECK=false or use the AGE-enabled Docker image for full functionality.",
                stacklevel=2,
            )
            return
        raise RuntimeError(
            "Apache AGE extension not found. Ensure the database was initialized with the AGE-enabled Docker image. Set SKIP_AGE_CHECK=true to skip this check."
        )

    # Verify healthcare_ontology graph exists
    result = connection.execute(text("SELECT 1 FROM ag_catalog.ag_graph WHERE name = 'healthcare_ontology'")).fetchone()
    if not result:
        if skip_age_check:
            warnings.warn(
                "healthcare_ontology graph not found. Graph features will not be available.",
                stacklevel=2,
            )
            return
        raise RuntimeError(
            "healthcare_ontology graph not found. Ensure the database was initialized with the AGE-enabled Docker image. Set SKIP_AGE_CHECK=true to skip this check."
        )


def downgrade() -> None:
    """No downgrade needed - this is a verification-only migration."""
    pass
