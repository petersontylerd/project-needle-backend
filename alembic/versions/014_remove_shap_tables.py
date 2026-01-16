"""Remove SHAP tables and views.

Revision ID: 014_remove_shap_tables
Revises: 013_fix_entity_dimensions_hash
Create Date: 2025-12-20

This migration removes all SHAP-related database objects as part of
the removal of SHAP/driver functionality from Quality Compass.

Tables/views being dropped:
- public_staging.stg_global_shap_features (staging view)
- public_staging.stg_encounter_shap (staging view)
- public_staging.stg_facility_shap_aggregates (staging view)
- public_marts.fct_shap_features (mart table)
- public.raw_global_shap (raw source table)
- public.raw_encounter_shap (raw source table)
- public.raw_facility_shap (raw source table)
"""

from collections.abc import Sequence

from alembic import op

revision: str = "014_remove_shap_tables"
down_revision: str = "013_fix_entity_dimensions_hash"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop SHAP-related tables and views."""
    # Drop staging views (they depend on raw tables, so drop first)
    op.execute("DROP VIEW IF EXISTS public_staging.stg_global_shap_features CASCADE")
    op.execute("DROP VIEW IF EXISTS public_staging.stg_encounter_shap CASCADE")
    op.execute("DROP VIEW IF EXISTS public_staging.stg_facility_shap_aggregates CASCADE")

    # Drop mart table
    op.execute("DROP TABLE IF EXISTS public_marts.fct_shap_features CASCADE")

    # Drop raw source tables
    op.execute("DROP TABLE IF EXISTS public.raw_global_shap CASCADE")
    op.execute("DROP TABLE IF EXISTS public.raw_encounter_shap CASCADE")
    op.execute("DROP TABLE IF EXISTS public.raw_facility_shap CASCADE")


def downgrade() -> None:
    """Placeholder for rollback - requires dbt run to fully restore.

    This migration creates minimal raw table schemas for rollback capability.
    Full restoration requires:
    1. Re-running this downgrade migration
    2. Re-running the dbt pipeline to recreate staging views and mart tables
    3. Re-running the ETL pipeline to populate data from source files
    """
    # Recreate raw_global_shap table (minimal schema)
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.raw_global_shap (
            run_id VARCHAR(50) NOT NULL,
            feature VARCHAR(255) NOT NULL,
            mean_abs_shap NUMERIC,
            mean_shap NUMERIC,
            count INTEGER,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)

    # Recreate raw_encounter_shap table (minimal schema)
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.raw_encounter_shap (
            run_id VARCHAR(50) NOT NULL,
            record_id VARCHAR(100) NOT NULL,
            feature VARCHAR(255) NOT NULL,
            shap_value NUMERIC NOT NULL,
            fold INTEGER,
            shap_baseline NUMERIC,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)

    # Recreate raw_facility_shap table (minimal schema)
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.raw_facility_shap (
            run_id VARCHAR(50) NOT NULL,
            facility_id VARCHAR(50) NOT NULL,
            feature VARCHAR(255) NOT NULL,
            mean_abs_shap NUMERIC NOT NULL,
            mean_shap NUMERIC,
            std_shap NUMERIC,
            min_shap NUMERIC,
            max_shap NUMERIC,
            encounter_count INTEGER,
            model_groupby VARCHAR(100),
            model_group VARCHAR(100),
            estimator VARCHAR(50),
            file_path TEXT,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)
