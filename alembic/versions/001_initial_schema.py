"""Initial database schema.

Creates the core tables for Quality Compass:
- users: User accounts and authentication
- signals: Detected quality signals
- assignments: Signal assignment workflow
- activity_events: Activity feed events

Revision ID: 001
Revises:
Create Date: 2025-12-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic
revision: str = "001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Enum types - names must match SQLAlchemy model enum names (lowercase, no underscores)
# create_type=False prevents auto-creation when used in columns
user_role = postgresql.ENUM(
    "ADMIN",
    "CLINICAL_LEADERSHIP",
    "NURSING_LEADERSHIP",
    "ADMINISTRATION",
    "VIEWER",
    name="userrole",
    create_type=False,
)

signal_severity = postgresql.ENUM(
    "CRITICAL",
    "HIGH",
    "MODERATE",
    "WATCH",
    name="signalseverity",
    create_type=False,
)

signal_domain = postgresql.ENUM(
    "EFFICIENCY",
    "SAFETY",
    "EFFECTIVENESS",
    name="signaldomain",
    create_type=False,
)

assignment_status = postgresql.ENUM(
    "NEW",
    "ASSIGNED",
    "IN_PROGRESS",
    "RESOLVED",
    "CLOSED",
    name="assignmentstatus",
    create_type=False,
)

assignment_role_type = postgresql.ENUM(
    "CLINICAL_LEADERSHIP",
    "NURSING_LEADERSHIP",
    "ADMINISTRATION",
    name="assignmentroletype",
    create_type=False,
)

event_type = postgresql.ENUM(
    "NEW_SIGNAL",
    "REGRESSION",
    "IMPROVEMENT",
    "INSIGHT",
    "TECHNICAL_ERROR",
    "ASSIGNMENT",
    "STATUS_CHANGE",
    "COMMENT",
    "INTERVENTION_ACTIVATED",
    "INTERVENTION_OUTCOME",
    name="eventtype",
    create_type=False,
)


def upgrade() -> None:
    """Create initial database schema."""
    # Create enum types
    user_role.create(op.get_bind(), checkfirst=True)
    signal_severity.create(op.get_bind(), checkfirst=True)
    signal_domain.create(op.get_bind(), checkfirst=True)
    assignment_status.create(op.get_bind(), checkfirst=True)
    assignment_role_type.create(op.get_bind(), checkfirst=True)
    event_type.create(op.get_bind(), checkfirst=True)

    # Create users table
    op.create_table(
        "users",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("hashed_password", sa.String(length=255), nullable=False),
        sa.Column(
            "role",
            user_role,
            server_default="VIEWER",
            nullable=False,
        ),
        sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
        sa.UniqueConstraint("email", name=op.f("uq_users_email")),
    )
    op.create_index(op.f("ix_users_email"), "users", ["email"], unique=False)
    op.create_index("ix_users_role", "users", ["role"], unique=False)

    # Create signals table
    op.create_table(
        "signals",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("canonical_node_id", sa.String(length=255), nullable=False),
        sa.Column("metric_id", sa.String(length=100), nullable=False),
        sa.Column("severity", signal_severity, nullable=False),
        sa.Column("domain", signal_domain, nullable=False),
        sa.Column("facility", sa.String(length=255), nullable=False),
        sa.Column("facility_id", sa.String(length=50), nullable=True),
        sa.Column("service_line", sa.String(length=255), nullable=False),
        sa.Column("sub_service_line", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metric_value", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("benchmark_value", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("variance_percent", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("percentile_rank", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("z_score", sa.Numeric(precision=6, scale=3), nullable=True),
        sa.Column("encounters", sa.Integer(), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("node_result_path", sa.String(length=500), nullable=True),
        sa.Column("contribution_path", sa.String(length=500), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_signals")),
        sa.UniqueConstraint(
            "canonical_node_id",
            "metric_id",
            "detected_at",
            name="uq_signals_canonical_metric_detected",
        ),
    )
    op.create_index("ix_signals_severity", "signals", ["severity"], unique=False)
    op.create_index("ix_signals_domain", "signals", ["domain"], unique=False)
    op.create_index("ix_signals_facility", "signals", ["facility"], unique=False)
    op.create_index("ix_signals_service_line", "signals", ["service_line"], unique=False)
    op.create_index("ix_signals_detected_at", "signals", ["detected_at"], unique=False)
    op.create_index("ix_signals_metric_id", "signals", ["metric_id"], unique=False)
    op.create_index(
        "ix_signals_composite",
        "signals",
        ["severity", "domain", "detected_at"],
        unique=False,
    )

    # Create assignments table
    op.create_table(
        "assignments",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("signal_id", sa.UUID(), nullable=False),
        sa.Column("assignee_id", sa.UUID(), nullable=True),
        sa.Column("assigner_id", sa.UUID(), nullable=True),
        sa.Column(
            "status",
            assignment_status,
            server_default="NEW",
            nullable=False,
        ),
        sa.Column("role_type", assignment_role_type, nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("resolution_notes", sa.Text(), nullable=True),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["assignee_id"],
            ["users.id"],
            name=op.f("fk_assignments_assignee_id_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["assigner_id"],
            ["users.id"],
            name=op.f("fk_assignments_assigner_id_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["signals.id"],
            name=op.f("fk_assignments_signal_id_signals"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_assignments")),
        sa.UniqueConstraint("signal_id", name=op.f("uq_assignments_signal_id")),
    )
    op.create_index("ix_assignments_signal_id", "assignments", ["signal_id"], unique=False)
    op.create_index("ix_assignments_assignee_id", "assignments", ["assignee_id"], unique=False)
    op.create_index("ix_assignments_status", "assignments", ["status"], unique=False)
    op.create_index(
        "ix_assignments_status_assignee",
        "assignments",
        ["status", "assignee_id"],
        unique=False,
    )

    # Create activity_events table
    op.create_table(
        "activity_events",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("event_type", event_type, nullable=False),
        sa.Column("signal_id", sa.UUID(), nullable=True),
        sa.Column("user_id", sa.UUID(), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="{}",
            nullable=False,
        ),
        sa.Column("read", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["signals.id"],
            name=op.f("fk_activity_events_signal_id_signals"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_activity_events_user_id_users"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_activity_events")),
    )
    op.create_index("ix_activity_events_signal_id", "activity_events", ["signal_id"], unique=False)
    op.create_index("ix_activity_events_user_id", "activity_events", ["user_id"], unique=False)
    op.create_index("ix_activity_events_event_type", "activity_events", ["event_type"], unique=False)
    op.create_index("ix_activity_events_created_at", "activity_events", ["created_at"], unique=False)
    # Partial index for unread events
    op.create_index(
        "ix_activity_events_unread",
        "activity_events",
        ["read", "created_at"],
        unique=False,
        postgresql_where=sa.text("read = FALSE"),
    )

    # Create updated_at trigger function
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Apply trigger to tables with updated_at
    for table in ["users", "signals", "assignments"]:
        op.execute(f"""
            CREATE TRIGGER update_{table}_updated_at
                BEFORE UPDATE ON {table}
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)


def downgrade() -> None:
    """Drop all tables and enum types."""
    # Drop triggers
    for table in ["users", "signals", "assignments"]:
        op.execute(f"DROP TRIGGER IF EXISTS update_{table}_updated_at ON {table}")

    # Drop trigger function
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")

    # Drop tables (reverse order due to foreign keys)
    op.drop_table("activity_events")
    op.drop_table("assignments")
    op.drop_table("signals")
    op.drop_table("users")

    # Drop enum types
    event_type.drop(op.get_bind(), checkfirst=True)
    assignment_role_type.drop(op.get_bind(), checkfirst=True)
    assignment_status.drop(op.get_bind(), checkfirst=True)
    signal_domain.drop(op.get_bind(), checkfirst=True)
    signal_severity.drop(op.get_bind(), checkfirst=True)
    user_role.drop(op.get_bind(), checkfirst=True)
