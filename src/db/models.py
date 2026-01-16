"""SQLAlchemy 2.0 models for Quality Compass database.

Defines the core domain models:
- User: User accounts and authentication
- Signal: Detected quality signals from Project Needle
- Assignment: Signal assignment workflow state
- ActivityEvent: Activity feed events

All models use UUID primary keys and timezone-aware timestamps.
"""

import enum
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base

if TYPE_CHECKING:
    pass


# =============================================================================
# Enums
# =============================================================================


class UserRole(str, enum.Enum):
    """User role enumeration for authorization.

    Attributes:
        ADMIN: Full system access
        CLINICAL_LEADERSHIP: Clinical leadership role
        NURSING_LEADERSHIP: Nursing leadership role
        ADMINISTRATION: Administrative role
        VIEWER: Read-only access
    """

    ADMIN = "admin"
    CLINICAL_LEADERSHIP = "clinical_leadership"
    NURSING_LEADERSHIP = "nursing_leadership"
    ADMINISTRATION = "administration"
    VIEWER = "viewer"


class SignalDomain(str, enum.Enum):
    """Quality domains for signal classification.

    Attributes:
        EFFICIENCY: Operational efficiency metrics
        SAFETY: Patient safety metrics
        EFFECTIVENESS: Clinical effectiveness metrics
    """

    EFFICIENCY = "Efficiency"
    SAFETY = "Safety"
    EFFECTIVENESS = "Effectiveness"


class MetricPolarity(str, enum.Enum):
    """Polarity indicating which direction is clinically desirable.

    Used to determine whether a statistical deviation (high or low)
    represents favorable or unfavorable performance.

    Attributes:
        LOWER_IS_BETTER: Lower values indicate better performance (e.g., LOS, readmission rate)
        HIGHER_IS_BETTER: Higher values indicate better performance (e.g., patient satisfaction)
        TARGET_RANGE: Both extremes can be unfavorable (e.g., lab values with optimal range)
    """

    LOWER_IS_BETTER = "lower_is_better"
    HIGHER_IS_BETTER = "higher_is_better"
    TARGET_RANGE = "target_range"


class MagnitudeTier(str, enum.Enum):
    """Magnitude tier based on aggregate statistics.

    Indicates the severity of deviation from peer benchmarks.

    Attributes:
        CRITICAL: Extreme deviation (percentile >= 99 or |z| >= 3.0)
        SEVERE: Very high deviation (percentile 95-98.9 or |z| 2.5-2.99)
        ELEVATED: High deviation (percentile 85-94.9 or |z| 1.5-2.49)
        MARGINAL: Moderate deviation (percentile 75-84.9 or |z| 1.0-1.49)
        EXPECTED: Within normal range (percentile 25-74.9 or |z| < 1.0)
        FAVORABLE: Better than expected (percentile 10-24.9)
        EXCELLENT: Best performers (percentile < 10)
    """

    CRITICAL = "critical"
    SEVERE = "severe"
    ELEVATED = "elevated"
    MARGINAL = "marginal"
    EXPECTED = "expected"
    FAVORABLE = "favorable"
    EXCELLENT = "excellent"


class TrajectoryTier(str, enum.Enum):
    """Trajectory tier based on temporal statistics.

    Indicates the direction and speed of change over time.

    Attributes:
        RAPIDLY_DETERIORATING: Fast worsening (slope percentile >= 90)
        DETERIORATING: Worsening (slope percentile 70-89.9)
        STABLE: Flat trend (slope percentile 30-69.9)
        IMPROVING: Getting better (slope percentile 10-29.9)
        RAPIDLY_IMPROVING: Fast improvement (slope percentile < 10)
    """

    RAPIDLY_DETERIORATING = "rapidly_deteriorating"
    DETERIORATING = "deteriorating"
    STABLE = "stable"
    IMPROVING = "improving"
    RAPIDLY_IMPROVING = "rapidly_improving"


class ConsistencyTier(str, enum.Enum):
    """Consistency tier based on temporal coefficient of variation.

    Indicates the stability of the signal pattern over time.

    Attributes:
        PERSISTENT: Low variance (CV < 0.30, 6+ periods)
        VARIABLE: Moderate variance (CV 0.30-0.70)
        TRANSIENT: High variance or insufficient history (CV > 0.70 or < 3 periods)
    """

    PERSISTENT = "persistent"
    VARIABLE = "variable"
    TRANSIENT = "transient"


class AssignmentStatus(str, enum.Enum):
    """Assignment workflow states.

    Attributes:
        NEW: Signal detected, not yet assigned
        ASSIGNED: Assigned to user, work not started
        IN_PROGRESS: Work in progress
        RESOLVED: Issue resolved by assignee
        CLOSED: Verified and closed
    """

    NEW = "new"
    ASSIGNED = "assigned"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    CLOSED = "closed"


class AssignmentRoleType(str, enum.Enum):
    """Role types that can be assigned signals.

    Attributes:
        CLINICAL_LEADERSHIP: Clinical leadership
        NURSING_LEADERSHIP: Nursing leadership
        ADMINISTRATION: Administrative staff
    """

    CLINICAL_LEADERSHIP = "clinical_leadership"
    NURSING_LEADERSHIP = "nursing_leadership"
    ADMINISTRATION = "administration"


class EventType(str, enum.Enum):
    """Activity event types.

    Attributes:
        NEW_SIGNAL: New signal detected
        REGRESSION: Metric regressed
        IMPROVEMENT: Metric improved
        INSIGHT: AI-generated insight
        TECHNICAL_ERROR: System error
        ASSIGNMENT: Signal assigned to user
        STATUS_CHANGE: Workflow status changed
        COMMENT: User comment added
        INTERVENTION_ACTIVATED: Intervention started
        INTERVENTION_OUTCOME: Intervention outcome measured
    """

    NEW_SIGNAL = "new_signal"
    REGRESSION = "regression"
    IMPROVEMENT = "improvement"
    INSIGHT = "insight"
    TECHNICAL_ERROR = "technical_error"
    ASSIGNMENT = "assignment"
    STATUS_CHANGE = "status_change"
    COMMENT = "comment"
    INTERVENTION_ACTIVATED = "intervention_activated"
    INTERVENTION_OUTCOME = "intervention_outcome"


# =============================================================================
# Models
# =============================================================================


class User(Base):
    """User account model for authentication and authorization.

    Attributes:
        id: Unique identifier (UUID).
        email: Unique email address.
        name: Display name.
        hashed_password: bcrypt password hash.
        role: User role for authorization.
        is_active: Soft delete flag.
        created_at: Account creation timestamp.
        updated_at: Last modification timestamp.

    Relationships:
        assigned_signals: Signals assigned to this user.
        assigned_by_signals: Signals this user has assigned to others.
        activity_events: Activity events created by this user.

    Example:
        >>> user = User(
        ...     email="admin@example.com",
        ...     name="Admin User",
        ...     hashed_password="$2b$12$...",
        ...     role=UserRole.ADMIN,
        ... )
    """

    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    hashed_password: Mapped[str] = mapped_column(String(255))
    role: Mapped[UserRole] = mapped_column(default=UserRole.VIEWER)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(server_default=text("NOW()"), onupdate=datetime.now)

    # Relationships
    assigned_signals: Mapped[list["Assignment"]] = relationship(
        "Assignment",
        foreign_keys="Assignment.assignee_id",
        back_populates="assignee",
    )
    assigned_by_signals: Mapped[list["Assignment"]] = relationship(
        "Assignment",
        foreign_keys="Assignment.assigner_id",
        back_populates="assigner",
    )
    activity_events: Mapped[list["ActivityEvent"]] = relationship(
        "ActivityEvent",
        back_populates="user",
    )

    __table_args__ = (Index("ix_users_role", "role"),)


class Signal(Base):
    """Detected quality signal from Project Needle analysis.

    Represents an anomaly detected in clinical metrics that requires attention.
    Signals are consolidated per entity/metric pair, combining aggregate and
    temporal statistical analysis into a single record.

    Attributes:
        id: Unique identifier (UUID).
        canonical_node_id: Reference to source node in insight graph.
        metric_id: Metric identifier (e.g., "losIndex").
        domain: Quality domain classification.
        facility: Facility/Site of Care name.
        facility_id: Medicare ID or facility code.
        service_line: Primary service line.
        sub_service_line: Sub-service line if present.
        description: Human-readable signal description.
        metric_value: Current metric value from aggregate node.
        peer_mean: Peer cohort mean (benchmark value).
        peer_std: Peer cohort standard deviation.
        percentile_rank: Position in peer distribution.
        encounters: Number of patient encounters.
        detected_at: When anomaly was detected.
        created_at: Record creation timestamp.
        updated_at: Last modification timestamp.
        simplified_signal_type: One of 9 signal types (e.g., "critical_trajectory", "baseline").
        simplified_severity: Severity score 0-100 for triage.
        simplified_severity_range: [min, max] severity range for signal type.
        simplified_inputs: 5 classification input values.
        simplified_indicators: Categorical interpretations of inputs.
        simplified_reasoning: Human-readable classification reasoning.
        simplified_severity_calculation: Breakdown of severity score calculation.
        temporal_node_id: Reference to temporal node via trends_to edge.
        entity_dimensions: Entity dimension key-value pairs (excluding medicareId).
        entity_dimensions_hash: MD5 hash of entity_dimensions for unique constraint.
        groupby_label: Human-readable label for groupby dimension(s).
        group_value: Entity dimension value(s) for the group.
        metric_trend_timeline: Timeline of metric values for sparkline visualization.
        trend_direction: Direction of trend (increasing, decreasing, stable).

    Relationships:
        assignment: Current assignment for this signal.
        activity_events: Activity events related to this signal.

    Example:
        >>> signal = Signal(
        ...     canonical_node_id="losIndex__medicareId__aggregate_time_period",
        ...     metric_id="losIndex",
        ...     domain=SignalDomain.EFFICIENCY,
        ...     facility="General Hospital",
        ...     service_line="Inpatient",
        ...     description="LOS Index above benchmark",
        ...     metric_value=Decimal("1.25"),
        ...     groupby_label="Facility-wide",
        ...     group_value="Facility-wide",
        ...     detected_at=datetime.now(tz=timezone.utc),
        ... )
    """

    __tablename__ = "signals"

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    canonical_node_id: Mapped[str] = mapped_column(String(255))
    metric_id: Mapped[str] = mapped_column(String(100))
    domain: Mapped[SignalDomain]
    facility: Mapped[str] = mapped_column(String(255))
    facility_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    system_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    service_line: Mapped[str] = mapped_column(String(255))
    sub_service_line: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description: Mapped[str] = mapped_column(Text)
    metric_value: Mapped[Decimal] = mapped_column(Numeric(10, 4))
    peer_mean: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    peer_std: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    percentile_rank: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    encounters: Mapped[int | None] = mapped_column(Integer, nullable=True)
    detected_at: Mapped[datetime]
    created_at: Mapped[datetime] = mapped_column(server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(server_default=text("NOW()"), onupdate=datetime.now)

    # 9 Signal Type classification system
    simplified_signal_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    simplified_severity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    simplified_severity_range: Mapped[list[int] | None] = mapped_column(JSONB, nullable=True)
    simplified_inputs: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    simplified_indicators: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    simplified_reasoning: Mapped[str | None] = mapped_column(Text, nullable=True)
    simplified_severity_calculation: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    # Temporal node reference
    temporal_node_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Entity identification and grouping (for signal consolidation)
    entity_dimensions: Mapped[dict[str, str] | None] = mapped_column(JSONB, nullable=True)
    entity_dimensions_hash: Mapped[str | None] = mapped_column(String(32), nullable=True)
    groupby_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    group_value: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Metric trend timeline for sparkline visualization
    metric_trend_timeline: Mapped[list[dict[str, object]] | None] = mapped_column(JSONB, nullable=True)

    # Additional temporal statistics
    trend_direction: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Business impact metrics (for wireframe Overview tab)
    annual_excess_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    excess_los_days: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    capacity_impact_bed_days: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    expected_metric_value: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    why_matters_narrative: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Extensible metadata for future fields
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, nullable=True)

    # Per-period metadata for temporal analysis
    metadata_per_period: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(astext_type=Text()),
        nullable=True,
        comment="Per-period breakdown of metadata values (encounters per period, etc.)",
    )

    # Peer percentile reference trends for temporal context
    peer_percentile_trends: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(astext_type=Text()),
        nullable=True,
        comment="Peer distribution percentile trends (p10, p25, p50, p75, p90) at each time period",
    )

    # Relationships
    assignment: Mapped["Assignment | None"] = relationship(
        "Assignment",
        back_populates="signal",
        uselist=False,
    )
    activity_events: Mapped[list["ActivityEvent"]] = relationship(
        "ActivityEvent",
        back_populates="signal",
    )

    __table_args__ = (
        UniqueConstraint("canonical_node_id", "metric_id", "facility_id", "entity_dimensions_hash", "detected_at", name="uq_signals_entity_metric_detected"),
        Index("ix_signals_domain", "domain"),
        Index("ix_signals_facility", "facility"),
        Index("ix_signals_system_name", "system_name"),
        Index("ix_signals_service_line", "service_line"),
        Index("ix_signals_detected_at", "detected_at"),
        Index("ix_signals_metric_id", "metric_id"),
        Index("ix_signals_groupby_label", "groupby_label"),
        Index("ix_signals_entity_dimensions", "entity_dimensions", postgresql_using="gin"),
        Index("ix_signals_simplified_signal_type", "simplified_signal_type"),
        Index("ix_signals_simplified_severity", "simplified_severity"),
    )


class Assignment(Base):
    """Signal assignment and workflow state.

    Tracks the assignment workflow for each signal. One active assignment
    per signal at a time.

    Attributes:
        id: Unique identifier (UUID).
        signal_id: Foreign key to signal.
        assignee_id: Foreign key to assigned user.
        assigner_id: Foreign key to user who assigned.
        status: Current workflow status.
        role_type: Role type for assignment.
        notes: Assignment notes.
        resolution_notes: Notes on how signal was resolved.
        assigned_at: When first assigned.
        started_at: When work started.
        resolved_at: When marked resolved.
        closed_at: When closed.
        created_at: Record creation timestamp.
        updated_at: Last modification timestamp.

    Relationships:
        signal: The signal being assigned.
        assignee: User assigned to the signal.
        assigner: User who made the assignment.

    Example:
        >>> assignment = Assignment(
        ...     signal_id=signal.id,
        ...     status=AssignmentStatus.NEW,
        ... )
    """

    __tablename__ = "assignments"

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    signal_id: Mapped[UUID] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), unique=True)
    assignee_id: Mapped[UUID | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    assigner_id: Mapped[UUID | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    status: Mapped[AssignmentStatus] = mapped_column(default=AssignmentStatus.NEW)
    role_type: Mapped[AssignmentRoleType | None] = mapped_column(nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    assigned_at: Mapped[datetime | None] = mapped_column(nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=text("NOW()"))
    updated_at: Mapped[datetime] = mapped_column(server_default=text("NOW()"), onupdate=datetime.now)

    # Relationships
    signal: Mapped["Signal"] = relationship("Signal", back_populates="assignment")
    assignee: Mapped["User | None"] = relationship(
        "User",
        foreign_keys=[assignee_id],
        back_populates="assigned_signals",
    )
    assigner: Mapped["User | None"] = relationship(
        "User",
        foreign_keys=[assigner_id],
        back_populates="assigned_by_signals",
    )

    __table_args__ = (
        Index("ix_assignments_signal_id", "signal_id"),
        Index("ix_assignments_assignee_id", "assignee_id"),
        Index("ix_assignments_status", "status"),
        Index("ix_assignments_status_assignee", "status", "assignee_id"),
    )


class ActivityEvent(Base):
    """Activity feed event.

    Stores all activity events including system-generated and user-triggered
    events for the activity feed.

    Attributes:
        id: Unique identifier (UUID).
        event_type: Type of event.
        signal_id: Foreign key to related signal (optional).
        user_id: Foreign key to user who triggered event (null for system).
        payload: Event-specific data as JSONB.
        read: Whether event has been read.
        created_at: Event timestamp.

    Relationships:
        signal: Related signal (if applicable).
        user: User who triggered the event (if applicable).

    Example:
        >>> event = ActivityEvent(
        ...     event_type=EventType.NEW_SIGNAL,
        ...     signal_id=signal.id,
        ...     payload={"significance": "Extreme", "metric": "losIndex"},
        ... )
    """

    __tablename__ = "activity_events"

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    event_type: Mapped[EventType]
    signal_id: Mapped[UUID | None] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), nullable=True)
    user_id: Mapped[UUID | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=text("NOW()"))

    # Relationships
    signal: Mapped["Signal | None"] = relationship("Signal", back_populates="activity_events")
    user: Mapped["User | None"] = relationship("User", back_populates="activity_events")

    __table_args__ = (
        Index("ix_activity_events_signal_id", "signal_id"),
        Index("ix_activity_events_user_id", "user_id"),
        Index("ix_activity_events_event_type", "event_type"),
        Index("ix_activity_events_created_at", "created_at"),
        Index(
            "ix_activity_events_unread",
            "read",
            "created_at",
            postgresql_where="read = FALSE",
        ),
    )
