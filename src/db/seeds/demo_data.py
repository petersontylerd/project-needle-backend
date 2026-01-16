"""Demo seed data for Quality Compass.

This module provides functions to seed the database with realistic demo data
for testing and demonstration purposes. Creates 10 signals, 3 users, and
20 activity events that mirror real healthcare quality metrics scenarios.

Usage:
    # As a script
    python -m src.db.seeds.demo_data

    # Or import and call
    from src.db.seeds.demo_data import seed_demo_data
    await seed_demo_data(session)
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db.models import (
    ActivityEvent,
    Assignment,
    AssignmentRoleType,
    AssignmentStatus,
    EventType,
    Signal,
    SignalDomain,
    User,
    UserRole,
)


def _now() -> datetime:
    """Get current UTC datetime.

    Returns:
        datetime: Current datetime with UTC timezone.
    """
    return datetime.now(tz=UTC)


def _days_ago(days: int) -> datetime:
    """Get datetime for specified number of days ago.

    Args:
        days: Number of days in the past.

    Returns:
        datetime: UTC datetime for that many days ago.
    """
    return _now() - timedelta(days=days)


def _hours_ago(hours: int) -> datetime:
    """Get datetime for specified number of hours ago.

    Args:
        hours: Number of hours in the past.

    Returns:
        datetime: UTC datetime for that many hours ago.
    """
    return _now() - timedelta(hours=hours)


# =============================================================================
# Demo Users
# =============================================================================


def create_demo_users() -> list[User]:
    """Create demo user records for testing.

    Creates 3 users representing different leadership roles:
    - Clinical Leadership (Dr. Sarah Chen)
    - Nursing Leadership (Dr. Michael Johnson)
    - Administration (Jennifer Martinez)

    Returns:
        list[User]: List of 3 demo User objects with preset UUIDs.

    Example:
        >>> users = create_demo_users()
        >>> len(users)
        3
        >>> users[0].name
        'Dr. Sarah Chen'
    """
    return [
        User(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            email="sarah.chen@hospital.example",
            name="Dr. Sarah Chen",
            hashed_password="$2b$12$demo_hash_not_real_password_1",
            role=UserRole.CLINICAL_LEADERSHIP,
            is_active=True,
        ),
        User(
            id=UUID("00000000-0000-0000-0000-000000000002"),
            email="michael.johnson@hospital.example",
            name="Dr. Michael Johnson",
            hashed_password="$2b$12$demo_hash_not_real_password_2",
            role=UserRole.NURSING_LEADERSHIP,
            is_active=True,
        ),
        User(
            id=UUID("00000000-0000-0000-0000-000000000003"),
            email="jennifer.martinez@hospital.example",
            name="Jennifer Martinez",
            hashed_password="$2b$12$demo_hash_not_real_password_3",
            role=UserRole.ADMINISTRATION,
            is_active=True,
        ),
    ]


# =============================================================================
# Demo Signals
# =============================================================================


def create_demo_signals() -> list[Signal]:
    """Create demo signal records for testing.

    Creates 10 signals across various domains and service lines representing
    different quality metrics and facilities.

    Returns:
        list[Signal]: List of 10 demo Signal objects with preset UUIDs.

    Example:
        >>> signals = create_demo_signals()
        >>> len(signals)
        10
        >>> signals[0].domain
        <SignalDomain.EFFICIENCY: 'Efficiency'>
    """
    return [
        # Critical Signals (3)
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000001"),
            canonical_node_id="SoC:facility_001/cardiology/cardiac_icu",
            metric_id="losIndex",
            domain=SignalDomain.EFFICIENCY,
            facility="University Medical Center",
            facility_id="450001",
            service_line="Cardiology",
            sub_service_line="Cardiac Intensive Care",
            description="Cardiac Surgery Length of Stay trending 31% above benchmark. Average LOS of 10.6 days vs peer benchmark of 8.1 days.",
            metric_value=Decimal("10.6"),
            peer_mean=Decimal("8.1"),
            percentile_rank=Decimal("92.5"),
            encounters=847,
            detected_at=_days_ago(4),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000002"),
            canonical_node_id="SoC:facility_001/surgery/general",
            metric_id="clabsiRate",
            domain=SignalDomain.SAFETY,
            facility="University Medical Center",
            facility_id="450001",
            service_line="Surgery",
            sub_service_line="General Surgery",
            description="CLABSI rate 62% above benchmark at 2.1 per 1,000 line days vs peer benchmark of 1.3.",
            metric_value=Decimal("2.1"),
            peer_mean=Decimal("1.3"),
            percentile_rank=Decimal("95.0"),
            encounters=1205,
            detected_at=_days_ago(2),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000003"),
            canonical_node_id="SoC:facility_002/medicine/pulmonary",
            metric_id="readmissionRate",
            domain=SignalDomain.EFFECTIVENESS,
            facility="Community General Hospital",
            facility_id="450002",
            service_line="Medicine",
            sub_service_line="Pulmonary",
            description="30-day readmission rate at 28.4% vs benchmark 18.2%, primarily driven by COPD exacerbations.",
            metric_value=Decimal("28.4"),
            peer_mean=Decimal("18.2"),
            percentile_rank=Decimal("94.0"),
            encounters=562,
            detected_at=_days_ago(1),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        # High Severity Signals (3)
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000004"),
            canonical_node_id="SoC:facility_001/orthopedics/spine",
            metric_id="fallRate",
            domain=SignalDomain.SAFETY,
            facility="University Medical Center",
            facility_id="450001",
            service_line="Orthopedics",
            sub_service_line="Spine Surgery",
            description="Fall rate trending upward at 4.2 per 1,000 patient days vs benchmark 2.8. Post-operative mobility protocols under review.",
            metric_value=Decimal("4.2"),
            peer_mean=Decimal("2.8"),
            percentile_rank=Decimal("88.0"),
            encounters=423,
            detected_at=_days_ago(5),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000005"),
            canonical_node_id="SoC:facility_002/cardiology/heart_failure",
            metric_id="mortalityRate",
            domain=SignalDomain.EFFECTIVENESS,
            facility="Community General Hospital",
            facility_id="450002",
            service_line="Cardiology",
            sub_service_line="Heart Failure",
            description="Risk-adjusted mortality at 4.8% vs expected 3.2%. Analysis indicates delayed recognition of sepsis in HF patients.",
            metric_value=Decimal("4.8"),
            peer_mean=Decimal("3.2"),
            percentile_rank=Decimal("85.0"),
            encounters=312,
            detected_at=_days_ago(3),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000006"),
            canonical_node_id="SoC:facility_001/ed/main",
            metric_id="throughput",
            domain=SignalDomain.EFFICIENCY,
            facility="University Medical Center",
            facility_id="450001",
            service_line="Emergency",
            sub_service_line=None,
            description="ED throughput time averaging 312 minutes, 24% above target of 252 minutes. Boarding hours contributing factor.",
            metric_value=Decimal("312.0"),
            peer_mean=Decimal("252.0"),
            percentile_rank=Decimal("78.0"),
            encounters=8234,
            detected_at=_days_ago(6),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        # Moderate Severity Signals (2)
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000007"),
            canonical_node_id="SoC:facility_002/surgery/trauma",
            metric_id="vaeRate",
            domain=SignalDomain.SAFETY,
            facility="Community General Hospital",
            facility_id="450002",
            service_line="Surgery",
            sub_service_line="Trauma",
            description="VAE rate at 8.5 per 1,000 ventilator days vs benchmark 6.8. Ventilator bundle compliance audit initiated.",
            metric_value=Decimal("8.5"),
            peer_mean=Decimal("6.8"),
            percentile_rank=Decimal("72.0"),
            encounters=189,
            detected_at=_days_ago(7),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000008"),
            canonical_node_id="SoC:facility_001/medicine/hospitalist",
            metric_id="averageLos",
            domain=SignalDomain.EFFICIENCY,
            facility="University Medical Center",
            facility_id="450001",
            service_line="Medicine",
            sub_service_line="Hospitalist",
            description="Average LOS at 5.2 days vs benchmark 4.6 days. Discharge planning delays identified as primary driver.",
            metric_value=Decimal("5.2"),
            peer_mean=Decimal("4.6"),
            percentile_rank=Decimal("65.0"),
            encounters=2456,
            detected_at=_days_ago(8),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        # Watch Severity Signals (2)
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000009"),
            canonical_node_id="SoC:facility_002/pediatrics/nicu",
            metric_id="readmissionRate",
            domain=SignalDomain.EFFECTIVENESS,
            facility="Community General Hospital",
            facility_id="450002",
            service_line="Pediatrics",
            sub_service_line="NICU",
            description="7-day readmission rate at 3.8% vs benchmark 3.2%. Monitoring feeding protocol adherence.",
            metric_value=Decimal("3.8"),
            peer_mean=Decimal("3.2"),
            percentile_rank=Decimal("58.0"),
            encounters=167,
            detected_at=_days_ago(10),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
        Signal(
            id=UUID("10000000-0000-0000-0000-000000000010"),
            canonical_node_id="SoC:facility_001/oncology/medical",
            metric_id="clabsiRate",
            domain=SignalDomain.SAFETY,
            facility="University Medical Center",
            facility_id="450001",
            service_line="Oncology",
            sub_service_line="Medical Oncology",
            description="CLABSI rate at 1.5 per 1,000 line days vs benchmark 1.3. Port access protocols under review.",
            metric_value=Decimal("1.5"),
            peer_mean=Decimal("1.3"),
            percentile_rank=Decimal("55.0"),
            encounters=543,
            detected_at=_days_ago(12),
            groupby_label="Vizient Service Line",
            group_value="All Service Lines",
        ),
    ]


# =============================================================================
# Demo Assignments
# =============================================================================


def create_demo_assignments(signals: list[Signal], users: list[User]) -> list[Assignment]:
    """Create demo assignment records linking signals to users.

    Creates assignments for signals in various workflow states:
    - 2 NEW (unassigned)
    - 3 ASSIGNED
    - 2 IN_PROGRESS
    - 2 RESOLVED
    - 1 CLOSED

    Args:
        signals: List of Signal objects to create assignments for.
        users: List of User objects to assign signals to.

    Returns:
        list[Assignment]: List of 10 demo Assignment objects.

    Raises:
        ValueError: If signals or users list is empty.

    Example:
        >>> signals = create_demo_signals()
        >>> users = create_demo_users()
        >>> assignments = create_demo_assignments(signals, users)
        >>> len(assignments)
        10
    """
    if not signals or not users:
        raise ValueError("Signals and users lists must not be empty")

    dr_chen = users[0]  # Clinical Leadership
    dr_johnson = users[1]  # Nursing Leadership
    jennifer = users[2]  # Administration

    return [
        # NEW signals (2) - signals[0], signals[1]
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000001"),
            signal_id=signals[0].id,
            status=AssignmentStatus.NEW,
        ),
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000002"),
            signal_id=signals[1].id,
            status=AssignmentStatus.NEW,
        ),
        # ASSIGNED signals (3) - signals[2], signals[3], signals[4]
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000003"),
            signal_id=signals[2].id,
            assignee_id=dr_chen.id,
            assigner_id=jennifer.id,
            status=AssignmentStatus.ASSIGNED,
            role_type=AssignmentRoleType.CLINICAL_LEADERSHIP,
            notes="High priority - review readmission patterns",
            assigned_at=_days_ago(1),
        ),
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000004"),
            signal_id=signals[3].id,
            assignee_id=dr_johnson.id,
            assigner_id=jennifer.id,
            status=AssignmentStatus.ASSIGNED,
            role_type=AssignmentRoleType.NURSING_LEADERSHIP,
            notes="Review fall prevention protocols with nursing staff",
            assigned_at=_days_ago(4),
        ),
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000005"),
            signal_id=signals[4].id,
            assignee_id=dr_chen.id,
            assigner_id=jennifer.id,
            status=AssignmentStatus.ASSIGNED,
            role_type=AssignmentRoleType.CLINICAL_LEADERSHIP,
            notes="Urgent: mortality variance requires immediate attention",
            assigned_at=_days_ago(2),
        ),
        # IN_PROGRESS signals (2) - signals[5], signals[6]
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000006"),
            signal_id=signals[5].id,
            assignee_id=jennifer.id,
            assigner_id=dr_chen.id,
            status=AssignmentStatus.IN_PROGRESS,
            role_type=AssignmentRoleType.ADMINISTRATION,
            notes="Working with ED leadership on throughput improvements",
            assigned_at=_days_ago(5),
            started_at=_days_ago(4),
        ),
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000007"),
            signal_id=signals[6].id,
            assignee_id=dr_johnson.id,
            assigner_id=jennifer.id,
            status=AssignmentStatus.IN_PROGRESS,
            role_type=AssignmentRoleType.NURSING_LEADERSHIP,
            notes="VAE bundle compliance audit underway",
            assigned_at=_days_ago(6),
            started_at=_days_ago(5),
        ),
        # RESOLVED signals (2) - signals[7], signals[8]
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000008"),
            signal_id=signals[7].id,
            assignee_id=dr_chen.id,
            assigner_id=jennifer.id,
            status=AssignmentStatus.RESOLVED,
            role_type=AssignmentRoleType.CLINICAL_LEADERSHIP,
            notes="Discharge planning initiative assigned",
            resolution_notes="Implemented daily discharge planning huddles. Initial data shows 15% reduction in discharge delays.",
            assigned_at=_days_ago(7),
            started_at=_days_ago(6),
            resolved_at=_days_ago(1),
        ),
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000009"),
            signal_id=signals[8].id,
            assignee_id=dr_johnson.id,
            assigner_id=dr_chen.id,
            status=AssignmentStatus.RESOLVED,
            role_type=AssignmentRoleType.NURSING_LEADERSHIP,
            notes="Monitor NICU readmission patterns",
            resolution_notes="Enhanced parent education program implemented. Readmission rate trending down over past 2 weeks.",
            assigned_at=_days_ago(9),
            started_at=_days_ago(8),
            resolved_at=_days_ago(2),
        ),
        # CLOSED signals (1) - signals[9]
        Assignment(
            id=UUID("20000000-0000-0000-0000-000000000010"),
            signal_id=signals[9].id,
            assignee_id=dr_chen.id,
            assigner_id=jennifer.id,
            status=AssignmentStatus.CLOSED,
            role_type=AssignmentRoleType.CLINICAL_LEADERSHIP,
            notes="Review port access procedures",
            resolution_notes="Port access protocol updated. CLABSI rate normalized.",
            assigned_at=_days_ago(11),
            started_at=_days_ago(10),
            resolved_at=_days_ago(5),
            closed_at=_days_ago(3),
        ),
    ]


# =============================================================================
# Demo Activity Events
# =============================================================================


def create_demo_activity_events(signals: list[Signal], users: list[User]) -> list[ActivityEvent]:
    """Create demo activity events for the feed.

    Creates 20 activity events of various types:
    - 3 new_signal events
    - 2 regression events
    - 2 improvement events
    - 2 insight events
    - 1 technical_error event
    - 4 assignment events
    - 3 status_change events
    - 2 comment events
    - 1 intervention_activated event

    Args:
        signals: List of Signal objects to reference in events.
        users: List of User objects to reference in events.

    Returns:
        list[ActivityEvent]: List of 20 demo ActivityEvent objects.

    Raises:
        ValueError: If signals or users list is empty.

    Example:
        >>> signals = create_demo_signals()
        >>> users = create_demo_users()
        >>> events = create_demo_activity_events(signals, users)
        >>> len(events)
        20
    """
    if not signals or not users:
        raise ValueError("Signals and users lists must not be empty")

    dr_chen = users[0]
    dr_johnson = users[1]
    jennifer = users[2]

    return [
        # New Signal Events (3)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000001"),
            event_type=EventType.NEW_SIGNAL,
            signal_id=signals[0].id,
            payload={
                "signal_type": "critical_trajectory",
                "severity": 85,
                "domain": "Efficiency",
                "service_line": "Cardiology",
                "description": "Cardiac Surgery LOS trending above benchmark",
                "variance_percent": 30.86,
            },
            read=True,
            created_at=_days_ago(4),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000002"),
            event_type=EventType.NEW_SIGNAL,
            signal_id=signals[1].id,
            payload={
                "signal_type": "chronic_underperformer",
                "severity": 75,
                "domain": "Safety",
                "service_line": "Surgery",
                "description": "CLABSI rate above benchmark",
                "variance_percent": 61.54,
            },
            read=True,
            created_at=_days_ago(2),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000003"),
            event_type=EventType.NEW_SIGNAL,
            signal_id=signals[2].id,
            payload={
                "signal_type": "emerging_risk",
                "severity": 65,
                "domain": "Effectiveness",
                "service_line": "Medicine",
                "description": "Readmission rate elevated",
                "variance_percent": 56.04,
            },
            read=False,
            created_at=_days_ago(1),
        ),
        # Regression Events (2)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000004"),
            event_type=EventType.REGRESSION,
            signal_id=signals[3].id,
            payload={
                "description": "Fall rate increased from 3.8 to 4.2 per 1,000 patient days",
                "previous_value": 3.8,
                "current_value": 4.2,
                "percent_change": 10.5,
            },
            read=True,
            created_at=_days_ago(5),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000005"),
            event_type=EventType.REGRESSION,
            signal_id=signals[4].id,
            payload={
                "description": "Mortality rate trending upward",
                "previous_value": 4.2,
                "current_value": 4.8,
                "percent_change": 14.3,
            },
            read=False,
            created_at=_days_ago(3),
        ),
        # Improvement Events (2)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000006"),
            event_type=EventType.IMPROVEMENT,
            signal_id=signals[7].id,
            payload={
                "intervention_name": "Discharge Planning Huddles",
                "before_value": 5.2,
                "after_value": 4.8,
                "percent_change": -7.7,
                "effectiveness_label": "MODERATELY_EFFECTIVE",
            },
            read=True,
            created_at=_days_ago(1),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000007"),
            event_type=EventType.IMPROVEMENT,
            signal_id=signals[9].id,
            payload={
                "intervention_name": "Port Access Protocol Update",
                "before_value": 1.5,
                "after_value": 1.3,
                "percent_change": -13.3,
                "effectiveness_label": "HIGHLY_EFFECTIVE",
            },
            read=True,
            created_at=_days_ago(3),
        ),
        # Insight Events (2)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000008"),
            event_type=EventType.INSIGHT,
            signal_id=signals[0].id,
            payload={
                "title": "Discharge Time Pattern Detected",
                "description": "62% of discharges occur after 3pm, extending LOS by average 0.8 days. Early discharge rounds recommended.",
            },
            read=False,
            created_at=_hours_ago(6),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000009"),
            event_type=EventType.INSIGHT,
            signal_id=signals[1].id,
            payload={
                "title": "Line Insertion Correlation",
                "description": "Higher CLABSI rates correlate with weekend line insertions. Consider standardizing insertion checklists.",
            },
            read=False,
            created_at=_hours_ago(12),
        ),
        # Technical Error Event (1)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000010"),
            event_type=EventType.TECHNICAL_ERROR,
            payload={
                "error_type": "integration_error",
                "title": "Data feed interruption",
                "description": "ADT feed temporarily unavailable. Some metrics may be delayed.",
                "resolution_status": "resolved",
                "affected_systems": ["ADT", "Census"],
            },
            read=True,
            created_at=_days_ago(6),
        ),
        # Assignment Events (4)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000011"),
            event_type=EventType.ASSIGNMENT,
            signal_id=signals[2].id,
            user_id=jennifer.id,
            payload={
                "assignee_name": "Dr. Sarah Chen",
                "assignee_role": "Medical Director, Cardiology",
                "notes": "High priority - review readmission patterns",
            },
            read=True,
            created_at=_days_ago(1),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000012"),
            event_type=EventType.ASSIGNMENT,
            signal_id=signals[3].id,
            user_id=jennifer.id,
            payload={
                "assignee_name": "Dr. Michael Johnson",
                "assignee_role": "Chief Nursing Officer",
                "notes": "Review fall prevention protocols",
            },
            read=True,
            created_at=_days_ago(4),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000013"),
            event_type=EventType.ASSIGNMENT,
            signal_id=signals[7].id,
            user_id=jennifer.id,
            payload={
                "assignee_name": "Dr. Sarah Chen",
                "assignee_role": "Medical Director, Cardiology",
                "previous_assignee_name": None,
            },
            read=True,
            created_at=_days_ago(7),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000014"),
            event_type=EventType.ASSIGNMENT,
            signal_id=signals[5].id,
            user_id=dr_chen.id,
            payload={
                "assignee_name": "Jennifer Martinez",
                "assignee_role": "Operations Director",
                "notes": "ED throughput initiative",
            },
            read=True,
            created_at=_days_ago(5),
        ),
        # Status Change Events (3)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000015"),
            event_type=EventType.STATUS_CHANGE,
            signal_id=signals[7].id,
            user_id=dr_chen.id,
            payload={
                "from_status": "in_progress",
                "to_status": "resolved",
                "notes": "Discharge huddles implemented",
                "resolution_notes": "Initial data shows 15% reduction in discharge delays",
            },
            read=True,
            created_at=_days_ago(1),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000016"),
            event_type=EventType.STATUS_CHANGE,
            signal_id=signals[9].id,
            user_id=dr_chen.id,
            payload={
                "from_status": "resolved",
                "to_status": "closed",
                "notes": "CLABSI rate normalized for 30 days",
            },
            read=True,
            created_at=_days_ago(3),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000017"),
            event_type=EventType.STATUS_CHANGE,
            signal_id=signals[5].id,
            user_id=jennifer.id,
            payload={
                "from_status": "assigned",
                "to_status": "in_progress",
                "notes": "Started ED throughput analysis",
            },
            read=True,
            created_at=_days_ago(4),
        ),
        # Comment Events (2)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000018"),
            event_type=EventType.COMMENT,
            signal_id=signals[0].id,
            user_id=dr_chen.id,
            payload={
                "author_name": "Dr. Sarah Chen",
                "content": "I've reviewed the root cause analysis. The discharge timing data is compelling - recommending we pilot early rounds.",
                "mentions": ["Dr.Johnson", "NursingTeam"],
            },
            read=False,
            created_at=_hours_ago(3),
        ),
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000019"),
            event_type=EventType.COMMENT,
            signal_id=signals[1].id,
            user_id=dr_johnson.id,
            payload={
                "author_name": "Dr. Michael Johnson",
                "content": "Nursing staff trained on updated line insertion checklist. Will monitor compliance over next 2 weeks.",
            },
            read=False,
            created_at=_hours_ago(8),
        ),
        # Intervention Activated Event (1)
        ActivityEvent(
            id=UUID("30000000-0000-0000-0000-000000000020"),
            event_type=EventType.INTERVENTION_ACTIVATED,
            signal_id=signals[0].id,
            user_id=dr_chen.id,
            payload={
                "intervention_name": "Automated Discharge Summary Agent",
                "intervention_type": "ai_agent",
                "expected_impact": {
                    "metric": "LOS",
                    "percent_change": -10,
                },
            },
            read=False,
            created_at=_hours_ago(1),
        ),
    ]


# =============================================================================
# Seed Function
# =============================================================================


async def seed_demo_data(session: AsyncSession, *, force: bool = False) -> dict[str, int]:
    """Seed the database with demo data.

    Creates users, signals, assignments, and activity events for testing
    and demonstration purposes. By default, skips seeding if data exists.

    Args:
        session: Async database session.
        force: If True, delete existing data before seeding. Defaults to False.

    Returns:
        dict[str, int]: Counts of created records, e.g.
            {"users": 3, "signals": 10, "assignments": 10, "events": 20}

    Raises:
        RuntimeError: If data exists and force=False.

    Example:
        >>> async with async_session() as session:
        ...     result = await seed_demo_data(session, force=True)
        ...     print(result)
        {'users': 3, 'signals': 10, 'assignments': 10, 'events': 20}

    Security:
        This function is intended for development/testing only and will refuse
        to run if DEBUG mode is disabled (production environment).
    """
    # Production guard - prevent accidental demo seeding in production
    if not settings.DEBUG:
        raise RuntimeError("Demo data seeding is only allowed in DEBUG mode. Set DEBUG=true in environment to enable demo seeding.")

    # Check for existing data
    existing_users = await session.execute(select(User).limit(1))
    if existing_users.scalar_one_or_none() is not None:
        if not force:
            raise RuntimeError("Database already contains data. Use force=True to overwrite.")
        # Delete existing data in correct order (respecting foreign keys)
        await session.execute(delete(ActivityEvent))
        await session.execute(delete(Assignment))
        await session.execute(delete(Signal))
        await session.execute(delete(User))
        await session.commit()

    # Create demo data
    users = create_demo_users()
    signals = create_demo_signals()
    assignments = create_demo_assignments(signals, users)
    events = create_demo_activity_events(signals, users)

    # Add to session
    session.add_all(users)
    session.add_all(signals)
    session.add_all(assignments)
    session.add_all(events)

    await session.commit()

    return {
        "users": len(users),
        "signals": len(signals),
        "assignments": len(assignments),
        "events": len(events),
    }


async def clear_demo_data(session: AsyncSession) -> dict[str, int]:
    """Clear all demo data from the database.

    Removes all records from users, signals, assignments, and activity_events
    tables in the correct order to respect foreign key constraints.

    Args:
        session: Async database session.

    Returns:
        dict[str, int]: Counts of deleted records per table.

    Example:
        >>> async with async_session() as session:
        ...     result = await clear_demo_data(session)
        ...     print(result)
        {'events': 20, 'assignments': 10, 'signals': 10, 'users': 3}
    """
    # Count before deleting
    events_count = (await session.execute(select(ActivityEvent))).scalars().all()
    assignments_count = (await session.execute(select(Assignment))).scalars().all()
    signals_count = (await session.execute(select(Signal))).scalars().all()
    users_count = (await session.execute(select(User))).scalars().all()

    # Delete in correct order
    await session.execute(delete(ActivityEvent))
    await session.execute(delete(Assignment))
    await session.execute(delete(Signal))
    await session.execute(delete(User))
    await session.commit()

    return {
        "events": len(events_count),
        "assignments": len(assignments_count),
        "signals": len(signals_count),
        "users": len(users_count),
    }


# =============================================================================
# CLI Entry Point
# =============================================================================


if __name__ == "__main__":
    import asyncio
    import sys

    from src.db.session import async_session_maker

    async def main() -> None:
        """Run the seed script from command line.

        Returns:
            None

        Raises:
            SystemExit: If seeding fails.
        """
        force = "--force" in sys.argv

        print("Seeding demo data...")
        async with async_session_maker() as session:
            try:
                result = await seed_demo_data(session, force=force)
                print(f"Created {result['users']} users")
                print(f"Created {result['signals']} signals")
                print(f"Created {result['assignments']} assignments")
                print(f"Created {result['events']} activity events")
                print("Done!")
            except RuntimeError as e:
                print(f"Error: {e}")
                print("Use --force to overwrite existing data")
                sys.exit(1)

    asyncio.run(main())
