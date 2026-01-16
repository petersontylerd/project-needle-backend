"""Signals API router - List and detail endpoints for quality signals.

This module provides REST endpoints for retrieving signals parsed from
Project Needle node results.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import ColumnElement, func, select, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.db.models import (
    Assignment,
    AssignmentStatus,
    Signal,
    SignalDomain,
)
from src.db.session import get_async_db_session
from src.schemas.contribution import HierarchicalContributionsResponse
from src.schemas.signal import (
    FilterOptionsResponse,
    PercentileTrendsSchema,
    SignalChildrenResponse,
    SignalListResponse,
    SignalParentResponse,
    SignalResponse,
    SignalTechnicalDetails,
    SignalTemporalResponse,
    SignalUpdate,
)
from src.services.contribution_service import ContributionService, ContributionServiceError
from src.services.signal_hydrator import SignalHydrator

router = APIRouter(prefix="/signals", tags=["signals"])


# =============================================================================
# Type Aliases for Dependencies
# =============================================================================

# Database session dependency
DbSession = Annotated[AsyncSession, Depends(get_async_db_session)]

# Query parameter annotations
DomainFilter = Annotated[SignalDomain | None, Query(description="Filter by quality domain")]
FacilityFilter = Annotated[str | None, Query(description="Filter by facility name(s) - comma-separated for multiple")]
SystemNameFilter = Annotated[str | None, Query(description="Filter by health system name")]
ServiceLineFilter = Annotated[str | None, Query(description="Filter by service line")]
StatusFilter = Annotated[AssignmentStatus | None, Query(description="Filter by assignment status")]
SignalTypeFilter = Annotated[str | None, Query(description="Filter by simplified signal type (9 types)")]
LimitQuery = Annotated[int, Query(ge=1, le=100, description="Maximum results")]
OffsetQuery = Annotated[int, Query(ge=0, description="Results offset")]
TopNQuery = Annotated[int, Query(ge=1, le=50, description="Number of top contributors to return")]

# Sort options for signal list
SortByField = Literal["detected_at", "priority", "metric_id"]
SortOrder = Literal["asc", "desc"]
SortByQuery = Annotated[SortByField, Query(description="Field to sort by (detected_at, priority, metric_id)")]
SortOrderQuery = Annotated[SortOrder, Query(description="Sort order (asc or desc)")]


# =============================================================================
# Response Helpers
# =============================================================================


def _signal_to_response(
    signal: Signal,
    *,
    has_children: bool = False,
    has_parent: bool = False,
    edge_types: list[str] | None = None,
    related_signal_count: int = 0,
) -> SignalResponse:
    """Convert a Signal model to SignalResponse schema.

    Args:
        signal: Signal ORM model instance.
        has_children: Whether signal has child signals via drills_to edges.
        has_parent: Whether signal has a parent signal via drills_to edges.
        edge_types: Available edge types from this signal.
        related_signal_count: Count of related signals via relates_to edges.

    Returns:
        SignalResponse: Pydantic response model with signal data and navigation fields.
    """
    # Calculate days open
    days_open = (datetime.now(tz=UTC) - signal.detected_at).days if signal.detected_at else 0

    # Convert metric_trend_timeline from model to Pydantic schema if present
    from src.schemas.signal import MetricTrendPeriod

    metric_trend_timeline = None
    if signal.metric_trend_timeline:
        metric_trend_timeline = [
            MetricTrendPeriod(
                period=str(p.get("period", "")),
                value=cast(float | None, p.get("value")),
                encounters=cast(int | None, p.get("encounters")),
            )
            for p in signal.metric_trend_timeline
        ]

    # Convert peer_percentile_trends from model to Pydantic schema if present
    peer_percentile_trends = None
    if signal.peer_percentile_trends:
        peer_percentile_trends = PercentileTrendsSchema(**signal.peer_percentile_trends)

    # Get workflow status from assignment relationship
    workflow_status = signal.assignment.status.value if signal.assignment else "new"

    return SignalResponse(
        id=str(signal.id),
        canonical_node_id=signal.canonical_node_id,
        metric_id=signal.metric_id,
        domain=signal.domain,
        facility=signal.facility,
        facility_id=signal.facility_id,
        system_name=signal.system_name,
        service_line=signal.service_line,
        sub_service_line=signal.sub_service_line,
        description=signal.description,
        metric_value=signal.metric_value,
        peer_mean=signal.peer_mean,
        peer_std=signal.peer_std,
        percentile_rank=signal.percentile_rank,
        encounters=signal.encounters,
        detected_at=signal.detected_at,
        created_at=signal.created_at,
        days_open=days_open,
        simplified_signal_type=signal.simplified_signal_type,
        simplified_severity=signal.simplified_severity,
        temporal_node_id=signal.temporal_node_id,
        has_children=has_children,
        has_parent=has_parent,
        edge_types=edge_types or [],
        related_signal_count=related_signal_count,
        # Entity identification and grouping
        entity_dimensions=signal.entity_dimensions,
        groupby_label=signal.groupby_label,
        group_value=signal.group_value,
        # Metric trend timeline for sparkline
        metric_trend_timeline=metric_trend_timeline,
        # Trend direction (kept in signals table)
        trend_direction=signal.trend_direction,
        # Workflow status from assignment
        workflow_status=workflow_status,
        # Metadata
        metadata=signal.metadata_,
        metadata_per_period=signal.metadata_per_period,
        # Peer percentile trends (for reference band visualization)
        peer_percentile_trends=peer_percentile_trends,
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.get("", response_model=SignalListResponse)
async def list_signals(
    session: DbSession,
    domain: DomainFilter = None,
    facility: FacilityFilter = None,
    system_name: SystemNameFilter = None,
    service_line: ServiceLineFilter = None,
    status: StatusFilter = None,
    signal_type: SignalTypeFilter = None,
    sort_by: SortByQuery = "detected_at",
    sort_order: SortOrderQuery = "desc",
    limit: LimitQuery = 25,
    offset: OffsetQuery = 0,
) -> SignalListResponse:
    """List signals with optional filters, sorting, and pagination.

    Retrieves signals from the database with support for filtering by
    domain, facility, system name, service line, assignment status, and signal type.
    Returns paginated results with total count for pagination UI.

    Args:
        session: Database session (injected).
        domain: Filter by quality domain.
        facility: Filter by facility name.
        system_name: Filter by health system name.
        service_line: Filter by service line.
        status: Filter by assignment status.
        signal_type: Filter by simplified signal type (9 types).
        sort_by: Field to sort by (detected_at, priority, metric_id).
        sort_order: Sort order (asc or desc).
        limit: Maximum number of results per page (default 25).
        offset: Number of results to skip.

    Returns:
        SignalListResponse: Paginated list of signals with total count.

    Example:
        >>> GET /api/signals?domain=Efficiency&limit=25&offset=0
        >>> GET /api/signals?system_name=ALPHA_HEALTH
        >>> GET /api/signals?signal_type=critical_trajectory
    """
    # Build base query with filters (without pagination)
    base_query = select(Signal)

    # Apply filters
    if domain:
        base_query = base_query.where(Signal.domain == domain)
    if facility:
        # Support comma-separated list of facilities for multi-select filter
        facility_list = [f.strip() for f in facility.split(",") if f.strip()]
        if len(facility_list) == 1:
            base_query = base_query.where(Signal.facility == facility_list[0])
        elif len(facility_list) > 1:
            base_query = base_query.where(Signal.facility.in_(facility_list))
    if system_name:
        base_query = base_query.where(Signal.system_name == system_name)
    if service_line:
        base_query = base_query.where(Signal.service_line == service_line)
    if status:
        base_query = base_query.join(Signal.assignment).where(Assignment.status == status)
    if signal_type:
        base_query = base_query.where(Signal.simplified_signal_type == signal_type)

    # Get total count (before pagination)
    count_query = select(func.count()).select_from(base_query.subquery())
    count_result = await session.execute(count_query)
    total_count = count_result.scalar() or 0

    # Build data query with sorting and pagination
    data_query = base_query.options(joinedload(Signal.assignment))

    # Apply sorting
    sort_column_map = {
        "detected_at": Signal.detected_at,
        "priority": Signal.simplified_severity,
        "metric_id": Signal.metric_id,
    }
    sort_column = sort_column_map.get(sort_by, Signal.detected_at)
    order_expr = sort_column.desc() if sort_order == "desc" else sort_column.asc()
    data_query = data_query.order_by(order_expr)

    # Apply pagination
    data_query = data_query.offset(offset).limit(limit)

    result = await session.execute(data_query)
    signals = result.scalars().unique().all()

    return SignalListResponse(
        total_count=total_count,
        offset=offset,
        limit=limit,
        signals=[_signal_to_response(signal) for signal in signals],
    )


@router.get("/filter-options", response_model=FilterOptionsResponse)
async def get_filter_options(
    session: DbSession,
    system_name: SystemNameFilter = None,
    facility: FacilityFilter = None,
) -> FilterOptionsResponse:
    """Return distinct values for column filter dropdowns.

    Retrieves all unique values for each filterable column, optionally
    scoped to a specific system and/or facility. Used to populate column filter menus
    with complete options regardless of current pagination.

    Args:
        session: Database session (injected).
        system_name: Optional system name filter to scope results.
        facility: Optional facility filter to scope results.

    Returns:
        FilterOptionsResponse: Distinct values for each filterable column.

    Example:
        >>> GET /api/signals/filter-options
        >>> GET /api/signals/filter-options?system_name=MERIDIAN_HEALTH
        >>> GET /api/signals/filter-options?facility=010033
    """
    # Build base query with optional system and facility filters
    base_filter: ColumnElement[bool] = true()
    if system_name:
        base_filter = base_filter & (Signal.system_name == system_name)
    if facility:
        # Support comma-separated list of facilities for multi-select filter
        facility_list = [f.strip() for f in facility.split(",") if f.strip()]
        if len(facility_list) == 1:
            base_filter = base_filter & (Signal.facility == facility_list[0])
        elif len(facility_list) > 1:
            base_filter = base_filter & (Signal.facility.in_(facility_list))

    # Query distinct values for each filterable column
    metric_result = await session.execute(select(Signal.metric_id).where(base_filter).where(Signal.metric_id.isnot(None)).distinct().order_by(Signal.metric_id))
    metric_ids = [row[0] for row in metric_result.fetchall()]

    domain_result = await session.execute(select(Signal.domain).where(base_filter).where(Signal.domain.isnot(None)).distinct().order_by(Signal.domain))
    domains = [str(row[0].value) if hasattr(row[0], "value") else str(row[0]) for row in domain_result.fetchall()]

    signal_type_result = await session.execute(
        select(Signal.simplified_signal_type)
        .where(base_filter)
        .where(Signal.simplified_signal_type.isnot(None))
        .distinct()
        .order_by(Signal.simplified_signal_type)
    )
    signal_types = [row[0] for row in signal_type_result.fetchall()]

    system_name_result = await session.execute(
        select(Signal.system_name).where(base_filter).where(Signal.system_name.isnot(None)).distinct().order_by(Signal.system_name)
    )
    system_names = [row[0] for row in system_name_result.fetchall()]

    service_line_result = await session.execute(
        select(Signal.service_line).where(base_filter).where(Signal.service_line.isnot(None)).distinct().order_by(Signal.service_line)
    )
    service_lines = [row[0] for row in service_line_result.fetchall()]

    return FilterOptionsResponse(
        metric_id=metric_ids,
        domain=domains,
        simplified_signal_type=signal_types,
        system_name=system_names,
        service_line=service_lines,
    )


@router.get("/facilities", response_model=list[str])
async def list_facilities(
    session: DbSession,
    system_name: SystemNameFilter = None,
) -> list[str]:
    """Return distinct facility names, optionally filtered by system.

    Retrieves unique facility names from the signals table,
    optionally filtered by health system name.

    Args:
        session: Database session (injected).
        system_name: Optional health system name filter.

    Returns:
        list[str]: Sorted list of distinct facility names.

    Example:
        >>> GET /api/signals/facilities
        ["AFP658", "KYR088", "UMC001"]
        >>> GET /api/signals/facilities?system_name=BANNER_SYSTEM
        ["AFP658", "KYR088"]
    """
    query = select(Signal.facility).distinct().order_by(Signal.facility)
    if system_name:
        query = query.where(Signal.system_name == system_name)
    result = await session.execute(query)
    return [row[0] for row in result.fetchall()]


@router.get("/{signal_id}", response_model=SignalResponse)
async def get_signal(
    signal_id: UUID,
    session: DbSession,
) -> SignalResponse:
    """Get a single signal by ID.

    Retrieves full signal details including benchmark data and navigation
    information (has_children, has_parent, edge_types, related_signal_count).

    Args:
        signal_id: UUID of the signal to retrieve.
        session: Database session (injected).

    Returns:
        SignalResponse: Signal details with navigation fields populated.

    Raises:
        HTTPException: 404 if signal not found.

    Example:
        >>> GET /api/signals/550e8400-e29b-41d4-a716-446655440000
    """
    query = select(Signal).options(joinedload(Signal.assignment)).where(Signal.id == signal_id)
    result = await session.execute(query)
    signal = result.scalars().first()

    if not signal:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    # Navigation info extraction removed - node_result_path column dropped
    # Navigation is now handled via canonical_node_id lookups in database

    return _signal_to_response(signal)


@router.patch("/{signal_id}", response_model=SignalResponse)
async def patch_signal(
    signal_id: UUID,
    update: SignalUpdate,
    session: DbSession,
) -> SignalResponse:
    """Partially update a signal.

    Updates only the fields provided in the request body.
    Currently supports: description, why_matters_narrative.

    Args:
        signal_id: UUID of the signal to update.
        update: Partial update payload with optional fields.
        session: Database session (injected).

    Returns:
        SignalResponse: Updated signal with all fields populated.

    Raises:
        HTTPException: 404 if signal not found.

    Example:
        >>> PATCH /api/signals/550e8400-e29b-41d4-a716-446655440000
        >>> {"description": "Updated description", "why_matters_narrative": "New narrative"}
    """
    signal = await session.get(Signal, signal_id)
    if signal is None:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    # Apply only non-None fields from update (exclude_unset for true partial updates)
    update_data = update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(signal, field, value)

    await session.commit()
    await session.refresh(signal)
    return _signal_to_response(signal)


@router.get("/{signal_id}/temporal", response_model=SignalTemporalResponse)
async def get_signal_temporal(
    signal_id: UUID,
    session: DbSession,
) -> SignalTemporalResponse:
    """Get temporal trend data for a signal.

    Retrieves temporal classification, trend metrics, and monthly z-scores
    for a signal. Returns has_temporal_data=False if the signal has no
    linked temporal node.

    Args:
        signal_id: UUID of the signal to retrieve temporal data for.
        session: Database session (injected).

    Returns:
        SignalTemporalResponse: Temporal data including classification,
            trend slope percentile, and monthly z-scores.

    Raises:
        HTTPException: 404 if signal not found.

    Example:
        >>> GET /api/signals/550e8400-e29b-41d4-a716-446655440000/temporal
        {
            "signal_id": "550e8400-e29b-41d4-a716-446655440000",
            "temporal_node_id": "losIndex__medicareId__dischargeMonth",
            "simplified_signal_type": "chronic_underperformer",
            "slope_percentile": 25.5,
            "monthly_z_scores": [-0.2, -0.4, -0.5, ...],
            "has_temporal_data": true
        }

    Note:
        slope_percentile and monthly_z_scores are fetched on-demand from
        fct_signals rather than stored in the signals table.
    """
    signal = await session.get(Signal, signal_id)
    if signal is None:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    # Check if signal has temporal data
    has_temporal_data = signal.temporal_node_id is not None

    # Fetch temporal details from fct_signals (slope_percentile, monthly_z_scores removed from signals table)
    slope_percentile = None
    monthly_z_scores = None
    if has_temporal_data:
        hydrator = SignalHydrator()
        details = await hydrator.get_technical_details(
            signal.canonical_node_id,
            entity_dimensions_hash=signal.entity_dimensions_hash,
        )
        if details:
            slope_percentile = details.get("slope_percentile")
            monthly_z_scores = details.get("monthly_z_scores")

    return SignalTemporalResponse(
        signal_id=str(signal.id),
        temporal_node_id=signal.temporal_node_id,
        slope_percentile=slope_percentile,
        monthly_z_scores=monthly_z_scores,
        monthly_values=None,  # Would require loading temporal node file
        has_temporal_data=has_temporal_data,
    )


@router.get("/{signal_id}/contributions", response_model=HierarchicalContributionsResponse)
async def get_signal_contributions(
    signal_id: UUID,
    session: DbSession,
    top_n: TopNQuery = 10,
) -> HierarchicalContributionsResponse:
    """Get hierarchical contribution breakdown for a signal.

    Retrieves both upward (how this signal contributes to its parent) and
    downward (how children contribute to this signal) contribution data
    for proper drill-down analysis in the Quality Compass dashboard.

    The response includes:
    - upward_contribution: How THIS signal contributes to its parent aggregate
      (e.g., how Cardiology contributes to Facility-wide). None for facility-wide.
    - downward_contributions: How child entities contribute to THIS signal
      (e.g., how Sub-Service-Lines contribute to Cardiology).

    Args:
        signal_id: UUID of the signal.
        session: Database session (injected).
        top_n: Number of top downward contributors to return (default 10).

    Returns:
        HierarchicalContributionsResponse: Both upward and downward contributions.

    Raises:
        HTTPException: 404 if signal not found or has no canonical_node_id.
        HTTPException: 500 if contribution query fails.

    Example:
        >>> GET /api/signals/550e8400-e29b-41d4-a716-446655440000/contributions?top_n=5
    """
    # Get signal
    query = select(Signal).where(Signal.id == signal_id)
    result = await session.execute(query)
    signal = result.scalars().first()

    if not signal:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    # Check for canonical_node_id (required for dbt contribution query)
    if not signal.canonical_node_id:
        raise HTTPException(
            status_code=404,
            detail=f"No contribution data available for signal: {signal_id}",
        )

    # Check for facility_id (required to filter contributions to this signal's facility)
    if not signal.facility_id:
        raise HTTPException(
            status_code=404,
            detail=f"No facility_id available for signal: {signal_id}",
        )

    # Query hierarchical contributions (both upward and downward)
    service = ContributionService()
    try:
        upward, downward, hierarchy_level = await service.get_hierarchical_contributions(
            signal=signal,
            top_n=top_n,
        )
    except ContributionServiceError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to query contribution data: {e.message}",
        ) from e

    # Convert to response
    return HierarchicalContributionsResponse(
        upward_contribution=service.to_response(upward) if upward else None,
        downward_contributions=[service.to_response(r) for r in downward],
        signal_hierarchy_level=hierarchy_level,
        has_children=len(downward) > 0,
        has_parent=upward is not None,
    )


@router.get("/{signal_id}/children", response_model=SignalChildrenResponse)
async def get_signal_children(
    signal_id: UUID,
    session: DbSession,
    limit: LimitQuery = 50,
) -> SignalChildrenResponse:
    """Get child signals reachable via drills_to edges.

    Retrieves signals that are hierarchically beneath the specified signal,
    following the drills_to edge type from the node result graph. This enables
    drill-down navigation from aggregate signals to more granular views.

    Args:
        signal_id: UUID of the parent signal.
        session: Database session (injected).
        limit: Maximum number of child signals to return (default 50).

    Returns:
        SignalChildrenResponse: Child signals with edge type and counts.

    Raises:
        HTTPException: 404 if signal not found or has no node result data.

    Example:
        >>> GET /api/signals/550e8400-e29b-41d4-a716-446655440000/children
        {
            "signal_id": "550e8400-e29b-41d4-a716-446655440000",
            "canonical_node_id": "losIndex__medicareId",
            "edge_type": "drills_to",
            "children": [...],
            "total_count": 15,
            "has_children": true
        }
    """
    # Get parent signal
    query = select(Signal).where(Signal.id == signal_id)
    result = await session.execute(query)
    signal = result.scalars().first()

    if not signal:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    # node_result_path column has been dropped - navigation via file lookup removed
    # Return empty children response; navigation now uses canonical_node_id database lookups
    return SignalChildrenResponse(
        signal_id=str(signal_id),
        canonical_node_id=signal.canonical_node_id,
        edge_type="drills_to",
        children=[],
        total_count=0,
        has_children=False,
    )


@router.get("/{signal_id}/related", response_model=list[SignalResponse])
async def get_related_signals(
    signal_id: UUID,
    session: DbSession,
) -> list[SignalResponse]:
    """Get signals related via same facility and service line.

    Returns companion signals that share the same facility and service line
    but track different metrics. Useful for understanding cross-metric
    correlations.

    Args:
        signal_id: UUID of the signal to find related signals for.
        session: Database session (injected).

    Returns:
        list[SignalResponse]: Related signals with same facility/service_line.

    Raises:
        HTTPException: 404 if signal not found.

    Example:
        >>> GET /api/signals/550e8400-e29b-41d4-a716-446655440000/related
        [
            {"id": "...", "metric_id": "readmissionRate", ...},
            {"id": "...", "metric_id": "mortalityIndex", ...}
        ]
    """
    # First verify the signal exists
    signal_query = select(Signal).where(Signal.id == signal_id)
    result = await session.execute(signal_query)
    signal = result.scalar_one_or_none()

    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    # Find related signals: same facility + service_line, different metric
    related_query = (
        select(Signal)
        .where(Signal.facility == signal.facility)
        .where(Signal.service_line == signal.service_line)
        .where(Signal.metric_id != signal.metric_id)
        .where(Signal.id != signal_id)
        .order_by(Signal.simplified_severity.desc().nulls_last())
        .limit(10)
    )

    related_result = await session.execute(related_query)
    related_signals = related_result.scalars().all()

    return [_signal_to_response(s) for s in related_signals]


@router.get("/{signal_id}/parent", response_model=SignalParentResponse)
async def get_signal_parent(
    signal_id: UUID,
    session: DbSession,
) -> SignalParentResponse:
    """Get the parent signal reachable via drills_to edge.

    Retrieves the signal that is hierarchically above the specified signal,
    following the reverse drills_to edge from the node result graph. This enables
    drill-up navigation from granular signals to more aggregate views.

    Args:
        signal_id: UUID of the child signal.
        session: Database session (injected).

    Returns:
        SignalParentResponse: Parent signal with edge type, or null parent for root signals.

    Raises:
        HTTPException: 404 if signal not found.

    Example:
        >>> GET /api/signals/550e8400-e29b-41d4-a716-446655440000/parent
        {
            "signal_id": "550e8400-e29b-41d4-a716-446655440000",
            "canonical_node_id": "losIndex__facilityId__serviceLineId",
            "edge_type": "drills_to",
            "parent": {...},
            "has_parent": true
        }
    """
    # Get child signal
    query = select(Signal).where(Signal.id == signal_id)
    result = await session.execute(query)
    signal = result.scalars().first()

    if not signal:
        raise HTTPException(status_code=404, detail=f"Signal not found: {signal_id}")

    # node_result_path column has been dropped - navigation via file lookup removed
    # Return empty parent response; navigation now uses canonical_node_id database lookups
    return SignalParentResponse(
        signal_id=str(signal_id),
        canonical_node_id=signal.canonical_node_id,
        edge_type="drills_to",
        parent=None,
        has_parent=False,
    )


@router.get(
    "/{signal_id}/technical-details",
    response_model=SignalTechnicalDetails,
    summary="Get signal technical details",
    description="Fetch technical z-scores, anomaly labels, and classification tiers for a signal.",
)
async def get_signal_technical_details(
    signal_id: UUID,
    session: DbSession,
) -> SignalTechnicalDetails:
    """Get technical details for a specific signal.

    These details are fetched on-demand for drill-down views,
    not included in list responses to reduce payload size.

    Args:
        signal_id: UUID of the signal to retrieve technical details for.
        session: Database session (injected).

    Returns:
        SignalTechnicalDetails: Technical z-scores, anomaly labels, and classification tiers.

    Raises:
        HTTPException: 404 if signal not found or technical details not available.

    Example:
        >>> GET /api/signals/550e8400-e29b-41d4-a716-446655440000/technical-details
        {
            "simple_zscore": 1.25,
            "robust_zscore": 1.18,
            "simple_zscore_anomaly": "slightly_high",
            "magnitude_tier": "ELEVATED",
            ...
        }
    """
    # First get the signal to find its canonical_node_id
    signal = await session.get(Signal, signal_id)
    if signal is None:
        raise HTTPException(status_code=404, detail="Signal not found")

    # Fetch technical details from fct_signals
    hydrator = SignalHydrator()
    details = await hydrator.get_technical_details(
        signal.canonical_node_id,
        entity_dimensions_hash=signal.entity_dimensions_hash,
    )

    if details is None:
        raise HTTPException(
            status_code=404,
            detail="Technical details not found in fct_signals",
        )

    return SignalTechnicalDetails(**details)
