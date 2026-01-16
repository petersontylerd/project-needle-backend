"""Narrative insights API router.

Provides endpoints for retrieving narrative contribution analysis data,
including executive summaries, Pareto analysis, and hierarchical breakdowns.
"""

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Path

from src.schemas.narrative import (
    ContributorSummaryResponse,
    NarrativeInsightsResponse,
    NarrativeListResponse,
    NarrativeSummaryResponse,
)
from src.services.narrative_service import (
    HierarchyNode,
    NarrativeInsights,
    NarrativeService,
    NarrativeServiceError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/narratives", tags=["narratives"])


def get_narrative_service() -> NarrativeService:
    """Dependency injection for NarrativeService.

    Returns:
        NarrativeService: Configured narrative service instance.

    Raises:
        None: Returns service or logs warning if unavailable.
    """
    return NarrativeService()


NarrativeServiceDep = Annotated[NarrativeService, Depends(get_narrative_service)]


@router.get(
    "",
    response_model=NarrativeListResponse,
    summary="List available narratives",
    description="Returns a list of facility IDs that have narrative analysis available.",
)
async def list_narratives(
    service: NarrativeServiceDep,
) -> NarrativeListResponse:
    """List all facilities with available narrative insights.

    Args:
        service: Injected narrative service.

    Returns:
        NarrativeListResponse: List of facility IDs and count.

    Raises:
        None: Returns empty list if no narratives available.
    """
    facilities = service.list_available_facilities()
    return NarrativeListResponse(
        facilities=facilities,
        count=len(facilities),
    )


@router.get(
    "/{facility_id}",
    response_model=NarrativeInsightsResponse,
    summary="Get full narrative insights",
    description="Returns complete narrative analysis for a facility including executive summary, Pareto analysis, top drivers, and hierarchical breakdown.",
)
async def get_narrative(
    facility_id: Annotated[
        str,
        Path(
            description="Medicare facility identifier (e.g., AFP658)",
            min_length=1,
            max_length=50,
        ),
    ],
    service: NarrativeServiceDep,
) -> NarrativeInsightsResponse:
    """Get full narrative insights for a facility.

    Args:
        facility_id: Medicare facility identifier.
        service: Injected narrative service.

    Returns:
        NarrativeInsightsResponse: Complete narrative analysis data.

    Raises:
        HTTPException: 404 if facility not found, 500 on parsing error.
    """
    try:
        insights = service.get_narrative(facility_id)
        if insights is None:
            raise HTTPException(
                status_code=404,
                detail=f"Narrative not found for facility: {facility_id}",
            )

        # Convert dataclass to response model
        return NarrativeInsightsResponse.model_validate(
            _insights_to_dict(insights),
        )

    except NarrativeServiceError as e:
        logger.error("Narrative parsing error for %s: %s", facility_id, e.message)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to parse narrative for facility {facility_id}: {e.message}",
        ) from e


@router.get(
    "/{facility_id}/summary",
    response_model=NarrativeSummaryResponse,
    summary="Get narrative summary",
    description="Returns lightweight executive summary for a facility (faster than full narrative).",
)
async def get_narrative_summary(
    facility_id: Annotated[
        str,
        Path(
            description="Medicare facility identifier (e.g., AFP658)",
            min_length=1,
            max_length=50,
        ),
    ],
    service: NarrativeServiceDep,
) -> NarrativeSummaryResponse:
    """Get executive summary for a facility.

    Args:
        facility_id: Medicare facility identifier.
        service: Injected narrative service.

    Returns:
        NarrativeSummaryResponse: Executive summary data.

    Raises:
        HTTPException: 404 if facility not found, 500 on parsing error.
    """
    try:
        insights = service.get_narrative(facility_id)
        if insights is None:
            raise HTTPException(
                status_code=404,
                detail=f"Narrative not found for facility: {facility_id}",
            )

        summary = insights.executive_summary
        return NarrativeSummaryResponse(
            facility_id=insights.facility_id,
            metric_value=insights.metric_value,
            pareto_insight=summary.pareto_insight,
            top_contributors_higher=[
                ContributorSummaryResponse(
                    dimension=c.dimension,
                    segment=c.segment,
                    excess=c.excess,
                    trend=c.trend,
                    flags=c.flags,
                )
                for c in summary.top_contributors_higher
            ],
            top_contributors_lower=[
                ContributorSummaryResponse(
                    dimension=c.dimension,
                    segment=c.segment,
                    excess=c.excess,
                    trend=c.trend,
                    flags=c.flags,
                )
                for c in summary.top_contributors_lower
            ],
        )

    except NarrativeServiceError as e:
        logger.error("Narrative parsing error for %s: %s", facility_id, e.message)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to parse narrative for facility {facility_id}: {e.message}",
        ) from e


def _insights_to_dict(insights: NarrativeInsights) -> dict[str, Any]:
    """Convert NarrativeInsights dataclass to dictionary for Pydantic validation.

    Args:
        insights: NarrativeInsights dataclass instance.

    Returns:
        dict: Dictionary representation of insights.

    Raises:
        None: Handles missing attributes gracefully.
    """
    exec_summary = insights.executive_summary
    pareto = insights.pareto_analysis
    drivers = insights.top_drivers
    insight_cats = insights.insights

    return {
        "facility_id": insights.facility_id,
        "metric_value": insights.metric_value,
        "generated_at": insights.generated_at,
        "executive_summary": {
            "metric_value": exec_summary.metric_value,
            "total_segments": exec_summary.total_segments,
            "facility_segments": exec_summary.facility_segments,
            "pareto_insight": exec_summary.pareto_insight,
            "top_contributors_higher": [
                {
                    "dimension": c.dimension,
                    "segment": c.segment,
                    "excess": c.excess,
                    "trend": c.trend,
                    "flags": c.flags,
                }
                for c in exec_summary.top_contributors_higher
            ],
            "top_contributors_lower": [
                {
                    "dimension": c.dimension,
                    "segment": c.segment,
                    "excess": c.excess,
                    "trend": c.trend,
                    "flags": c.flags,
                }
                for c in exec_summary.top_contributors_lower
            ],
        },
        "cross_metric_comparison": [
            {
                "metric_name": m.metric_name,
                "value": m.value,
                "z_score": m.z_score,
                "peer_status": m.peer_status,
            }
            for m in insights.cross_metric_comparison
        ],
        "pareto_analysis": {
            "positive_excess": [
                {
                    "segment_name": s.segment_name,
                    "excess": s.excess,
                    "cumulative_pct": s.cumulative_pct,
                }
                for s in pareto.positive_excess
            ],
            "negative_excess": [
                {
                    "segment_name": s.segment_name,
                    "excess": s.excess,
                    "cumulative_pct": s.cumulative_pct,
                }
                for s in pareto.negative_excess
            ],
            "top_n_positive_pct": pareto.top_n_positive_pct,
            "top_n_negative_pct": pareto.top_n_negative_pct,
        },
        "top_drivers": {
            "higher_los": [
                {
                    "rank": d.rank,
                    "dimension": d.dimension,
                    "segment": d.segment,
                    "value": d.value,
                    "weight": d.weight,
                    "excess": d.excess,
                    "trend": d.trend,
                    "z_score": d.z_score,
                    "peer_status": d.peer_status,
                    "interpretation": d.interpretation,
                }
                for d in drivers.higher_los
            ],
            "lower_los": [
                {
                    "rank": d.rank,
                    "dimension": d.dimension,
                    "segment": d.segment,
                    "value": d.value,
                    "weight": d.weight,
                    "excess": d.excess,
                    "trend": d.trend,
                    "z_score": d.z_score,
                    "peer_status": d.peer_status,
                    "interpretation": d.interpretation,
                }
                for d in drivers.lower_los
            ],
        },
        "insights": {
            "double_trouble": [
                {
                    "segment": i.segment,
                    "excess": i.excess,
                    "z_score": i.z_score,
                    "peer_status": i.peer_status,
                }
                for i in insight_cats.double_trouble
            ],
            "internal_issue": [
                {
                    "segment": i.segment,
                    "excess": i.excess,
                    "z_score": i.z_score,
                    "peer_status": i.peer_status,
                }
                for i in insight_cats.internal_issue
            ],
        },
        "hierarchical_breakdown": [_hierarchy_node_to_dict(node) for node in insights.hierarchical_breakdown],
    }


def _hierarchy_node_to_dict(node: HierarchyNode) -> dict[str, Any]:
    """Convert HierarchyNode to dictionary recursively.

    Args:
        node: HierarchyNode dataclass instance.

    Returns:
        dict: Dictionary representation of the node.

    Raises:
        None: Returns empty dict for invalid nodes.
    """
    return {
        "level": node.level,
        "segment": node.segment,
        "value": node.value,
        "weight": node.weight,
        "excess": node.excess,
        "z_score": node.z_score,
        "peer_status": node.peer_status,
        "interpretation": node.interpretation,
        "children": [_hierarchy_node_to_dict(c) for c in node.children],
    }
