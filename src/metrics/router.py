"""Metrics router - Semantic layer metrics endpoints.

Exposes semantic layer metrics via REST API.
Reads metric definitions from dbt semantic manifest and provides
query capabilities for metric exploration.

Backward compatibility:
- GET /metrics - Returns legacy hardcoded metrics
- GET /metrics/{metric_id} - Returns legacy metric info

New semantic layer endpoints:
- GET /metrics/definitions - Returns metrics from semantic manifest
- GET /metrics/definitions/{name} - Returns specific metric definition
- POST /metrics/query/{name} - Executes metric query with dimensions
- GET /metrics/semantic-models - Returns semantic model definitions
- GET /metrics/categories - Returns unique metric categories
"""

from __future__ import annotations

from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_async_db_session
from src.services.metric_query_service import (
    MetricQueryService,
    get_metric_query_service,
)
from src.services.semantic_manifest_service import (
    SemanticManifestService,
    get_semantic_manifest_service,
)

router = APIRouter(prefix="/metrics", tags=["metrics"])


# ============================================
# Legacy Response Models (backward compatibility)
# ============================================


class MetricInfo(BaseModel):
    """Semantic information about a quality metric (legacy)."""

    metric_id: str
    display_name: str
    description: str
    domain: str
    domain_description: str
    unit: str
    direction: str
    interpretation: str


class MetricListResponse(BaseModel):
    """Response containing all metric definitions (legacy)."""

    metrics: list[MetricInfo]
    count: int


# ============================================
# New Semantic Layer Response Models
# ============================================


class MetricDefinitionResponse(BaseModel):
    """Metric definition from semantic manifest."""

    name: str
    description: str
    label: str
    type: str
    category: str | None = None
    display_format: str | None = None
    available_dimensions: list[str] = Field(default_factory=list)


class MetricQueryRequest(BaseModel):
    """Request body for metric query."""

    group_by: list[str] = Field(default_factory=list)
    where: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    limit: int | None = Field(None, ge=1, le=10000)


class MetricQueryResponse(BaseModel):
    """Response from a metric query."""

    metric_name: str
    dimensions: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    sql: str | None = None


class SemanticModelResponse(BaseModel):
    """Semantic model definition response."""

    name: str
    description: str
    table_name: str
    dimensions: list[str]
    measures: list[str]


# ============================================
# Legacy Hardcoded Metrics (backward compatibility)
# ============================================


METRIC_DEFINITIONS: list[MetricInfo] = [
    MetricInfo(
        metric_id="losIndex",
        display_name="LOS Index",
        description="Length of stay adjusted for case complexity. Compares actual days to expected days based on patient acuity.",
        domain="Efficiency",
        domain_description="Metrics measuring operational efficiency and resource utilization",
        unit="ratio",
        direction="lower_is_better",
        interpretation="A value above 1.0 indicates patients are staying longer than expected for their condition complexity. Values below 1.0 suggest efficient throughput.",
    ),
    MetricInfo(
        metric_id="averageLos",
        display_name="Average LOS",
        description="Mean length of stay in days across all patient encounters. Unadjusted for case mix.",
        domain="Efficiency",
        domain_description="Metrics measuring operational efficiency and resource utilization",
        unit="days",
        direction="lower_is_better",
        interpretation="Raw average days patients spend in the facility. Should be interpreted alongside case mix index.",
    ),
    MetricInfo(
        metric_id="throughput",
        display_name="Throughput",
        description="Patient volume processed through the service line. Measures operational capacity utilization.",
        domain="Efficiency",
        domain_description="Metrics measuring operational efficiency and resource utilization",
        unit="count",
        direction="higher_is_better",
        interpretation="Higher values indicate greater patient volume. Should be balanced against quality metrics.",
    ),
    MetricInfo(
        metric_id="clabsiRate",
        display_name="CLABSI Rate",
        description="Central Line-Associated Bloodstream Infection rate per 1,000 central line days. A key hospital-acquired infection metric.",
        domain="Safety",
        domain_description="Metrics measuring patient safety and harm prevention",
        unit="per_1000_days",
        direction="lower_is_better",
        interpretation="Any CLABSI is considered preventable. Rates above zero warrant investigation of line insertion and maintenance practices.",
    ),
    MetricInfo(
        metric_id="vaeRate",
        display_name="VAE Rate",
        description="Ventilator-Associated Event rate per 1,000 ventilator days. Measures complications in mechanically ventilated patients.",
        domain="Safety",
        domain_description="Metrics measuring patient safety and harm prevention",
        unit="per_1000_days",
        direction="lower_is_better",
        interpretation="Lower rates indicate better ventilator bundle compliance and respiratory care quality.",
    ),
    MetricInfo(
        metric_id="fallRate",
        display_name="Fall Rate",
        description="Patient falls per 1,000 patient days. Includes both with and without injury.",
        domain="Safety",
        domain_description="Metrics measuring patient safety and harm prevention",
        unit="per_1000_days",
        direction="lower_is_better",
        interpretation="Falls are largely preventable. High rates may indicate staffing, environment, or assessment gaps.",
    ),
    MetricInfo(
        metric_id="readmissionRate",
        display_name="Readmission Rate",
        description="30-day all-cause readmission rate. Percentage of patients who return within 30 days of discharge.",
        domain="Effectiveness",
        domain_description="Metrics measuring clinical outcomes and care quality",
        unit="percentage",
        direction="lower_is_better",
        interpretation="High rates may indicate premature discharge, inadequate care coordination, or poor discharge planning.",
    ),
    MetricInfo(
        metric_id="mortalityRate",
        display_name="Mortality Rate",
        description="In-hospital mortality rate adjusted for expected mortality based on patient risk factors.",
        domain="Effectiveness",
        domain_description="Metrics measuring clinical outcomes and care quality",
        unit="ratio",
        direction="lower_is_better",
        interpretation="Observed-to-expected ratio. Values above 1.0 indicate higher than expected mortality for the case mix.",
    ),
]

METRIC_LOOKUP: dict[str, MetricInfo] = {m.metric_id: m for m in METRIC_DEFINITIONS}


# ============================================
# Semantic Layer Endpoints (new)
# ============================================


@router.get("/definitions", response_model=list[MetricDefinitionResponse])
async def list_metric_definitions(
    manifest: Annotated[SemanticManifestService, Depends(get_semantic_manifest_service)],
    category: str | None = Query(None, description="Filter by category"),
) -> list[MetricDefinitionResponse]:
    """List all metric definitions from semantic manifest.

    Returns metrics defined in the dbt semantic layer with their
    descriptions, types, and available dimensions.

    Args:
        manifest: Semantic manifest service (injected).
        category: Optional category filter.

    Returns:
        List of metric definitions.
    """
    try:
        metrics = manifest.get_metrics_by_category(category) if category else manifest.get_all_metrics()

        return [
            MetricDefinitionResponse(
                name=m.name,
                description=m.description,
                label=m.label,
                type=m.type,
                category=m.category,
                display_format=m.display_format,
                available_dimensions=manifest.get_available_dimensions(m.name),
            )
            for m in metrics
        ]
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e)) from None


@router.get("/definitions/{metric_name}", response_model=MetricDefinitionResponse)
async def get_metric_definition(
    metric_name: str,
    manifest: Annotated[SemanticManifestService, Depends(get_semantic_manifest_service)],
) -> MetricDefinitionResponse:
    """Get a specific metric definition from semantic manifest.

    Args:
        metric_name: Name of the metric.
        manifest: Semantic manifest service (injected).

    Returns:
        Metric definition with available dimensions.

    Raises:
        HTTPException: 404 if metric not found.
    """
    try:
        metric = manifest.get_metric(metric_name)
        if not metric:
            raise HTTPException(404, f"Metric '{metric_name}' not found")

        return MetricDefinitionResponse(
            name=metric.name,
            description=metric.description,
            label=metric.label,
            type=metric.type,
            category=metric.category,
            display_format=metric.display_format,
            available_dimensions=manifest.get_available_dimensions(metric_name),
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e)) from None


@router.post("/query/{metric_name}", response_model=MetricQueryResponse)
async def query_metric(
    metric_name: str,
    request: MetricQueryRequest,
    db: Annotated[AsyncSession, Depends(get_async_db_session)],
    query_service: Annotated[MetricQueryService, Depends(get_metric_query_service)],
) -> MetricQueryResponse:
    """Execute a metric query with optional dimensions and filters.

    Generates and executes a SQL query based on the metric's semantic
    definition. Supports grouping by dimensions and filtering by
    date range.

    Args:
        metric_name: Name of the metric to query.
        request: Query parameters including group_by, filters, and limits.
        db: Database session (injected).
        query_service: Metric query service (injected).

    Returns:
        Query results with rows and metadata.

    Raises:
        HTTPException: 400 if query generation fails, 404 if metric not found.
    """
    try:
        time_range = None
        if request.start_date and request.end_date:
            time_range = (request.start_date, request.end_date)

        result = await query_service.query_metric(
            db=db,
            metric_name=metric_name,
            group_by=request.group_by or None,
            where=request.where,
            time_range=time_range,
            limit=request.limit,
        )

        return MetricQueryResponse(
            metric_name=result.metric_name,
            dimensions=result.dimensions,
            rows=result.rows,
            row_count=result.row_count,
            sql=result.sql,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e)) from None


@router.get("/semantic-models", response_model=list[SemanticModelResponse])
async def list_semantic_models(
    manifest: Annotated[SemanticManifestService, Depends(get_semantic_manifest_service)],
) -> list[SemanticModelResponse]:
    """List all semantic model definitions.

    Returns the semantic models defined in dbt, including their
    dimensions and measures.

    Returns:
        List of semantic model definitions.
    """
    try:
        models = manifest.get_all_semantic_models()
        return [
            SemanticModelResponse(
                name=sm.name,
                description=sm.description,
                table_name=sm.table_name,
                dimensions=[d.name for d in sm.dimensions],
                measures=[m.name for m in sm.measures],
            )
            for sm in models
        ]
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e)) from None


@router.get("/categories", response_model=list[str])
async def list_categories(
    manifest: Annotated[SemanticManifestService, Depends(get_semantic_manifest_service)],
) -> list[str]:
    """List all metric categories.

    Returns unique category names from metric definitions.

    Returns:
        Sorted list of category names.
    """
    try:
        return manifest.get_all_categories()
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e)) from None


# ============================================
# Legacy Endpoints (backward compatibility)
# ============================================


@router.get("", response_model=MetricListResponse)
async def get_all_metrics() -> MetricListResponse:
    """Get all metric definitions with semantic descriptions (legacy).

    Returns plain-language descriptions, domain context, and interpretation
    guidance for all quality metrics tracked in the system.

    Note: This is a legacy endpoint. Use GET /metrics/definitions for
    semantic layer metrics.

    Returns:
        MetricListResponse: List of all metric definitions.
    """
    return MetricListResponse(metrics=METRIC_DEFINITIONS, count=len(METRIC_DEFINITIONS))


@router.get("/{metric_id}", response_model=MetricInfo)
async def get_metric(metric_id: str) -> MetricInfo:
    """Get semantic information for a specific metric (legacy).

    Args:
        metric_id: Metric identifier (e.g., 'losIndex', 'clabsiRate').

    Note: This is a legacy endpoint. Use GET /metrics/definitions/{name}
    for semantic layer metrics.

    Returns:
        MetricInfo: Metric definition with description and context.

    Raises:
        HTTPException: 404 if metric_id not found.
    """
    if metric_id not in METRIC_LOOKUP:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")
    return METRIC_LOOKUP[metric_id]
