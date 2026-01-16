"""Runs API router - Insight graph run exploration endpoints.

This module provides REST endpoints for browsing insight graph runs,
parsing DOT files for visualization, and streaming entity results.
"""

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from src.config import settings
from src.runs.schemas import (
    EntityResultResponse,
    GraphListResponse,
    GraphStructureResponse,
    GraphSummary,
    NodeResultsResponse,
    RunListResponse,
    RunMetadataResponse,
    RunSummaryResponse,
    VisEdgeResponse,
    VisNodeResponse,
)
from src.runs.services.dot_parser import DotParserService
from src.runs.services.results_reader import ResultsReaderService
from src.runs.services.run_discovery import RunDiscoveryService

router = APIRouter(prefix="/runs", tags=["runs"])


# =============================================================================
# Dependencies
# =============================================================================


def get_run_discovery_service() -> RunDiscoveryService:
    """Dependency for RunDiscoveryService."""
    return RunDiscoveryService()


def get_dot_parser_service() -> DotParserService:
    """Dependency for DotParserService."""
    return DotParserService()


def get_results_reader_service() -> ResultsReaderService:
    """Dependency for ResultsReaderService."""
    return ResultsReaderService()


RunDiscoveryDep = Annotated[RunDiscoveryService, Depends(get_run_discovery_service)]
DotParserDep = Annotated[DotParserService, Depends(get_dot_parser_service)]
ResultsReaderDep = Annotated[ResultsReaderService, Depends(get_results_reader_service)]


# Query parameters for results pagination
LimitQuery = Annotated[int, Query(ge=1, le=100, description="Maximum results per page")]
OffsetQuery = Annotated[int, Query(ge=0, description="Results offset")]


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/graphs", response_model=GraphListResponse)
async def list_graphs(service: RunDiscoveryDep) -> GraphListResponse:
    """List available insight graphs with run counts.

    Scans $RUNS_ROOT/insight_graph/ for available graphs and their runs.

    Returns:
        GraphListResponse: List of graphs with metadata including run counts
            and latest run information.
    """
    discovered = service.discover_graphs()
    graphs = [GraphSummary.model_validate(g, from_attributes=True) for g in discovered]
    return GraphListResponse(graphs=graphs)


@router.get("/graphs/{graph_name}/runs", response_model=RunListResponse)
async def list_runs(graph_name: str, service: RunDiscoveryDep) -> RunListResponse:
    """List all runs for a specific insight graph.

    Args:
        graph_name: Name of the insight graph.
        service: RunDiscoveryService dependency.

    Returns:
        RunListResponse: List of runs with metadata, sorted newest first.
    """
    runs = service.list_runs_for_graph(graph_name)
    return RunListResponse(
        graph_name=graph_name,
        runs=[
            RunSummaryResponse(
                run_id=r.run_id,
                created_at=r.created_at,
                node_count=r.node_count,
            )
            for r in runs
        ],
    )


@router.get("/graphs/{graph_name}/runs/{run_id}", response_model=RunMetadataResponse)
async def get_run_metadata(graph_name: str, run_id: str, service: RunDiscoveryDep) -> RunMetadataResponse:
    """Get detailed metadata for a specific run.

    Args:
        graph_name: Name of the insight graph.
        run_id: Run identifier (timestamp format).
        service: RunDiscoveryService dependency.

    Returns:
        RunMetadataResponse: Full run metadata including node list.

    Raises:
        HTTPException: 404 if run not found.
    """
    metadata = service.get_run_metadata(graph_name, run_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {graph_name}/{run_id}")

    return RunMetadataResponse.model_validate(metadata, from_attributes=True)


@router.get(
    "/graphs/{graph_name}/runs/{run_id}/graph",
    response_model=GraphStructureResponse,
)
async def get_graph_structure(
    graph_name: str,
    run_id: str,
    discovery: RunDiscoveryDep,
    parser: DotParserDep,
) -> GraphStructureResponse:
    """Get graph structure in vis-network format.

    Parses the DOT file from the run and converts to vis-network compatible
    node/edge format.

    Args:
        graph_name: Name of the insight graph.
        run_id: Run identifier.
        discovery: RunDiscoveryService for validation.
        parser: DotParserService for DOT parsing.

    Returns:
        GraphStructureResponse: Nodes and edges for vis-network rendering.

    Raises:
        HTTPException: 404 if run not found.
    """
    # Validate run exists
    metadata = discovery.get_run_metadata(graph_name, run_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {graph_name}/{run_id}")

    # Build path to DOT file
    runs_root = Path(settings.RUNS_ROOT).expanduser().resolve()
    dot_path = runs_root / "insight_graph" / graph_name / run_id / "graphviz" / "graphviz_condensed.dot"

    # Parse DOT file
    structure = parser.parse_file(dot_path)

    return GraphStructureResponse(
        nodes=[
            VisNodeResponse(
                id=n.id,
                label=n.label,
                title=n.title,
                shape=n.shape,
                color=n.color,
            )
            for n in structure.nodes
        ],
        edges=[
            VisEdgeResponse(
                source=e.source,
                target=e.target,
                label=e.label,
                dashes=e.dashes,
                color=e.color,
            )
            for e in structure.edges
        ],
    )


@router.get(
    "/graphs/{graph_name}/runs/{run_id}/nodes/{node_id}/results",
    response_model=NodeResultsResponse,
)
async def get_node_results(
    graph_name: str,
    run_id: str,
    node_id: str,
    discovery: RunDiscoveryDep,
    reader: ResultsReaderDep,
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> NodeResultsResponse:
    """Get paginated entity results for a node.

    Args:
        graph_name: Name of the insight graph.
        run_id: Run identifier.
        node_id: Node identifier.
        discovery: RunDiscoveryService for validation.
        reader: ResultsReaderService for reading JSONL.
        limit: Maximum results per page (1-100).
        offset: Number of results to skip.

    Returns:
        NodeResultsResponse: Paginated entity results.

    Raises:
        HTTPException: 404 if run or node not found.
    """
    # Validate run exists
    metadata = discovery.get_run_metadata(graph_name, run_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {graph_name}/{run_id}")

    # Find node in metadata
    node_meta = next((n for n in metadata.nodes if n.node_id == node_id), None)
    if node_meta is None:
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    # Build path to JSONL file
    runs_root = Path(settings.RUNS_ROOT).expanduser().resolve()
    jsonl_path = runs_root / "insight_graph" / graph_name / run_id / node_meta.result_path

    # Read paginated results
    paginated = reader.read_results(jsonl_path, offset=offset, limit=limit)

    return NodeResultsResponse(
        node_id=node_id,
        total_count=paginated.total_count,
        offset=paginated.offset,
        limit=paginated.limit,
        results=[
            EntityResultResponse(
                entity_id=r.entity_id,
                entity_dimensions=r.entity_dimensions,
                encounters=r.encounters,
                metric_value=r.metric_value,
                z_score=r.z_score,
                percentile_rank=r.percentile_rank,
                anomaly_label=r.anomaly_label,
            )
            for r in paginated.results
        ],
    )
