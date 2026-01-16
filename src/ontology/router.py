"""FastAPI router for ontology graph traversal endpoints."""

import json
import logging
import re
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_async_db_session

from .cypher import age_query_wrapper, build_match_vertex, build_traverse
from .schema import EDGE_LABELS, GRAPH_NAME, VERTEX_LABELS
from .schemas import (
    EdgeResponse,
    NeighborListResponse,
    NeighborResponse,
    StatsResponse,
    VertexListResponse,
    VertexResponse,
)

logger = logging.getLogger(__name__)

# Regex for AGE vertex format: Label{...}::vertex
AGE_VERTEX_PATTERN = re.compile(r"^(\w+)(\{.*\})::vertex$")

router = APIRouter(prefix="/ontology", tags=["ontology"])

# Type alias for dependency injection
SessionDep = Annotated[AsyncSession, Depends(get_async_db_session)]

# Valid vertex labels for validation
VALID_LABELS = {cls.label for cls in VERTEX_LABELS}


def _parse_agtype_vertex(result: Any) -> dict[str, Any]:
    """Parse an agtype vertex result into a dictionary.

    AGE returns vertices in format: Label{id: "value", ...}::vertex
    This parser handles the common formats returned by AGE queries.

    Args:
        result: The agtype result from AGE query.

    Returns:
        Dictionary with vertex properties.
    """
    if result is None:
        return {}

    result_str = str(result[0]) if isinstance(result, tuple) else str(result)

    # Try AGE vertex format first: Label{...}::vertex
    match = AGE_VERTEX_PATTERN.match(result_str)
    if match:
        label, props_str = match.groups()
        try:
            properties: dict[str, Any] = json.loads(props_str)
            properties["__label"] = label
            return properties
        except json.JSONDecodeError:
            logger.warning("Failed to parse AGE properties: %s", props_str[:100])

    # Try clean JSON format
    if result_str.startswith("{"):
        try:
            parsed: dict[str, Any] = json.loads(result_str)
            return parsed
        except json.JSONDecodeError:
            pass

    logger.debug("Unparseable AGE result, returning raw: %s", result_str[:100])
    return {"raw": result_str}


@router.get("/labels")
async def list_labels() -> dict[str, list[str]]:
    """List available vertex labels.

    Returns:
        Dictionary with list of valid vertex label names.
    """
    return {"labels": sorted(VALID_LABELS)}


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    session: SessionDep,
) -> StatsResponse:
    """Get graph statistics.

    Returns counts for all vertex and edge labels in the ontology graph.

    Args:
        session: Database session.

    Returns:
        StatsResponse with vertex_counts and edge_counts dictionaries.

    Raises:
        HTTPException: 503 if graph database is unavailable.
    """
    try:
        vertex_counts: dict[str, int] = {}
        for vertex_class in VERTEX_LABELS:
            label = vertex_class.label
            cypher = f"MATCH (n:{label}) RETURN count(n)"
            sql = age_query_wrapper(cypher, GRAPH_NAME)
            result = await session.execute(text(sql))
            rows = result.fetchall()
            # AGE returns count as agtype, need to parse
            count_val = rows[0][0] if rows else 0
            if isinstance(count_val, str):
                count_val = int(count_val.replace("::int8", "").strip())
            vertex_counts[label] = count_val

        edge_counts: dict[str, int] = {}
        for edge_class in EDGE_LABELS:
            label = edge_class.label
            cypher = f"MATCH ()-[r:{label}]->() RETURN count(r)"
            sql = age_query_wrapper(cypher, GRAPH_NAME)
            result = await session.execute(text(sql))
            rows = result.fetchall()
            count_val = rows[0][0] if rows else 0
            if isinstance(count_val, str):
                count_val = int(count_val.replace("::int8", "").strip())
            edge_counts[label] = count_val

        return StatsResponse(vertex_counts=vertex_counts, edge_counts=edge_counts)

    except Exception as exc:
        logger.error("Database query failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Graph database temporarily unavailable",
        ) from exc


@router.get("/vertices/{label}", response_model=VertexListResponse)
async def list_vertices(
    session: SessionDep,
    label: str,
    limit: int = Query(default=100, le=1000),
) -> VertexListResponse:
    """List vertices by label.

    Args:
        session: Database session.
        label: Vertex label to filter by.
        limit: Maximum number of results (default 100, max 1000).

    Returns:
        List of vertices matching the label.

    Raises:
        HTTPException: If label is invalid.
    """
    if label not in VALID_LABELS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid label '{label}'. Valid labels: {sorted(VALID_LABELS)}",
        )

    cypher = build_match_vertex(label, limit=limit)
    sql = age_query_wrapper(cypher, GRAPH_NAME)

    try:
        result = await session.execute(text(sql))
        rows = result.fetchall()
    except Exception as exc:
        logger.error("Database query failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Graph database temporarily unavailable",
        ) from exc

    vertices = []
    for row in rows:
        parsed = _parse_agtype_vertex(row[0])
        vertices.append(
            VertexResponse(
                id=parsed.get("id", "unknown"),
                label=label,
                properties=parsed,
            )
        )

    return VertexListResponse(vertices=vertices, count=len(vertices))


@router.get("/vertices/{label}/{vertex_id}", response_model=VertexResponse)
async def get_vertex(
    session: SessionDep,
    label: str,
    vertex_id: str,
) -> VertexResponse:
    """Get a single vertex by label and id.

    Args:
        session: Database session.
        label: Vertex label.
        vertex_id: Vertex id property.

    Returns:
        The matching vertex.

    Raises:
        HTTPException: If label is invalid or vertex not found.
    """
    if label not in VALID_LABELS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid label '{label}'. Valid labels: {sorted(VALID_LABELS)}",
        )

    cypher = build_match_vertex(label, filters={"id": vertex_id}, limit=1)
    sql = age_query_wrapper(cypher, GRAPH_NAME)

    try:
        result = await session.execute(text(sql))
        rows = result.fetchall()
    except Exception as exc:
        logger.error("Database query failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Graph database temporarily unavailable",
        ) from exc

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Vertex {label}/{vertex_id} not found",
        )

    parsed = _parse_agtype_vertex(rows[0][0])
    return VertexResponse(
        id=parsed.get("id", vertex_id),
        label=label,
        properties=parsed,
    )


@router.get("/vertices/{label}/{vertex_id}/neighbors", response_model=NeighborListResponse)
async def get_neighbors(
    session: SessionDep,
    label: str,
    vertex_id: str,
    edge_label: str | None = Query(default=None),
    direction: str = Query(default="outgoing", pattern="^(outgoing|incoming|both)$"),
    limit: int = Query(default=100, le=1000),
) -> NeighborListResponse:
    """Get neighbors of a vertex via graph traversal.

    Args:
        session: Database session.
        label: Starting vertex label.
        vertex_id: Starting vertex id.
        edge_label: Optional edge label to filter by.
        direction: Traversal direction (outgoing, incoming, both).
        limit: Maximum number of results.

    Returns:
        List of neighbor vertices with their connecting edges.

    Raises:
        HTTPException: If label is invalid.
    """
    if label not in VALID_LABELS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid label '{label}'. Valid labels: {sorted(VALID_LABELS)}",
        )

    cypher = build_traverse(label, vertex_id, edge_label, direction)
    sql = age_query_wrapper(cypher, GRAPH_NAME, columns="(m agtype, r agtype)")

    try:
        result = await session.execute(text(sql))
        rows = result.fetchall()
    except Exception as exc:
        logger.error("Database query failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Graph database temporarily unavailable",
        ) from exc

    neighbors = []
    for row in rows[:limit]:
        # AGE returns (vertex, edge) tuple
        vertex_data = _parse_agtype_vertex(row[0]) if len(row) > 0 else {}
        edge_data = _parse_agtype_vertex(row[1]) if len(row) > 1 else {}

        neighbor_vertex = VertexResponse(
            id=vertex_data.get("id", "unknown"),
            label=vertex_data.get("label", "unknown"),
            properties=vertex_data,
        )

        neighbor_edge = EdgeResponse(
            label=edge_data.get("label", edge_label or "unknown"),
            from_id=vertex_id,
            to_id=vertex_data.get("id", "unknown"),
            properties=edge_data if edge_data else None,
        )

        neighbors.append(NeighborResponse(vertex=neighbor_vertex, edge=neighbor_edge))

    return NeighborListResponse(neighbors=neighbors, count=len(neighbors))
