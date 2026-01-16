"""Pydantic schemas for ontology API responses."""

from typing import Any

from pydantic import BaseModel


class VertexResponse(BaseModel):
    """Response schema for a graph vertex."""

    id: str
    label: str
    properties: dict[str, Any]


class EdgeResponse(BaseModel):
    """Response schema for a graph edge."""

    label: str
    from_id: str
    to_id: str
    properties: dict[str, Any] | None = None


class NeighborResponse(BaseModel):
    """Response schema for a neighbor traversal result."""

    vertex: VertexResponse
    edge: EdgeResponse


class VertexListResponse(BaseModel):
    """Response schema for listing vertices."""

    vertices: list[VertexResponse]
    count: int


class NeighborListResponse(BaseModel):
    """Response schema for neighbor traversal results."""

    neighbors: list[NeighborResponse]
    count: int


class StatsResponse(BaseModel):
    """Response schema for graph statistics."""

    vertex_counts: dict[str, int]
    edge_counts: dict[str, int]
