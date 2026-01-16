"""Pydantic schemas for runs API responses."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class GraphSummary(BaseModel):
    """Summary of an insight graph with run counts."""

    model_config = ConfigDict(from_attributes=True)

    graph_name: str = Field(description="Name of the insight graph")
    run_count: int = Field(description="Number of runs available")
    latest_run_id: str | None = Field(default=None, description="Most recent run ID")
    latest_run_timestamp: datetime | None = Field(default=None, description="Timestamp of most recent run")


class GraphListResponse(BaseModel):
    """Response for listing available graphs."""

    graphs: list[GraphSummary] = Field(description="List of available graphs")


class RunSummaryResponse(BaseModel):
    """Summary of a single insight graph run."""

    model_config = ConfigDict(from_attributes=True)

    run_id: str = Field(description="Run identifier (timestamp format YYYYMMDDHHmmss)")
    created_at: datetime | None = Field(default=None, description="Run creation timestamp")
    node_count: int = Field(default=0, description="Number of nodes in this run")


class RunListResponse(BaseModel):
    """Response for listing runs of a graph."""

    graph_name: str = Field(description="Name of the insight graph")
    runs: list[RunSummaryResponse] = Field(description="List of runs, newest first")


class NodeMetadataResponse(BaseModel):
    """Node metadata from run index."""

    model_config = ConfigDict(from_attributes=True)

    canonical_node_id: str = Field(description="Canonical node identifier")
    node_id: str = Field(description="Node identifier")
    metric_id: str = Field(description="Associated metric")
    result_path: str = Field(description="Path to results JSONL file")
    statistical_methods: list[str] = Field(default_factory=list, description="Statistical methods used")
    entity_scope_display_name: str | None = Field(default=None, description="Human-readable entity scope")
    comparison_group: str | None = Field(default=None, description="Comparison group")


class RunMetadataResponse(BaseModel):
    """Full run metadata response."""

    model_config = ConfigDict(from_attributes=True)

    run_id: str = Field(description="Run identifier")
    created_at: datetime | None = Field(default=None, description="Creation timestamp")
    nodes: list[NodeMetadataResponse] = Field(description="Nodes in this run")


class VisNodeResponse(BaseModel):
    """Node in vis-network format."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: str = Field(description="Unique node identifier")
    label: str = Field(description="Display label")
    title: str | None = Field(default=None, description="Tooltip text")
    shape: str = Field(default="box", description="Node shape")
    color: dict[str, Any] | None = Field(default=None, description="Color configuration")


class VisEdgeResponse(BaseModel):
    """Edge in vis-network format."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True, serialize_by_alias=True)

    source: str = Field(description="Source node ID", serialization_alias="from")
    target: str = Field(description="Target node ID", serialization_alias="to")
    label: str | None = Field(default=None, description="Edge label")
    dashes: bool = Field(default=False, description="Whether edge is dashed")
    color: dict[str, Any] | None = Field(default=None, description="Color configuration")


class GraphStructureResponse(BaseModel):
    """Complete graph structure for vis-network."""

    nodes: list[VisNodeResponse] = Field(description="Graph nodes")
    edges: list[VisEdgeResponse] = Field(description="Graph edges")


class EntityResultResponse(BaseModel):
    """Parsed entity result."""

    model_config = ConfigDict(from_attributes=True)

    entity_id: str = Field(description="Primary entity identifier")
    entity_dimensions: list[dict[str, Any]] = Field(description="Full entity dimensions")
    encounters: int = Field(default=0, description="Encounter count")
    metric_value: float | None = Field(default=None, description="Metric value")
    z_score: float | None = Field(default=None, description="Z-score")
    percentile_rank: float | None = Field(default=None, description="Percentile rank")
    anomaly_label: str | None = Field(default=None, description="Anomaly classification")


class NodeResultsResponse(BaseModel):
    """Paginated node results response."""

    node_id: str = Field(description="Node identifier")
    total_count: int = Field(description="Total entity count")
    offset: int = Field(description="Current offset")
    limit: int = Field(description="Page size")
    results: list[EntityResultResponse] = Field(description="Entity results")
