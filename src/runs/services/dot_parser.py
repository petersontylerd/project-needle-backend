"""Service for parsing Graphviz DOT files to vis-network format."""

import logging
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class VisNode(BaseModel):
    """Node in vis-network format."""

    id: str = Field(description="Unique node identifier")
    label: str = Field(description="Display label (may contain newlines)")
    title: str | None = Field(default=None, description="Tooltip text")
    shape: str = Field(default="box", description="Node shape")
    color: dict[str, Any] | None = Field(default=None, description="Color configuration")


class VisEdge(BaseModel):
    """Edge in vis-network format.

    Uses 'from' and 'to' aliases for vis-network compatibility during serialization.
    """

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    source: str = Field(description="Source node ID", serialization_alias="from")
    target: str = Field(description="Target node ID", serialization_alias="to")
    label: str | None = Field(default=None, description="Edge label")
    dashes: bool = Field(default=False, description="Whether edge is dashed")
    color: dict[str, Any] | None = Field(default=None, description="Color configuration")


class GraphStructure(BaseModel):
    """Complete graph structure for vis-network."""

    nodes: list[VisNode] = Field(default_factory=list)
    edges: list[VisEdge] = Field(default_factory=list)


class DotParserService:
    """Parse Graphviz DOT files to vis-network format.

    Handles the specific DOT format produced by insight graph runs.
    """

    # Pattern for node definitions: "node_id" [attr="value", ...]
    # Uses DOTALL to handle multiline labels
    NODE_PATTERN = re.compile(
        r'^\s*"([^"]+)"\s*\[([^\]]+)\];?\s*$',
        re.MULTILINE,
    )

    # Pattern for edge definitions: "source" -> "target" [attr="value", ...]
    EDGE_PATTERN = re.compile(
        r'"([^"]+)"\s*->\s*"([^"]+)"\s*\[([^\]]+)\]',
        re.MULTILINE,
    )

    # Pattern for extracting attribute values (handles multiline labels)
    ATTR_PATTERN = re.compile(r'(\w+)="([^"]*(?:\n[^"]*)*)"')

    # Edge types that should be rendered as dashed
    DASHED_EDGE_TYPES = {"trends_to"}

    # Edge type colors
    EDGE_COLORS = {
        "trends_to": "#565EAA",
        "segments_into": "#38A169",
        "potentially_driven_by": "#DD6B20",  # Orange - suggests exploration/hypothesis
    }

    def parse_file(self, dot_path: Path) -> GraphStructure:
        """Parse a DOT file and return vis-network structure.

        Args:
            dot_path: Path to the DOT file.

        Returns:
            GraphStructure with nodes and edges for vis-network.
        """
        if not dot_path.exists():
            logger.warning("DOT file not found: %s", dot_path)
            return GraphStructure()

        try:
            content = dot_path.read_text(encoding="utf-8")
        except OSError as e:
            logger.error("Failed to read DOT file %s: %s", dot_path, e)
            return GraphStructure()

        nodes = self._parse_nodes(content)
        edges = self._parse_edges(content)

        return GraphStructure(nodes=nodes, edges=edges)

    def _parse_nodes(self, content: str) -> list[VisNode]:
        """Extract nodes from DOT content."""
        nodes: list[VisNode] = []
        seen_ids: set[str] = set()

        for match in self.NODE_PATTERN.finditer(content):
            node_id = match.group(1)
            attrs_str = match.group(2)

            # Skip if already seen (avoid duplicates)
            if node_id in seen_ids:
                continue
            seen_ids.add(node_id)

            attrs = self._parse_attributes(attrs_str)

            # Extract label (may have embedded literal newlines)
            label = attrs.get("label", node_id)

            # Build color config from fillcolor
            color = None
            if "fillcolor" in attrs:
                color = {
                    "background": attrs["fillcolor"],
                    "border": attrs.get("color", "#696969"),
                }

            nodes.append(
                VisNode(
                    id=node_id,
                    label=label,
                    title=node_id,  # Full ID as tooltip
                    shape=attrs.get("shape", "box"),
                    color=color,
                )
            )

        return nodes

    def _parse_edges(self, content: str) -> list[VisEdge]:
        """Extract edges from DOT content."""
        edges: list[VisEdge] = []

        for match in self.EDGE_PATTERN.finditer(content):
            source = match.group(1)
            target = match.group(2)
            attrs_str = match.group(3)

            attrs = self._parse_attributes(attrs_str)

            edge_type = attrs.get("xlabel", "")
            dashes = edge_type in self.DASHED_EDGE_TYPES

            # Build color config
            color = None
            if "color" in attrs:
                color = {"color": attrs["color"]}
            elif edge_type in self.EDGE_COLORS:
                color = {"color": self.EDGE_COLORS[edge_type]}

            edges.append(
                VisEdge(
                    source=source,
                    target=target,
                    label=edge_type or None,
                    dashes=dashes,
                    color=color,
                )
            )

        return edges

    def _parse_attributes(self, attrs_str: str) -> dict[str, str]:
        """Parse attribute string into dictionary."""
        attrs: dict[str, str] = {}
        for match in self.ATTR_PATTERN.finditer(attrs_str):
            key = match.group(1)
            value = match.group(2)
            attrs[key] = value
        return attrs
