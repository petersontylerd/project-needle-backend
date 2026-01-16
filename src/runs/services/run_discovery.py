"""Service for discovering insight graph runs from filesystem."""

import contextlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from src.config import settings
from src.services.path_validation import PathValidationError, validate_path_within_root

logger = logging.getLogger(__name__)


class RunSummary(BaseModel):
    """Summary of a single run."""

    run_id: str = Field(description="Run identifier (timestamp format)")
    created_at: datetime | None = Field(default=None, description="Run creation timestamp")
    node_count: int = Field(default=0, description="Number of nodes in run")


class NodeMetadata(BaseModel):
    """Metadata for a single node from index.json."""

    canonical_node_id: str
    node_id: str
    metric_id: str
    result_path: str
    statistical_methods: list[str] = Field(default_factory=list)
    entity_scope_display_name: str | None = None
    comparison_group: str | None = None


class RunMetadata(BaseModel):
    """Full metadata for a run from index.json."""

    run_id: str
    created_at: datetime | None = None
    nodes: list[NodeMetadata] = Field(default_factory=list)


class GraphSummary(BaseModel):
    """Summary of an insight graph."""

    graph_name: str
    run_count: int
    latest_run_id: str | None = None
    latest_run_timestamp: datetime | None = None


class RunDiscoveryService:
    """Service for discovering insight graph runs from filesystem.

    Scans $RUNS_ROOT/ for available graphs and runs.
    """

    def __init__(self, runs_root: Path | None = None) -> None:
        """Initialize service with runs root directory.

        Args:
            runs_root: Root directory for runs. Defaults to settings.RUNS_ROOT.
        """
        if runs_root is None:
            runs_root = Path(settings.RUNS_ROOT).expanduser().resolve()
        self.runs_root = runs_root
        self.insight_graph_root = runs_root

    def discover_graphs(self) -> list[GraphSummary]:
        """Discover all available insight graphs.

        Returns:
            List of GraphSummary objects sorted by graph name.
        """
        graphs: list[GraphSummary] = []

        if not self.insight_graph_root.exists():
            logger.warning("Insight graph root does not exist: %s", self.insight_graph_root)
            return graphs

        for graph_dir in sorted(self.insight_graph_root.iterdir()):
            if not graph_dir.is_dir():
                continue

            runs = self._get_valid_runs(graph_dir)
            if not runs:
                continue

            # Sort runs by ID (timestamp) descending
            runs.sort(reverse=True)
            latest_run_id = runs[0]

            # Parse timestamp from run ID
            latest_timestamp = self._parse_run_timestamp(latest_run_id)

            graphs.append(
                GraphSummary(
                    graph_name=graph_dir.name,
                    run_count=len(runs),
                    latest_run_id=latest_run_id,
                    latest_run_timestamp=latest_timestamp,
                )
            )

        return graphs

    def list_runs_for_graph(self, graph_name: str) -> list[RunSummary]:
        """List all runs for a specific graph.

        Args:
            graph_name: Name of the insight graph.

        Returns:
            List of RunSummary objects sorted newest first.
        """
        try:
            graph_dir = validate_path_within_root(
                self.insight_graph_root / graph_name,
                self.insight_graph_root,
            )
        except PathValidationError:
            logger.warning("Invalid graph name rejected: %s", graph_name)
            return []

        if not graph_dir.exists():
            return []

        runs: list[RunSummary] = []
        for run_id in self._get_valid_runs(graph_dir):
            run_dir = graph_dir / run_id
            index_path = run_dir / "results" / "index.json"

            created_at = self._parse_run_timestamp(run_id)
            node_count = 0

            # Try to read node count from index.json
            if index_path.exists():
                try:
                    with index_path.open() as f:
                        index_data = json.load(f)
                        node_count = len(index_data.get("nodes", []))
                        if "created_at" in index_data:
                            created_at = datetime.fromisoformat(index_data["created_at"].replace("Z", "+00:00"))
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning("Failed to parse index.json for %s/%s: %s", graph_name, run_id, e)

            runs.append(
                RunSummary(
                    run_id=run_id,
                    created_at=created_at,
                    node_count=node_count,
                )
            )

        # Sort by run_id descending (newest first)
        runs.sort(key=lambda r: r.run_id, reverse=True)
        return runs

    def get_run_metadata(self, graph_name: str, run_id: str) -> RunMetadata | None:
        """Get detailed metadata for a specific run.

        Args:
            graph_name: Name of the insight graph.
            run_id: Run identifier.

        Returns:
            RunMetadata or None if not found.
        """
        # Validate path to prevent traversal
        try:
            run_dir = validate_path_within_root(
                self.insight_graph_root / graph_name / run_id,
                self.insight_graph_root,
            )
        except PathValidationError:
            logger.warning("Invalid path rejected: %s/%s", graph_name, run_id)
            return None

        index_path = run_dir / "results" / "index.json"

        if not index_path.exists():
            return None

        try:
            with index_path.open() as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to read index.json for %s/%s: %s", graph_name, run_id, e)
            return None

        created_at = None
        if "created_at" in data:
            with contextlib.suppress(ValueError):
                created_at = datetime.fromisoformat(data["created_at"].replace("Z", "+00:00"))

        nodes: list[NodeMetadata] = []
        for node_data in data.get("nodes", []):
            summary = node_data.get("summary", {})
            entity_scope = summary.get("entity_scope", {})

            nodes.append(
                NodeMetadata(
                    canonical_node_id=node_data.get("canonical_node_id", ""),
                    node_id=node_data.get("node_id", ""),
                    metric_id=node_data.get("metric_id", ""),
                    result_path=node_data.get("result_path", ""),
                    statistical_methods=node_data.get("statistical_methods", []),
                    entity_scope_display_name=entity_scope.get("display_name"),
                    comparison_group=entity_scope.get("comparison_group"),
                )
            )

        return RunMetadata(
            run_id=data.get("run_id", run_id),
            created_at=created_at,
            nodes=nodes,
        )

    def _get_valid_runs(self, graph_dir: Path) -> list[str]:
        """Get valid run IDs from a graph directory.

        A valid run has a results/index.json file.

        Args:
            graph_dir: Path to graph directory.

        Returns:
            List of valid run ID strings.
        """
        valid_runs: list[str] = []
        for run_dir in graph_dir.iterdir():
            if not run_dir.is_dir():
                continue
            index_path = run_dir / "results" / "index.json"
            if index_path.exists():
                valid_runs.append(run_dir.name)
        return valid_runs

    def _parse_run_timestamp(self, run_id: str) -> datetime | None:
        """Parse timestamp from run ID.

        Run IDs are in format YYYYMMDDHHmmss.

        Args:
            run_id: Run identifier string.

        Returns:
            Parsed datetime or None if parsing fails.
        """
        try:
            return datetime.strptime(run_id, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        except ValueError:
            return None
