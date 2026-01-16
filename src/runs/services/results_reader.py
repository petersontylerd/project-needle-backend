"""Service for reading JSONL entity results with pagination."""

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class EntityResult(BaseModel):
    """Parsed entity result from JSONL."""

    entity_id: str = Field(description="Primary entity identifier value")
    entity_dimensions: list[dict[str, Any]] = Field(default_factory=list, description="Full entity dimension list")
    encounters: int = Field(default=0, description="Encounter count")
    metric_value: float | None = Field(default=None, description="Metric value")
    z_score: float | None = Field(default=None, description="Z-score statistic")
    percentile_rank: float | None = Field(default=None, description="Percentile rank")
    anomaly_label: str | None = Field(default=None, description="Anomaly classification")
    statistical_methods: list[dict[str, Any]] = Field(default_factory=list, description="Raw statistical methods data")


class PaginatedResults(BaseModel):
    """Paginated entity results."""

    total_count: int = Field(description="Total number of entity results")
    offset: int = Field(description="Current offset")
    limit: int = Field(description="Page size limit")
    results: list[EntityResult] = Field(description="Entity results for this page")


class ResultsReaderService:
    """Read and paginate JSONL entity results.

    JSONL files have a metadata line first, followed by entity result lines.
    """

    def read_results(
        self,
        jsonl_path: Path,
        offset: int = 0,
        limit: int = 50,
    ) -> PaginatedResults:
        """Read paginated entity results from JSONL file.

        Args:
            jsonl_path: Path to the JSONL file.
            offset: Number of results to skip.
            limit: Maximum results to return.

        Returns:
            PaginatedResults with entity data.
        """
        if not jsonl_path.exists():
            logger.warning("JSONL file not found: %s", jsonl_path)
            return PaginatedResults(total_count=0, offset=offset, limit=limit, results=[])

        try:
            with jsonl_path.open() as f:
                lines = f.readlines()
        except OSError as e:
            logger.error("Failed to read JSONL file %s: %s", jsonl_path, e)
            return PaginatedResults(total_count=0, offset=offset, limit=limit, results=[])

        # Filter out metadata lines (first line typically)
        entity_lines: list[str] = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                # Skip metadata records
                if data.get("type") == "node_metadata":
                    continue
                entity_lines.append(line)
            except json.JSONDecodeError:
                continue

        total_count = len(entity_lines)

        # Apply pagination
        start = offset
        end = offset + limit
        page_lines = entity_lines[start:end]

        results: list[EntityResult] = []
        for line in page_lines:
            try:
                data = json.loads(line)
                result = self._parse_entity_result(data)
                results.append(result)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning("Failed to parse entity result: %s", e)
                continue

        return PaginatedResults(
            total_count=total_count,
            offset=offset,
            limit=limit,
            results=results,
        )

    def _parse_entity_result(self, data: dict[str, Any]) -> EntityResult:
        """Parse a single entity result record."""
        # Extract entity ID from first dimension
        entity_dimensions = data.get("entity", [])
        entity_id = ""
        if entity_dimensions:
            entity_id = str(entity_dimensions[0].get("value", ""))

        # Extract metric value
        metric_value = None
        metrics = data.get("metric", [])
        if metrics:
            metric_value = metrics[0].get("values")

        # Extract statistics from first statistical method
        z_score = None
        percentile_rank = None
        anomaly_label = None
        statistical_methods = data.get("statistical_methods", [])

        if statistical_methods:
            first_method = statistical_methods[0]
            stats = first_method.get("statistics", {})

            # Try different z-score field names
            z_score = stats.get("simple_zscore") or stats.get("robust_zscore") or stats.get("trending_simple_zscore")
            percentile_rank = stats.get("percentile_rank")

            # Get anomaly from first anomaly method
            anomalies = first_method.get("anomalies", [])
            if anomalies:
                methods = anomalies[0].get("methods", [])
                if methods:
                    anomaly_label = methods[0].get("anomaly")

        return EntityResult(
            entity_id=entity_id,
            entity_dimensions=entity_dimensions,
            encounters=data.get("encounters", 0),
            metric_value=metric_value,
            z_score=z_score,
            percentile_rank=percentile_rank,
            anomaly_label=anomaly_label,
            statistical_methods=statistical_methods,
        )

    def count_results(self, jsonl_path: Path) -> int:
        """Count total entity results in JSONL file.

        Args:
            jsonl_path: Path to the JSONL file.

        Returns:
            Total number of entity results (excluding metadata).
        """
        if not jsonl_path.exists():
            return 0

        count = 0
        try:
            with jsonl_path.open() as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        if data.get("type") != "node_metadata":
                            count += 1
                    except json.JSONDecodeError:
                        continue
        except OSError:
            return 0

        return count
