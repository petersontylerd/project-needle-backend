"""Semantic Manifest Parser Service.

Parses dbt semantic_manifest.json to provide metric definitions,
semantic model metadata, and SQL generation capabilities.

This is a lightweight alternative to the full dbt-metricflow SDK,
parsing the already-generated semantic manifest directly.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class MeasureDefinition:
    """Definition of a measure from semantic model."""

    name: str
    description: str
    agg: str  # count, sum, average, min, max, count_distinct
    expr: str
    create_metric: bool = False


@dataclass
class DimensionDefinition:
    """Definition of a dimension from semantic model."""

    name: str
    type: str  # categorical, time
    expr: str
    description: str = ""
    time_granularity: str | None = None


@dataclass
class EntityDefinition:
    """Definition of an entity (join key) from semantic model."""

    name: str
    type: str  # primary, foreign, unique
    expr: str
    description: str = ""


@dataclass
class SemanticModelDefinition:
    """Parsed semantic model with all components."""

    name: str
    description: str
    table_name: str  # e.g., "fct_signals"
    schema_name: str  # e.g., "public_marts"
    database: str  # e.g., "quality_compass"
    entities: list[EntityDefinition] = field(default_factory=list)
    dimensions: list[DimensionDefinition] = field(default_factory=list)
    measures: list[MeasureDefinition] = field(default_factory=list)
    default_time_dimension: str | None = None


@dataclass
class MetricDefinition:
    """Parsed metric definition with query metadata."""

    name: str
    description: str
    label: str
    type: str  # simple, derived, cumulative, ratio
    category: str | None = None
    display_format: str | None = None
    measure_reference: str | None = None  # For simple metrics
    filter_expression: str | None = None
    derived_expr: str | None = None  # For derived metrics
    input_metrics: list[str] = field(default_factory=list)  # For derived metrics


class SemanticManifestService:
    """Service for parsing and querying dbt semantic manifest.

    Provides:
    1. Metric definitions from semantic_manifest.json
    2. Semantic model metadata (measures, dimensions, entities)
    3. Dimension lookups for metric queries
    """

    def __init__(self, dbt_project_path: Path | None = None) -> None:
        """Initialize the service.

        Args:
            dbt_project_path: Path to dbt project. Defaults to backend/dbt.
        """
        self._dbt_path = dbt_project_path or Path(__file__).parent.parent.parent / "dbt"
        self._manifest: dict[str, Any] | None = None
        self._semantic_models: dict[str, SemanticModelDefinition] = {}
        self._metrics: dict[str, MetricDefinition] = {}

    @property
    def manifest_path(self) -> Path:
        """Path to semantic_manifest.json."""
        return self._dbt_path / "target" / "semantic_manifest.json"

    def _load_manifest(self) -> dict[str, Any]:
        """Load and cache semantic manifest."""
        if self._manifest is None:
            if not self.manifest_path.exists():
                raise FileNotFoundError(f"Semantic manifest not found at {self.manifest_path}. Run 'dbt parse' to generate it.")
            self._manifest = json.loads(self.manifest_path.read_text())
            self._parse_semantic_models()
            self._parse_metrics()
            logger.info(
                "Loaded semantic manifest: %d models, %d metrics",
                len(self._semantic_models),
                len(self._metrics),
            )
        return self._manifest

    def _parse_semantic_models(self) -> None:
        """Parse semantic models from manifest."""
        manifest = self._manifest or {}
        for sm in manifest.get("semantic_models", []):
            # Extract table info from node_relation
            node_relation = sm.get("node_relation", {})
            table_name = node_relation.get("alias", sm["name"])
            schema_name = node_relation.get("schema_name", "public")
            database = node_relation.get("database", "")

            entities = [
                EntityDefinition(
                    name=e["name"],
                    type=e["type"],
                    expr=e.get("expr", e["name"]),
                    description=e.get("description", ""),
                )
                for e in sm.get("entities", [])
            ]

            dimensions = [
                DimensionDefinition(
                    name=d["name"],
                    type=d["type"],
                    expr=d.get("expr", d["name"]),
                    description=d.get("description", ""),
                    time_granularity=(d.get("type_params") or {}).get("time_granularity"),
                )
                for d in sm.get("dimensions", [])
            ]

            measures = [
                MeasureDefinition(
                    name=m["name"],
                    description=m.get("description", ""),
                    agg=m["agg"],
                    expr=m.get("expr", m["name"]),
                    create_metric=m.get("create_metric", False),
                )
                for m in sm.get("measures", [])
            ]

            self._semantic_models[sm["name"]] = SemanticModelDefinition(
                name=sm["name"],
                description=sm.get("description", "").strip(),
                table_name=table_name,
                schema_name=schema_name,
                database=database,
                entities=entities,
                dimensions=dimensions,
                measures=measures,
                default_time_dimension=(sm.get("defaults") or {}).get("agg_time_dimension"),
            )

    def _parse_metrics(self) -> None:
        """Parse metrics from manifest."""
        manifest = self._manifest or {}
        for m in manifest.get("metrics", []):
            meta = (m.get("config") or {}).get("meta", {})
            type_params = m.get("type_params", {})

            # Extract measure reference for simple metrics
            measure_ref = None
            if m["type"] == "simple":
                measure_info = type_params.get("measure", {})
                if isinstance(measure_info, dict):
                    measure_ref = measure_info.get("name")

            # Extract filter expression
            filter_expr = None
            filter_info = m.get("filter")
            if filter_info:
                where_filters = filter_info.get("where_filters", [])
                if where_filters:
                    # Join multiple filters with AND
                    filter_parts = [f.get("where_sql_template", "") for f in where_filters]
                    filter_expr = " AND ".join(filter_parts)

            # Extract derived metric info
            derived_expr = None
            input_metrics: list[str] = []
            if m["type"] in ("derived", "ratio"):
                derived_expr = type_params.get("expr")
                metrics_list = type_params.get("metrics", [])
                input_metrics = [im.get("name") for im in metrics_list if im.get("name")]

            self._metrics[m["name"]] = MetricDefinition(
                name=m["name"],
                description=m.get("description", "").strip(),
                label=m.get("label", m["name"]),
                type=m["type"],
                category=meta.get("category"),
                display_format=meta.get("display_format"),
                measure_reference=measure_ref,
                filter_expression=filter_expr,
                derived_expr=derived_expr,
                input_metrics=input_metrics,
            )

    def get_all_metrics(self) -> list[MetricDefinition]:
        """Get all metric definitions.

        Returns:
            List of all metric definitions.
        """
        self._load_manifest()
        return list(self._metrics.values())

    def get_metric(self, name: str) -> MetricDefinition | None:
        """Get a specific metric by name.

        Args:
            name: Metric name.

        Returns:
            MetricDefinition or None if not found.
        """
        self._load_manifest()
        return self._metrics.get(name)

    def get_metrics_by_category(self, category: str) -> list[MetricDefinition]:
        """Get metrics filtered by category.

        Args:
            category: Category name to filter by.

        Returns:
            List of metrics in the specified category.
        """
        self._load_manifest()
        return [m for m in self._metrics.values() if m.category == category]

    def get_all_categories(self) -> list[str]:
        """Get all unique metric categories.

        Returns:
            Sorted list of category names.
        """
        self._load_manifest()
        categories = {m.category for m in self._metrics.values() if m.category}
        return sorted(categories)

    def get_all_semantic_models(self) -> list[SemanticModelDefinition]:
        """Get all semantic model definitions.

        Returns:
            List of all semantic model definitions.
        """
        self._load_manifest()
        return list(self._semantic_models.values())

    def get_semantic_model(self, name: str) -> SemanticModelDefinition | None:
        """Get a specific semantic model by name.

        Args:
            name: Semantic model name.

        Returns:
            SemanticModelDefinition or None if not found.
        """
        self._load_manifest()
        return self._semantic_models.get(name)

    def get_available_dimensions(self, metric_name: str) -> list[str]:
        """Get dimensions available for a metric.

        For simple metrics, returns dimensions from the semantic model
        that contains the metric's measure.

        Args:
            metric_name: Name of the metric.

        Returns:
            List of dimension names available for grouping.
        """
        metric = self.get_metric(metric_name)
        if not metric or not metric.measure_reference:
            return []

        # Find semantic model containing this measure
        for sm in self._semantic_models.values():
            for measure in sm.measures:
                if measure.name == metric.measure_reference:
                    return [d.name for d in sm.dimensions]
        return []

    def find_measure_context(self, measure_name: str) -> tuple[SemanticModelDefinition, MeasureDefinition] | None:
        """Find the semantic model and measure definition for a measure name.

        Args:
            measure_name: Name of the measure.

        Returns:
            Tuple of (semantic model, measure) or None if not found.
        """
        self._load_manifest()
        for sm in self._semantic_models.values():
            for measure in sm.measures:
                if measure.name == measure_name:
                    return sm, measure
        return None

    def refresh_cache(self) -> None:
        """Force refresh of cached manifest."""
        self._manifest = None
        self._semantic_models = {}
        self._metrics = {}
        logger.info("Semantic manifest cache cleared")


# Singleton instance for dependency injection
_service: SemanticManifestService | None = None


def get_semantic_manifest_service() -> SemanticManifestService:
    """Get or create the semantic manifest service singleton.

    Returns:
        SemanticManifestService instance.
    """
    global _service
    if _service is None:
        _service = SemanticManifestService()
    return _service
