"""dbt Metadata Service.

Parses dbt artifacts (manifest.json, catalog.json) and exposes
model metadata, lineage, and documentation URLs for the Quality Compass dashboard.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.config import settings

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Metadata for a model column."""

    name: str
    description: str
    data_type: str | None = None


@dataclass
class ModelMetadata:
    """Metadata for a dbt model."""

    unique_id: str
    name: str
    description: str
    schema_name: str
    database: str
    materialization: str
    columns: list[ColumnMetadata] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    referenced_by: list[str] = field(default_factory=list)
    docs_url: str | None = None


@dataclass
class LineageNode:
    """Node in the lineage graph."""

    unique_id: str
    name: str
    resource_type: str
    layer: str
    docs_url: str | None = None


@dataclass
class LineageGraph:
    """Full lineage graph for a model."""

    target_model: str
    upstream: list[LineageNode]
    downstream: list[LineageNode]


class DbtMetadataService:
    """Service for accessing dbt metadata and generating documentation URLs.

    Parses manifest.json for model metadata and lineage information.
    Generates deep-link URLs to dbt documentation.

    Attributes:
        _dbt_path: Path to dbt project directory.
        _manifest: Cached manifest.json contents.
        _catalog: Cached catalog.json contents.
    """

    def __init__(self, dbt_project_path: Path | None = None) -> None:
        """Initialize metadata service.

        Args:
            dbt_project_path: Path to dbt project. Defaults to backend/dbt
                relative to this file, or from settings.DBT_PROJECT_PATH.
        """
        if dbt_project_path:
            self._dbt_path = dbt_project_path
        elif settings.DBT_PROJECT_PATH:
            self._dbt_path = Path(settings.DBT_PROJECT_PATH)
        else:
            # Default: backend/dbt relative to this service file
            self._dbt_path = Path(__file__).parent.parent.parent / "dbt"

        self._manifest: dict[str, Any] | None = None
        self._catalog: dict[str, Any] | None = None

    @property
    def manifest_path(self) -> Path:
        """Path to manifest.json."""
        return self._dbt_path / "target" / "manifest.json"

    @property
    def catalog_path(self) -> Path:
        """Path to catalog.json."""
        return self._dbt_path / "target" / "catalog.json"

    @property
    def docs_base_url(self) -> str:
        """Base URL for dbt documentation server."""
        return settings.DBT_DOCS_URL

    def _load_manifest(self) -> dict[str, Any]:
        """Load and cache manifest.json.

        Returns:
            Parsed manifest dictionary.

        Raises:
            FileNotFoundError: If manifest.json doesn't exist.
        """
        if self._manifest is None:
            if not self.manifest_path.exists():
                raise FileNotFoundError(f"manifest.json not found at {self.manifest_path}. Run 'dbt docs generate' first.")
            self._manifest = json.loads(self.manifest_path.read_text())
        return self._manifest

    def _load_catalog(self) -> dict[str, Any]:
        """Load and cache catalog.json.

        Returns:
            Parsed catalog dictionary (empty dict if file doesn't exist).
        """
        if self._catalog is None:
            if self.catalog_path.exists():
                self._catalog = json.loads(self.catalog_path.read_text())
            else:
                self._catalog = {}
        return self._catalog

    def refresh_cache(self) -> None:
        """Force refresh of cached artifacts."""
        self._manifest = None
        self._catalog = None
        logger.info("dbt metadata cache refreshed")

    def get_docs_url(
        self,
        resource_type: str,
        name: str,
        source_name: str | None = None,
    ) -> str:
        """Generate dbt docs URL for a resource.

        Args:
            resource_type: Type of resource (model, source, overview, seed).
            name: Resource name.
            source_name: Source name (required for source type).

        Returns:
            Full URL to dbt documentation page.
        """
        base = self.docs_base_url.rstrip("/")

        match resource_type:
            case "overview":
                return f"{base}/#!/overview"
            case "model":
                return f"{base}/#!/model/model.quality_compass.{name}"
            case "source":
                return f"{base}/#!/source/source.quality_compass.{source_name}.{name}"
            case "seed":
                return f"{base}/#!/seed/seed.quality_compass.{name}"
            case _:
                return f"{base}/#!/overview"

    def get_all_models(self) -> list[ModelMetadata]:
        """Get metadata for all models.

        Returns:
            List of ModelMetadata for all models in the project.

        Raises:
            FileNotFoundError: If manifest.json doesn't exist.
        """
        manifest = self._load_manifest()
        catalog = self._load_catalog()
        models: list[ModelMetadata] = []

        for unique_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") != "model":
                continue

            # Get column info from catalog if available
            columns: list[ColumnMetadata] = []
            catalog_node = catalog.get("nodes", {}).get(unique_id, {})
            for col_name, col_info in catalog_node.get("columns", {}).items():
                columns.append(
                    ColumnMetadata(
                        name=col_name,
                        description=col_info.get("comment", ""),
                        data_type=col_info.get("type"),
                    )
                )

            # If no catalog, use schema columns
            if not columns:
                for col_name, col_info in node.get("columns", {}).items():
                    columns.append(
                        ColumnMetadata(
                            name=col_name,
                            description=col_info.get("description", ""),
                            data_type=None,
                        )
                    )

            # Get dependencies (upstream models)
            depends_on = [dep for dep in node.get("depends_on", {}).get("nodes", []) if dep.startswith("model.")]

            # Determine referenced_by (downstream models)
            referenced_by: list[str] = []
            for other_id, other_node in manifest.get("nodes", {}).items():
                if other_node.get("resource_type") == "model" and unique_id in other_node.get("depends_on", {}).get("nodes", []):
                    referenced_by.append(other_id)

            models.append(
                ModelMetadata(
                    unique_id=unique_id,
                    name=node["name"],
                    description=node.get("description", ""),
                    schema_name=node.get("schema", ""),
                    database=node.get("database", ""),
                    materialization=node.get("config", {}).get("materialized", "unknown"),
                    columns=columns,
                    tags=node.get("tags", []),
                    depends_on=depends_on,
                    referenced_by=referenced_by,
                    docs_url=self.get_docs_url("model", node["name"]),
                )
            )

        return models

    def get_model(self, model_name: str) -> ModelMetadata | None:
        """Get metadata for a specific model.

        Args:
            model_name: Name of the model (e.g., 'fct_signals').

        Returns:
            ModelMetadata or None if not found.

        Raises:
            FileNotFoundError: If manifest.json doesn't exist.
        """
        for model in self.get_all_models():
            if model.name == model_name:
                return model
        return None

    def get_lineage(self, model_name: str) -> LineageGraph | None:
        """Get full lineage graph for a model.

        Args:
            model_name: Name of the model.

        Returns:
            LineageGraph with upstream and downstream nodes, or None if not found.

        Raises:
            FileNotFoundError: If manifest.json doesn't exist.
        """
        manifest = self._load_manifest()
        nodes = manifest.get("nodes", {})
        sources = manifest.get("sources", {})

        # Find the target model
        target_id = f"model.quality_compass.{model_name}"
        if target_id not in nodes:
            return None

        target_node = nodes[target_id]

        # Build upstream lineage (recursive)
        upstream: list[LineageNode] = []
        visited: set[str] = set()

        def add_upstream(node_id: str) -> None:
            if node_id in visited:
                return
            visited.add(node_id)

            # Get node info based on type
            node: dict[str, Any] = {}
            resource_type = ""

            if node_id.startswith("model."):
                node = nodes.get(node_id, {})
                resource_type = "model"
            elif node_id.startswith("source."):
                node = sources.get(node_id, {})
                resource_type = "source"
            elif node_id.startswith("seed."):
                node = nodes.get(node_id, {})
                resource_type = "seed"
            else:
                return

            if not node:
                return

            # Determine layer from tags or naming convention
            tags = node.get("tags", [])
            name = node.get("name", node_id.split(".")[-1])

            if "staging" in tags or name.startswith("stg_"):
                layer = "staging"
            elif "marts" in tags or name.startswith(("fct_", "dim_")):
                layer = "marts"
            elif resource_type == "source":
                layer = "raw"
            else:
                layer = "other"

            # Add to upstream list
            upstream.append(
                LineageNode(
                    unique_id=node_id,
                    name=name,
                    resource_type=resource_type,
                    layer=layer,
                    docs_url=self.get_docs_url(resource_type, name),
                )
            )

            # Recurse to dependencies
            for dep_id in node.get("depends_on", {}).get("nodes", []):
                add_upstream(dep_id)

        # Start with direct dependencies
        for dep_id in target_node.get("depends_on", {}).get("nodes", []):
            add_upstream(dep_id)

        # Build downstream lineage
        downstream: list[LineageNode] = []
        for node_id, node in nodes.items():
            if node.get("resource_type") != "model":
                continue
            if target_id in node.get("depends_on", {}).get("nodes", []):
                tags = node.get("tags", [])
                name = node["name"]

                if "staging" in tags or name.startswith("stg_"):
                    layer = "staging"
                elif "marts" in tags or name.startswith(("fct_", "dim_")):
                    layer = "marts"
                else:
                    layer = "other"

                downstream.append(
                    LineageNode(
                        unique_id=node_id,
                        name=name,
                        resource_type="model",
                        layer=layer,
                        docs_url=self.get_docs_url("model", name),
                    )
                )

        return LineageGraph(
            target_model=model_name,
            upstream=upstream,
            downstream=downstream,
        )

    def get_summary(self) -> dict[str, Any]:
        """Get summary of dbt project metadata.

        Returns:
            Dictionary with project info, counts, and docs URL.

        Raises:
            FileNotFoundError: If manifest.json doesn't exist.
        """
        manifest = self._load_manifest()
        nodes = manifest.get("nodes", {})

        model_count = sum(1 for n in nodes.values() if n.get("resource_type") == "model")
        source_count = len(manifest.get("sources", {}))
        metric_count = len(manifest.get("metrics", []))
        semantic_model_count = len(manifest.get("semantic_models", []))

        return {
            "project_name": manifest.get("metadata", {}).get("project_name", "unknown"),
            "dbt_version": manifest.get("metadata", {}).get("dbt_version", "unknown"),
            "model_count": model_count,
            "source_count": source_count,
            "metric_count": metric_count,
            "semantic_model_count": semantic_model_count,
            "docs_base_url": self.docs_base_url,
            "generated_at": manifest.get("metadata", {}).get("generated_at"),
        }


# Singleton instance for dependency injection
_metadata_service: DbtMetadataService | None = None


def get_dbt_metadata_service() -> DbtMetadataService:
    """Get or create metadata service singleton.

    Returns:
        DbtMetadataService instance.
    """
    global _metadata_service
    if _metadata_service is None:
        _metadata_service = DbtMetadataService()
    return _metadata_service
