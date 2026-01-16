"""dbt Metadata API Router.

Exposes dbt model metadata, lineage, documentation URLs,
and semantic layer bundles for the Quality Compass dashboard.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.config import settings
from src.services.dbt_metadata_service import (
    DbtMetadataService,
    get_dbt_metadata_service,
)

router = APIRouter(prefix="/metadata", tags=["metadata"])


# =============================================================================
# Response Models
# =============================================================================


class ColumnResponse(BaseModel):
    """Column metadata response."""

    name: str = Field(..., description="Column name")
    description: str = Field(..., description="Column description from schema.yml")
    data_type: str | None = Field(None, description="Column data type from catalog")


class ModelResponse(BaseModel):
    """Model metadata response."""

    unique_id: str = Field(..., description="Unique model identifier (e.g., model.quality_compass.fct_signals)")
    name: str = Field(..., description="Model name (e.g., fct_signals)")
    description: str = Field(..., description="Model description from schema.yml")
    schema_name: str = Field(..., description="Database schema name")
    database: str = Field(..., description="Database name")
    materialization: str = Field(..., description="Materialization type (table, view, ephemeral)")
    columns: list[ColumnResponse] = Field(default_factory=list, description="Column metadata")
    tags: list[str] = Field(default_factory=list, description="Model tags")
    depends_on: list[str] = Field(default_factory=list, description="Upstream model dependencies")
    referenced_by: list[str] = Field(default_factory=list, description="Downstream models that depend on this")
    docs_url: str | None = Field(None, description="Deep-link URL to dbt docs")


class LineageNodeResponse(BaseModel):
    """Lineage node response."""

    unique_id: str = Field(..., description="Unique node identifier")
    name: str = Field(..., description="Node name")
    resource_type: str = Field(..., description="Resource type (model, source, seed)")
    layer: str = Field(..., description="Data layer (raw, staging, marts, other)")
    docs_url: str | None = Field(None, description="Deep-link URL to dbt docs")


class LineageResponse(BaseModel):
    """Full lineage response."""

    target_model: str = Field(..., description="Name of the target model")
    upstream: list[LineageNodeResponse] = Field(default_factory=list, description="Upstream dependencies (sources)")
    downstream: list[LineageNodeResponse] = Field(default_factory=list, description="Downstream dependents")


class SummaryResponse(BaseModel):
    """Project summary response."""

    project_name: str = Field(..., description="dbt project name")
    dbt_version: str = Field(..., description="dbt version used to generate artifacts")
    model_count: int = Field(..., description="Number of models in the project")
    source_count: int = Field(..., description="Number of sources defined")
    metric_count: int = Field(..., description="Number of metrics defined")
    semantic_model_count: int = Field(0, description="Number of semantic models defined")
    docs_base_url: str = Field(..., description="Base URL for dbt documentation server")
    generated_at: str | None = Field(None, description="Timestamp when artifacts were generated")


class DocsUrlResponse(BaseModel):
    """Documentation URL response."""

    resource_type: str = Field(..., description="Resource type (model, source, overview)")
    name: str = Field(..., description="Resource name")
    url: str = Field(..., description="Full URL to documentation")


# =============================================================================
# Dependency Injection
# =============================================================================

MetadataService = Annotated[DbtMetadataService, Depends(get_dbt_metadata_service)]


# =============================================================================
# Endpoints
# =============================================================================


@router.get(
    "/summary",
    response_model=SummaryResponse,
    summary="Get project summary",
    description="Returns summary of dbt project metadata including model counts and docs URL.",
)
async def get_summary(service: MetadataService) -> SummaryResponse:
    """Get summary of dbt project metadata including counts and docs URL."""
    try:
        summary = service.get_summary()
        return SummaryResponse(**summary)
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"dbt artifacts not found: {e}. Run 'dbt docs generate' first.",
        ) from e


@router.get(
    "/models",
    response_model=list[ModelResponse],
    summary="List all models",
    description="Returns metadata for all models, optionally filtered by layer or tag.",
)
async def list_models(
    service: MetadataService,
    layer: str | None = Query(None, description="Filter by layer (staging, marts)"),
    tag: str | None = Query(None, description="Filter by tag"),
) -> list[ModelResponse]:
    """Get metadata for all models, optionally filtered by layer or tag."""
    try:
        models = service.get_all_models()

        # Filter by layer based on naming convention
        if layer:
            if layer == "staging":
                models = [m for m in models if m.name.startswith("stg_")]
            elif layer == "marts":
                models = [m for m in models if m.name.startswith(("fct_", "dim_"))]
            elif layer == "semantic":
                models = [m for m in models if "semantic" in m.tags]

        # Filter by tag
        if tag:
            models = [m for m in models if tag in m.tags]

        return [
            ModelResponse(
                unique_id=m.unique_id,
                name=m.name,
                description=m.description,
                schema_name=m.schema_name,
                database=m.database,
                materialization=m.materialization,
                columns=[
                    ColumnResponse(
                        name=c.name,
                        description=c.description,
                        data_type=c.data_type,
                    )
                    for c in m.columns
                ],
                tags=m.tags,
                depends_on=m.depends_on,
                referenced_by=m.referenced_by,
                docs_url=m.docs_url,
            )
            for m in models
        ]
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"dbt artifacts not found: {e}. Run 'dbt docs generate' first.",
        ) from e


@router.get(
    "/models/{model_name}",
    response_model=ModelResponse,
    summary="Get model metadata",
    description="Returns metadata for a specific model by name.",
)
async def get_model(
    model_name: str,
    service: MetadataService,
) -> ModelResponse:
    """Get metadata for a specific model by name."""
    try:
        model = service.get_model(model_name)
        if model is None:
            raise HTTPException(
                status_code=404,
                detail=f"Model '{model_name}' not found",
            )

        return ModelResponse(
            unique_id=model.unique_id,
            name=model.name,
            description=model.description,
            schema_name=model.schema_name,
            database=model.database,
            materialization=model.materialization,
            columns=[
                ColumnResponse(
                    name=c.name,
                    description=c.description,
                    data_type=c.data_type,
                )
                for c in model.columns
            ],
            tags=model.tags,
            depends_on=model.depends_on,
            referenced_by=model.referenced_by,
            docs_url=model.docs_url,
        )
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"dbt artifacts not found: {e}. Run 'dbt docs generate' first.",
        ) from e


@router.get(
    "/models/{model_name}/lineage",
    response_model=LineageResponse,
    summary="Get model lineage",
    description="Returns full lineage graph (upstream and downstream) for a model.",
)
async def get_lineage(
    model_name: str,
    service: MetadataService,
) -> LineageResponse:
    """Get full lineage graph for a model (upstream and downstream)."""
    try:
        lineage = service.get_lineage(model_name)
        if lineage is None:
            raise HTTPException(
                status_code=404,
                detail=f"Model '{model_name}' not found",
            )

        return LineageResponse(
            target_model=lineage.target_model,
            upstream=[
                LineageNodeResponse(
                    unique_id=n.unique_id,
                    name=n.name,
                    resource_type=n.resource_type,
                    layer=n.layer,
                    docs_url=n.docs_url,
                )
                for n in lineage.upstream
            ],
            downstream=[
                LineageNodeResponse(
                    unique_id=n.unique_id,
                    name=n.name,
                    resource_type=n.resource_type,
                    layer=n.layer,
                    docs_url=n.docs_url,
                )
                for n in lineage.downstream
            ],
        )
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"dbt artifacts not found: {e}. Run 'dbt docs generate' first.",
        ) from e


@router.get(
    "/docs-url/{resource_type}/{name}",
    response_model=DocsUrlResponse,
    summary="Get documentation URL",
    description="Returns the dbt docs URL for a specific resource.",
)
async def get_docs_url(
    resource_type: str,
    name: str,
    service: MetadataService,
) -> DocsUrlResponse:
    """Get the dbt docs URL for a specific resource."""
    url = service.get_docs_url(resource_type, name)
    return DocsUrlResponse(
        resource_type=resource_type,
        name=name,
        url=url,
    )


@router.post(
    "/refresh",
    summary="Refresh metadata cache",
    description="Force refresh of cached dbt artifacts. Use after running 'dbt docs generate'.",
)
async def refresh_cache(service: MetadataService) -> dict[str, str]:
    """Force refresh of cached dbt artifacts."""
    service.refresh_cache()
    return {"status": "refreshed", "message": "dbt metadata cache has been refreshed"}


# =============================================================================
# Semantic Layer Bundle Models
# =============================================================================


class MetricBundleEntry(BaseModel):
    """Metric semantic entry from taxonomy."""

    metric_id: str = Field(..., description="Canonical metric identifier")
    display_name: str = Field(..., description="Human-readable name for UI")
    description: str = Field(..., description="Explanation of the metric")
    domain: str = Field("", description="Business domain (Efficiency, Safety, etc.)")
    polarity: str = Field("", description="Optimal direction (lower_is_better, higher_is_better, neutral)")
    unit_type: str = Field("", description="Type of unit (ratio, percentage, days, etc.)")
    display_format: str = Field("", description="Python format string for display")


class EdgeTypeBundleEntry(BaseModel):
    """Edge type semantic entry from taxonomy."""

    id: str = Field(..., description="Canonical edge type identifier")
    display_name: str = Field(..., description="Human-readable name for UI")
    description: str = Field(..., description="Explanation of the edge type")
    ui_behavior: str = Field("", description="UI behavior hint (slide_right, expand, etc.)")
    direction: str = Field("", description="Edge direction (forward, bidirectional, etc.)")
    category: str = Field("", description="Category (hierarchical, temporal, analytical, causal)")


class TagTypeBundleEntry(BaseModel):
    """Tag type semantic entry from taxonomy."""

    id: str = Field(..., description="Canonical tag identifier")
    display_name: str = Field(..., description="Human-readable name for UI")
    category: str = Field(..., description="Tag category")
    description: str = Field(..., description="Explanation of the tag type")
    ui_filter_ids: list[str] = Field(default_factory=list, description="Associated UI filter identifiers")


class ComparisonModeBundleEntry(BaseModel):
    """Comparison mode semantic entry from taxonomy."""

    id: str = Field(..., description="Canonical comparison mode identifier")
    display_name: str = Field(..., description="Human-readable name for UI")
    description: str = Field(..., description="Explanation of the comparison mode")
    baseline_description: str = Field("", description="Description of baseline")
    authoring_hint: str = Field("", description="Authoring hint for the mode")


class GroupByAllowedValueBundleEntry(BaseModel):
    """Allowed value for a group-by type dimension."""

    code: str = Field(..., description="Code value for data matching")
    identifier: str = Field(..., description="Identifier for programmatic access")
    label: str = Field(..., description="Human-readable label")


class GroupByTypeBundleEntry(BaseModel):
    """Group-by type semantic entry from taxonomy."""

    id: str = Field(..., description="Canonical group-by type identifier")
    display_name: str = Field(..., description="Human-readable name for UI")
    category: str = Field(..., description="Dimension category")
    dataset_field: str = Field(..., description="Field name in dataset")
    description: str = Field(..., description="Explanation of the dimension")
    allowed_values: list[GroupByAllowedValueBundleEntry] = Field(default_factory=list, description="Allowed values for the dimension")


class GroupBySetBundleEntry(BaseModel):
    """Group-by set semantic entry from taxonomy."""

    id: str = Field(..., description="Canonical group-by set identifier")
    description: str = Field(..., description="Explanation of the dimension set")
    allowed_metric_method_sets: list[str] = Field(default_factory=list, description="Allowed metric method sets")
    allowed_dimensions: list[list[str]] = Field(default_factory=list, description="Allowed dimension combinations")
    optional_time_dimensions: list[str] = Field(default_factory=list, description="Optional time dimensions")


class MetadataBundleResponse(BaseModel):
    """Complete semantic metadata bundle."""

    version: str = Field(..., description="Bundle format version")
    generated_at: str = Field(..., description="ISO timestamp when bundle was generated")
    taxonomy_hash: str = Field(..., description="Hash of source taxonomy files")
    metrics: list[MetricBundleEntry] = Field(default_factory=list, description="Metric semantic entries")
    edge_types: list[EdgeTypeBundleEntry] = Field(default_factory=list, description="Edge type semantic entries")
    tag_types: list[TagTypeBundleEntry] = Field(default_factory=list, description="Tag type semantic entries")
    comparison_modes: list[ComparisonModeBundleEntry] = Field(default_factory=list, description="Comparison mode semantic entries")
    group_by_types: list[GroupByTypeBundleEntry] = Field(default_factory=list, description="Group-by type semantic entries")
    group_by_sets: list[GroupBySetBundleEntry] = Field(default_factory=list, description="Group-by set semantic entries")


class RunManifestSemanticResponse(BaseModel):
    """Semantic manifest for the current insight graph run."""

    run_id: str = Field(..., description="Unique run identifier")
    graph_name: str = Field(..., description="Name of the insight graph")
    requirements_path: str = Field(..., description="Path to requirements document")
    taxonomy_hash: str = Field(..., description="Hash of taxonomy files at run time")
    generated_at: str = Field(..., description="ISO timestamp when manifest was generated")
    metadata_bundle_version: str = Field(..., description="Version of metadata bundle format")
    modeling: dict[str, str] = Field(default_factory=dict, description="Modeling artifact paths if present")


# =============================================================================
# Semantic Layer Bundle Helpers
# =============================================================================


def _compute_taxonomy_hash(taxonomy_path: Path) -> str:
    """Compute hash of taxonomy files for versioning."""
    hasher = hashlib.sha256()
    yaml_files = sorted(taxonomy_path.glob("*.yaml"))
    for yaml_file in yaml_files:
        content = yaml_file.read_bytes()
        hasher.update(yaml_file.name.encode("utf-8"))
        hasher.update(content)
    return hasher.hexdigest()[:16]


def _load_metrics_from_taxonomy(taxonomy_path: Path) -> list[MetricBundleEntry]:
    """Load metrics from taxonomy/metrics.yaml."""
    metrics_file = taxonomy_path / "metrics.yaml"
    if not metrics_file.exists():
        return []

    with metrics_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict) or "entries" not in data:
        return []

    entries: list[MetricBundleEntry] = []
    for item in data.get("entries", []):
        if not isinstance(item, dict):
            continue
        entries.append(
            MetricBundleEntry(
                metric_id=item.get("metric_id", ""),
                display_name=item.get("display_name", ""),
                description=str(item.get("description", "")).strip(),
                domain=item.get("domain", ""),
                polarity=item.get("polarity", ""),
                unit_type=item.get("unit_type", ""),
                display_format=item.get("display_format", ""),
            )
        )
    return sorted(entries, key=lambda e: e.metric_id)


def _load_edge_types_from_taxonomy(taxonomy_path: Path) -> list[EdgeTypeBundleEntry]:
    """Load edge types from taxonomy/edge_types.yaml."""
    edge_types_file = taxonomy_path / "edge_types.yaml"
    if not edge_types_file.exists():
        return []

    with edge_types_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict) or "entries" not in data:
        return []

    entries: list[EdgeTypeBundleEntry] = []
    for item in data.get("entries", []):
        if not isinstance(item, dict):
            continue
        entries.append(
            EdgeTypeBundleEntry(
                id=item.get("id", ""),
                display_name=item.get("display_name", ""),
                description=str(item.get("description", "")).strip(),
                ui_behavior=item.get("ui_behavior", ""),
                direction=item.get("direction", ""),
                category=item.get("category", ""),
            )
        )
    return sorted(entries, key=lambda e: e.id)


def _load_tag_types_from_taxonomy(taxonomy_path: Path) -> list[TagTypeBundleEntry]:
    """Load tag types from taxonomy/tag_types.yaml."""
    tag_types_file = taxonomy_path / "tag_types.yaml"
    if not tag_types_file.exists():
        return []

    with tag_types_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict) or "tags" not in data:
        return []

    entries: list[TagTypeBundleEntry] = []
    for item in data.get("tags", []):
        if not isinstance(item, dict):
            continue
        entries.append(
            TagTypeBundleEntry(
                id=item.get("id", ""),
                display_name=item.get("display_name", ""),
                category=item.get("category", ""),
                description=str(item.get("description", "")).strip(),
                ui_filter_ids=item.get("ui_filter_ids", []),
            )
        )
    return sorted(entries, key=lambda e: e.id)


def _load_comparison_modes_from_taxonomy(taxonomy_path: Path) -> list[ComparisonModeBundleEntry]:
    """Load comparison modes from taxonomy/comparison_modes.yaml."""
    comparison_modes_file = taxonomy_path / "comparison_modes.yaml"
    if not comparison_modes_file.exists():
        return []

    with comparison_modes_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict) or "modes" not in data:
        return []

    entries: list[ComparisonModeBundleEntry] = []
    for item in data.get("modes", []):
        if not isinstance(item, dict):
            continue
        entries.append(
            ComparisonModeBundleEntry(
                id=item.get("id", ""),
                display_name=item.get("display_name", ""),
                description=str(item.get("description", "")).strip(),
                baseline_description=item.get("baseline_description", ""),
                authoring_hint=item.get("authoring_hint", ""),
            )
        )
    return sorted(entries, key=lambda e: e.id)


def _load_group_by_types_from_taxonomy(taxonomy_path: Path) -> list[GroupByTypeBundleEntry]:
    """Load group-by types from taxonomy/group_by_types.yaml."""
    group_by_types_file = taxonomy_path / "group_by_types.yaml"
    if not group_by_types_file.exists():
        return []

    with group_by_types_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict) or "entries" not in data:
        return []

    entries: list[GroupByTypeBundleEntry] = []
    for item in data.get("entries", []):
        if not isinstance(item, dict):
            continue
        allowed_values: list[GroupByAllowedValueBundleEntry] = []
        for value in item.get("allowed_values", []):
            if not isinstance(value, dict):
                continue
            allowed_values.append(
                GroupByAllowedValueBundleEntry(
                    code=value.get("code", ""),
                    identifier=value.get("identifier", ""),
                    label=value.get("label", ""),
                )
            )
        entries.append(
            GroupByTypeBundleEntry(
                id=item.get("id", ""),
                display_name=item.get("display_name", ""),
                category=item.get("category", ""),
                dataset_field=item.get("dataset_field", ""),
                description=str(item.get("description", "")).strip(),
                allowed_values=allowed_values,
            )
        )
    return sorted(entries, key=lambda e: e.id)


def _load_group_by_sets_from_taxonomy(taxonomy_path: Path) -> list[GroupBySetBundleEntry]:
    """Load group-by sets from taxonomy/group_by_sets.yaml."""
    group_by_sets_file = taxonomy_path / "group_by_sets.yaml"
    if not group_by_sets_file.exists():
        return []

    with group_by_sets_file.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict) or "sets" not in data:
        return []

    entries: list[GroupBySetBundleEntry] = []
    for item in data.get("sets", []):
        if not isinstance(item, dict):
            continue
        entries.append(
            GroupBySetBundleEntry(
                id=item.get("id", ""),
                description=str(item.get("description", "")).strip(),
                allowed_metric_method_sets=item.get("allowed_metric_method_sets", []),
                allowed_dimensions=item.get("allowed_dimensions", []),
                optional_time_dimensions=item.get("optional_time_dimensions", []),
            )
        )
    return sorted(entries, key=lambda e: e.id)


def _load_bundle_from_run(run_path: Path) -> MetadataBundleResponse | None:
    """Load metadata_bundle.json from a run directory if present."""
    bundle_path = run_path / "metadata_bundle.json"
    if not bundle_path.exists():
        return None
    with bundle_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return MetadataBundleResponse(**payload)


# =============================================================================
# Semantic Layer Bundle Endpoints
# =============================================================================


@router.get(
    "/bundle",
    response_model=MetadataBundleResponse,
    summary="Get semantic metadata bundle",
    description="Returns the complete semantic metadata bundle derived from taxonomy files. Use for UI tooltips and semantic lookups.",
)
async def get_metadata_bundle() -> MetadataBundleResponse:
    """Get the complete semantic metadata bundle.

    Generates the bundle from taxonomy files at request time.
    The bundle contains semantic metadata for metrics, edge types, tags,
    comparison modes, and group-by definitions.
    """
    run_path = Path(settings.RUNS_ROOT) / settings.INSIGHT_GRAPH_RUN
    if run_path.exists():
        bundle = _load_bundle_from_run(run_path)
        if bundle is not None:
            return bundle

    taxonomy_path = Path(settings.TAXONOMY_PATH)

    if not taxonomy_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Taxonomy directory not found: {settings.TAXONOMY_PATH}. Ensure taxonomy is mounted.",
        )

    return MetadataBundleResponse(
        version="1.0.0",
        generated_at=datetime.now(UTC).isoformat(),
        taxonomy_hash=_compute_taxonomy_hash(taxonomy_path),
        metrics=_load_metrics_from_taxonomy(taxonomy_path),
        edge_types=_load_edge_types_from_taxonomy(taxonomy_path),
        tag_types=_load_tag_types_from_taxonomy(taxonomy_path),
        comparison_modes=_load_comparison_modes_from_taxonomy(taxonomy_path),
        group_by_types=_load_group_by_types_from_taxonomy(taxonomy_path),
        group_by_sets=_load_group_by_sets_from_taxonomy(taxonomy_path),
    )


@router.get(
    "/run-manifest",
    response_model=RunManifestSemanticResponse,
    summary="Get current run semantic manifest",
    description="Returns the semantic manifest for the currently configured insight graph run.",
)
async def get_run_manifest_semantic() -> RunManifestSemanticResponse:
    """Get the semantic manifest for the current insight graph run.

    Returns metadata linking the run to its semantic context.
    """
    taxonomy_path = Path(settings.TAXONOMY_PATH)
    runs_root = Path(settings.RUNS_ROOT)
    run_relative = settings.INSIGHT_GRAPH_RUN

    run_path = runs_root / run_relative
    run_id = run_path.name if run_path.exists() else "unknown"
    graph_name = run_path.parent.name if run_path.exists() else "unknown"

    # Detect modeling artifacts
    modeling: dict[str, str] = {}
    if run_path.exists():
        modeling_dir = run_path / "modeling"
        if modeling_dir.exists():
            run_summary = modeling_dir / "run_summary.json"
            if run_summary.exists():
                modeling["run_summary_path"] = str(run_summary.relative_to(run_path))

            experiments = modeling_dir / "experiments.json"
            if experiments.exists():
                modeling["experiments_manifest_path"] = str(experiments.relative_to(run_path))

    taxonomy_hash = ""
    if taxonomy_path.exists():
        taxonomy_hash = _compute_taxonomy_hash(taxonomy_path)

    # Prefer reading from semantic manifest if it exists, fall back to generating response
    semantic_manifest_file = run_path / "run_manifest.semantic.json"
    if semantic_manifest_file.exists():
        try:
            with semantic_manifest_file.open("r", encoding="utf-8") as handle:
                manifest_data = json.load(handle)
                # Read all fields from the persisted semantic manifest
                return RunManifestSemanticResponse(
                    run_id=manifest_data.get("run_id", run_id),
                    graph_name=manifest_data.get("graph_name", graph_name),
                    requirements_path=manifest_data.get("requirements_path", ""),
                    taxonomy_hash=manifest_data.get("taxonomy_hash", taxonomy_hash),
                    generated_at=manifest_data.get("generated_at", datetime.now(UTC).isoformat()),
                    metadata_bundle_version=manifest_data.get("metadata_bundle_version", "1.0.0"),
                    modeling=manifest_data.get("modeling", modeling),
                )
        except (json.JSONDecodeError, OSError, KeyError):
            pass  # Fall through to generate response

    # Fallback: generate response from available data
    return RunManifestSemanticResponse(
        run_id=run_id,
        graph_name=graph_name,
        requirements_path="",
        taxonomy_hash=taxonomy_hash,
        generated_at=datetime.now(UTC).isoformat(),
        metadata_bundle_version="1.0.0",
        modeling=modeling,
    )
