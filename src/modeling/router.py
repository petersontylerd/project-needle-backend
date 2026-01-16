"""Modeling router - ML model performance and driver analysis endpoints.

Exposes modeling experiment results and feature driver analysis via REST API.
Reads data from dbt marts built from modeling run outputs.

Endpoints:
- GET /modeling/summary - Returns model run summary with metrics
- GET /modeling/drivers - Returns top feature drivers with SHAP values
- GET /modeling/experiments - Returns all experiments with performance metrics
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db.session import get_async_db_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/modeling", tags=["modeling"])


# ============================================
# Helper Functions
# ============================================


def load_shap_drivers_from_csv(shap_path: str, top_n: int) -> list[FeatureDriver]:
    """Load top feature drivers from a SHAP CSV file.

    Reads a global SHAP summary CSV with columns:
    - feature: feature name
    - mean_abs_shap: absolute mean SHAP value (for ranking)
    - mean_shap: signed mean SHAP value (for direction)
    - count: observation count

    Args:
        shap_path: Relative path to SHAP CSV within RUNS_ROOT.
        top_n: Number of top drivers to return.

    Returns:
        List of FeatureDriver objects sorted by importance.
        Empty list if file not found or on error.
    """
    runs_root = Path(settings.RUNS_ROOT).expanduser().resolve()
    relative_path = Path(shap_path)
    if relative_path.is_absolute():
        logger.warning("SHAP path is absolute and will be ignored: %s", shap_path)
        return []
    full_path = (runs_root / relative_path).resolve()
    if runs_root not in full_path.parents and full_path != runs_root:
        logger.warning("SHAP path escapes RUNS_ROOT: %s", full_path)
        return []

    if not full_path.exists():
        logger.warning("SHAP file not found: %s", full_path)
        return []

    try:
        with full_path.open(newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)

        # Validate required columns exist
        if not rows:
            return []
        required_columns = {"feature", "mean_abs_shap", "mean_shap"}
        if not required_columns.issubset(rows[0].keys()):
            logger.warning("SHAP CSV missing required columns: %s", full_path)
            return []

        # Sort by mean_abs_shap descending
        rows.sort(key=lambda r: float(r.get("mean_abs_shap", 0)), reverse=True)

        # Take top N and build driver list
        drivers: list[FeatureDriver] = []
        for rank, row in enumerate(rows[:top_n], start=1):
            mean_shap = float(row.get("mean_shap", 0))
            drivers.append(
                FeatureDriver(
                    feature_name=row.get("feature", "unknown"),
                    shap_value=float(row.get("mean_abs_shap", 0)),
                    direction="positive" if mean_shap >= 0 else "negative",
                    rank=rank,
                )
            )

        return drivers
    except (OSError, ValueError, KeyError) as e:
        logger.exception("Failed to load SHAP file %s: %s", full_path, e)
        return []


# ============================================
# Response Models
# ============================================


class ModelingRunSummary(BaseModel):
    """Summary of a modeling run."""

    run_id: str
    status: str
    config_path: str | None = None
    groups: list[str] = Field(default_factory=list)
    duration_seconds: float | None = None


class ModelingExperiment(BaseModel):
    """Details of a single modeling experiment."""

    run_id: str
    estimator: str
    groupby_label: str | None = None
    group_value: str | None = None
    row_count: int | None = None
    duration_seconds: float | None = None

    # Primary metrics (transformed scale)
    mae: float | None = None
    rmse: float | None = None
    r2: float | None = None

    # Original scale metrics
    mae_original: float | None = None
    rmse_original: float | None = None
    r2_original: float | None = None

    # Artifact paths for interpretability
    global_shap_path: str | None = None
    facility_shap_path: str | None = None
    encounter_shap_path: str | None = None

    experiment_rank: int | None = None


class FeatureDriver(BaseModel):
    """A feature driver with SHAP importance."""

    feature_name: str
    shap_value: float
    direction: str = Field(description="Direction of impact: 'positive' increases prediction, 'negative' decreases")
    rank: int


class ModelDriversResponse(BaseModel):
    """Response containing top feature drivers."""

    run_id: str
    estimator: str
    drivers: list[FeatureDriver]
    top_n: int


class ModelingSummaryResponse(BaseModel):
    """Response containing modeling run summary."""

    run: ModelingRunSummary | None = None
    experiments: list[ModelingExperiment] = Field(default_factory=list)
    experiment_count: int = 0
    best_experiment: ModelingExperiment | None = None


class ExperimentsListResponse(BaseModel):
    """Response containing all experiments."""

    experiments: list[ModelingExperiment]
    count: int


# ============================================
# API Endpoints
# ============================================


@router.get("/summary", response_model=ModelingSummaryResponse)
async def get_modeling_summary(
    db: Annotated[AsyncSession, Depends(get_async_db_session)],
    run_id: str | None = Query(None, description="Filter by run ID"),
) -> ModelingSummaryResponse:
    """Get modeling run summary with key metrics.

    Returns summary of the latest modeling run including configuration,
    status, and best-performing experiment.

    Args:
        db: Database session (injected).
        run_id: Optional filter for specific run ID.

    Returns:
        ModelingSummaryResponse with run details and experiments.
    """
    # Get run summary from raw_modeling_runs
    # Build WHERE clause conditionally to avoid asyncpg type inference issues with NULL
    if run_id:
        run_query = """
            SELECT run_id, config_path, status, groups, run_dir, duration_seconds
            FROM raw_modeling_runs
            WHERE run_id = :run_id
            ORDER BY loaded_at DESC
            LIMIT 1
        """
        run_result = await db.execute(text(run_query), {"run_id": run_id})
    else:
        run_query = """
            SELECT run_id, config_path, status, groups, run_dir, duration_seconds
            FROM raw_modeling_runs
            ORDER BY loaded_at DESC
            LIMIT 1
        """
        run_result = await db.execute(text(run_query))
    run_row = run_result.fetchone()

    run_summary: ModelingRunSummary | None = None
    if run_row:
        import json

        groups_raw = run_row.groups
        groups_list: list[str] = []
        if groups_raw:
            try:
                groups_list = json.loads(groups_raw) if isinstance(groups_raw, str) else groups_raw
            except (json.JSONDecodeError, TypeError):
                groups_list = []

        run_summary = ModelingRunSummary(
            run_id=run_row.run_id,
            status=run_row.status or "unknown",
            config_path=run_row.config_path,
            groups=groups_list,
            duration_seconds=run_row.duration_seconds,
        )

    # Get experiments from fct_model_drivers (if it exists) or fallback to raw
    # Build WHERE clause conditionally for asyncpg compatibility
    base_exp_query = """
        SELECT run_id, estimator, groupby_label, group_value, row_count,
               duration_seconds, mae, rmse, r2, mae_original, rmse_original,
               r2_original, global_shap_path, facility_shap_path,
               encounter_shap_path, experiment_rank
        FROM public_marts.fct_model_drivers
    """
    if run_id:
        experiments_query = base_exp_query + " WHERE run_id = :run_id ORDER BY experiment_rank ASC NULLS LAST, r2 DESC NULLS LAST"
        exp_params: dict[str, str] = {"run_id": run_id}
    else:
        experiments_query = base_exp_query + " ORDER BY experiment_rank ASC NULLS LAST, r2 DESC NULLS LAST"
        exp_params = {}

    experiments: list[ModelingExperiment] = []
    best_experiment: ModelingExperiment | None = None

    try:
        exp_result = await db.execute(text(experiments_query), exp_params)
        for row in exp_result.fetchall():
            exp = ModelingExperiment(
                run_id=row.run_id,
                estimator=row.estimator or "unknown",
                groupby_label=row.groupby_label,
                group_value=row.group_value,
                row_count=row.row_count,
                duration_seconds=row.duration_seconds,
                mae=row.mae,
                rmse=row.rmse,
                r2=row.r2,
                mae_original=row.mae_original,
                rmse_original=row.rmse_original,
                r2_original=row.r2_original,
                global_shap_path=row.global_shap_path,
                facility_shap_path=row.facility_shap_path,
                encounter_shap_path=row.encounter_shap_path,
                experiment_rank=row.experiment_rank,
            )
            experiments.append(exp)

            # Best experiment is the one with rank 1 or highest R²
            if row.experiment_rank == 1 or (best_experiment is None and row.r2 is not None):
                best_experiment = exp
    except Exception:
        # fct_model_drivers table may not exist yet, return empty
        pass

    return ModelingSummaryResponse(
        run=run_summary,
        experiments=experiments,
        experiment_count=len(experiments),
        best_experiment=best_experiment,
    )


@router.get("/experiments", response_model=ExperimentsListResponse)
async def list_experiments(
    db: Annotated[AsyncSession, Depends(get_async_db_session)],
    run_id: str | None = Query(None, description="Filter by run ID"),
    estimator: str | None = Query(None, description="Filter by estimator type"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
) -> ExperimentsListResponse:
    """List all modeling experiments with performance metrics.

    Returns all experiments optionally filtered by run ID or estimator type.

    Args:
        db: Database session (injected).
        run_id: Optional filter for specific run ID.
        estimator: Optional filter for estimator type (e.g., 'ridge', 'xgboost').
        limit: Maximum number of results (default 100).

    Returns:
        ExperimentsListResponse with experiment list.
    """
    # Build query conditionally to avoid asyncpg type inference issues
    base_query = """
        SELECT run_id, estimator, groupby_label, group_value, row_count,
               duration_seconds, mae, rmse, r2, mae_original, rmse_original,
               r2_original, global_shap_path, facility_shap_path,
               encounter_shap_path, experiment_rank
        FROM public_marts.fct_model_drivers
    """
    conditions: list[str] = []
    params: dict[str, str | int] = {"limit": limit}
    if run_id:
        conditions.append("run_id = :run_id")
        params["run_id"] = run_id
    if estimator:
        conditions.append("estimator = :estimator")
        params["estimator"] = estimator

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    query = base_query + where_clause + " ORDER BY experiment_rank ASC NULLS LAST, r2 DESC NULLS LAST LIMIT :limit"

    experiments: list[ModelingExperiment] = []

    try:
        result = await db.execute(text(query), params)
        for row in result.fetchall():
            experiments.append(
                ModelingExperiment(
                    run_id=row.run_id,
                    estimator=row.estimator or "unknown",
                    groupby_label=row.groupby_label,
                    group_value=row.group_value,
                    row_count=row.row_count,
                    duration_seconds=row.duration_seconds,
                    mae=row.mae,
                    rmse=row.rmse,
                    r2=row.r2,
                    mae_original=row.mae_original,
                    rmse_original=row.rmse_original,
                    r2_original=row.r2_original,
                    global_shap_path=row.global_shap_path,
                    facility_shap_path=row.facility_shap_path,
                    encounter_shap_path=row.encounter_shap_path,
                    experiment_rank=row.experiment_rank,
                )
            )
    except Exception:
        # Table may not exist yet
        pass

    return ExperimentsListResponse(experiments=experiments, count=len(experiments))


@router.get("/drivers", response_model=ModelDriversResponse)
async def get_model_drivers(
    db: Annotated[AsyncSession, Depends(get_async_db_session)],
    run_id: str | None = Query(None, description="Filter by run ID"),
    estimator: str | None = Query(None, description="Filter by estimator type"),
    top_n: int = Query(10, ge=1, le=50, description="Number of top drivers to return"),
) -> ModelDriversResponse:
    """Get top feature drivers with SHAP importance values.

    Returns the most important features driving model predictions,
    based on SHAP analysis from the best-performing experiment.
    Loads SHAP values from CSV artifact files stored during modeling runs.

    Args:
        db: Database session (injected).
        run_id: Optional filter for specific run ID.
        estimator: Optional filter for estimator type.
        top_n: Number of top drivers to return (default 10).

    Returns:
        ModelDriversResponse with top feature drivers sorted by importance.
    """
    # Build query conditionally to avoid asyncpg type inference issues
    base_query = """
        SELECT run_id, estimator, global_shap_path
        FROM public_marts.fct_model_drivers
        WHERE global_shap_path IS NOT NULL
    """
    conditions: list[str] = []
    params: dict[str, str] = {}
    if run_id:
        conditions.append("run_id = :run_id")
        params["run_id"] = run_id
    if estimator:
        conditions.append("estimator = :estimator")
        params["estimator"] = estimator

    extra_conditions = " AND " + " AND ".join(conditions) if conditions else ""
    query = base_query + extra_conditions + " ORDER BY experiment_rank ASC NULLS LAST, r2 DESC NULLS LAST LIMIT 1"

    actual_run_id = run_id or "unknown"
    actual_estimator = estimator or "unknown"
    drivers: list[FeatureDriver] = []

    try:
        result = await db.execute(text(query), params)
        row = result.fetchone()

        if row:
            actual_run_id = row.run_id
            actual_estimator = row.estimator or "unknown"

            # Load actual SHAP values from global_shap_path
            if row.global_shap_path:
                drivers = load_shap_drivers_from_csv(row.global_shap_path, top_n)
    except Exception:
        logger.exception("Failed to load model drivers")

    return ModelDriversResponse(
        run_id=actual_run_id,
        estimator=actual_estimator,
        drivers=drivers,
        top_n=top_n,
    )
