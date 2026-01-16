#!/usr/bin/env python3
"""Load modeling run outputs into PostgreSQL raw tables for dbt processing.

This script discovers and loads data from the modeling runs
into PostgreSQL staging tables that dbt transforms into mart models.

Data Sources:
    - Run summary: JSON file from runs/modeling/modeling/run_summary.json
    - Experiments: JSON file from runs/modeling/modeling/experiments.json

Tables Created (in 'public' schema):
    - raw_modeling_runs: Run-level summary with config, status, duration
    - raw_modeling_experiments: Experiment-level details with metrics and artifact paths

Usage:
    # Load all data (default paths from environment/config)
    UV_CACHE_DIR=../.uv-cache uv run python scripts/load_modeling_to_dbt.py

    # Load with custom paths
    UV_CACHE_DIR=../.uv-cache uv run python scripts/load_modeling_to_dbt.py \
        --runs-root /path/to/runs \
        --modeling-run modeling/my_run

Environment Variables:
    DATABASE_URL: PostgreSQL connection string (default: see config.py)
    RUNS_ROOT: Root directory for run outputs
    MODELING_RUN: Relative path to modeling run

Idempotency:
    - Uses TRUNCATE before loading to ensure clean state
    - Re-running the script replaces all data with fresh load

Author: Quality Compass Team
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# Docker container path prefix that should be converted to relative paths.
# When modeling runs in Docker, RUNS_ROOT is mounted to /data/runs.
# Artifact paths are written as absolute container paths, but the API expects
# relative paths that can be joined with RUNS_ROOT.
DOCKER_RUNS_PREFIX = "/data/runs/"


def normalize_artifact_path(path: str | None) -> str | None:
    """Convert Docker container absolute paths to relative paths.

    When the modeling pipeline runs inside Docker, it writes artifact paths
    as absolute container paths (e.g., /data/runs/<graph>/<run_id>/...).
    The API expects relative paths that can be joined with RUNS_ROOT.

    This function strips the /data/runs/ prefix to make paths relative.

    Args:
        path: Absolute container path or None.

    Returns:
        Relative path (stripped of prefix) or None if input was None/empty.

    Examples:
        >>> normalize_artifact_path("/data/runs/test_minimal/20260101/file.csv")
        'test_minimal/20260101/file.csv'
        >>> normalize_artifact_path("already/relative/path.csv")
        'already/relative/path.csv'
        >>> normalize_artifact_path(None)
        None
    """
    if not path:
        return None
    if path.startswith(DOCKER_RUNS_PREFIX):
        return path[len(DOCKER_RUNS_PREFIX) :]
    return path


def normalize_experiment_paths(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize all artifact paths in an experiment entry.

    Converts Docker container absolute paths to relative paths in:
    - Top-level artifact paths (config_path, dataset_path, etc.)
    - Nested artifacts.* paths
    - Nested artifacts.interpretability.* paths

    Args:
        item: Raw experiment dictionary from experiments.json.

    Returns:
        New dictionary with normalized paths (original is not modified).
    """
    result = dict(item)

    # Normalize top-level paths
    for key in ["config_path", "dataset_path"]:
        if key in result:
            result[key] = normalize_artifact_path(result.get(key))

    # Normalize artifacts section
    if "artifacts" in result and isinstance(result["artifacts"], dict):
        artifacts = dict(result["artifacts"])
        for key in [
            "metrics",
            "predictions",
            "eda_summary",
            "group_summary",
            "feature_descriptor",
            "hparam_trials",
        ]:
            if key in artifacts:
                artifacts[key] = normalize_artifact_path(artifacts.get(key))

        # Normalize interpretability sub-section
        if "interpretability" in artifacts and isinstance(artifacts["interpretability"], dict):
            interp = dict(artifacts["interpretability"])
            for key in ["encounter", "facility", "global_summary", "transparency"]:
                if key in interp:
                    interp[key] = normalize_artifact_path(interp.get(key))
            artifacts["interpretability"] = interp

        result["artifacts"] = artifacts

    return result


# Table DDL definitions
RAW_MODELING_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS raw_modeling_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    config_path TEXT,
    status VARCHAR(50),
    groups TEXT,
    run_dir TEXT,
    duration_seconds FLOAT,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_modeling_runs_run_id ON raw_modeling_runs(run_id);
"""

RAW_MODELING_EXPERIMENTS_DDL = """
CREATE TABLE IF NOT EXISTS raw_modeling_experiments (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    json_data TEXT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_modeling_experiments_run_id ON raw_modeling_experiments(run_id);
"""


def get_sync_database_url(async_url: str) -> str:
    """Convert async database URL to sync URL for pandas/SQLAlchemy.

    Args:
        async_url: PostgreSQL URL with asyncpg driver.

    Returns:
        PostgreSQL URL with psycopg2 driver for sync operations.
    """
    return async_url.replace("postgresql+asyncpg://", "postgresql://")


def create_tables(engine: Engine) -> None:
    """Create raw tables if they don't exist.

    Args:
        engine: SQLAlchemy engine for database connection.
    """
    logger.info("Creating raw modeling tables if not exists...")
    with engine.connect() as conn:
        for ddl in [
            RAW_MODELING_RUNS_DDL,
            RAW_MODELING_EXPERIMENTS_DDL,
        ]:
            conn.execute(text(ddl))
        conn.commit()
    logger.info("Tables created/verified.")


def truncate_table(engine: Engine, table_name: str) -> None:
    """Truncate a table for clean reload.

    Args:
        engine: SQLAlchemy engine.
        table_name: Name of table to truncate.
    """
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))
        conn.commit()
    logger.info(f"Truncated table: {table_name}")


def extract_run_id(modeling_path: Path) -> str:
    """Extract run_id from modeling path.

    Args:
        modeling_path: Path to the modeling directory.

    Returns:
        Run identifier string (directory name).
    """
    return modeling_path.name


def load_run_summary(engine: Engine, modeling_path: Path, run_id: str) -> int:
    """Load run_summary.json into raw_modeling_runs table.

    Args:
        engine: SQLAlchemy engine.
        modeling_path: Path to modeling run directory.
        run_id: Run identifier.

    Returns:
        Number of rows loaded.
    """
    summary_file = modeling_path / "modeling" / "run_summary.json"
    if not summary_file.exists():
        logger.warning(f"Run summary not found: {summary_file}")
        return 0

    with summary_file.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        data = [data]

    truncate_table(engine, "raw_modeling_runs")

    loaded_at = datetime.now(UTC)
    rows = []
    for item in data:
        rows.append(
            {
                "run_id": run_id,
                "config_path": item.get("config_path", ""),
                "status": item.get("status", ""),
                "groups": json.dumps(item.get("groups", [])),
                "run_dir": item.get("run_dir", ""),
                "duration_seconds": item.get("duration_seconds"),
                "loaded_at": loaded_at,
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            df.to_sql("raw_modeling_runs", conn, if_exists="append", index=False)

    logger.info(f"Loaded {len(rows)} run summary records")
    return len(rows)


def load_experiments(engine: Engine, modeling_path: Path, run_id: str) -> int:
    """Load experiments.json into raw_modeling_experiments table.

    Each experiment entry becomes one row with full JSON preserved.

    Args:
        engine: SQLAlchemy engine.
        modeling_path: Path to modeling run directory.
        run_id: Run identifier.

    Returns:
        Number of rows loaded.
    """
    experiments_file = modeling_path / "modeling" / "experiments.json"
    if not experiments_file.exists():
        logger.warning(f"Experiments file not found: {experiments_file}")
        return 0

    with experiments_file.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        data = [data]

    truncate_table(engine, "raw_modeling_experiments")

    loaded_at = datetime.now(UTC)
    rows = []
    for item in data:
        # Normalize artifact paths from Docker container paths to relative paths
        normalized_item = normalize_experiment_paths(item)
        rows.append(
            {
                "run_id": run_id,
                "json_data": json.dumps(normalized_item),
                "loaded_at": loaded_at,
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            df.to_sql("raw_modeling_experiments", conn, if_exists="append", index=False)

    logger.info(f"Loaded {len(rows)} experiment records")
    return len(rows)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    # Import settings here to avoid import issues when running outside the backend
    try:
        from src.config import settings

        default_database_url = settings.DATABASE_URL
        default_runs_root = settings.RUNS_ROOT
        default_modeling_run = getattr(settings, "MODELING_RUN", "modeling")
    except ImportError:
        # Running outside the backend package - use defaults
        default_database_url = "postgresql+asyncpg://postgres:postgres@localhost:5432/quality_compass"
        default_runs_root = "/home/ubuntu/repos/project_needle/runs"
        default_modeling_run = "modeling"

    parser = argparse.ArgumentParser(
        description="Load modeling run outputs into PostgreSQL for dbt processing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--database-url",
        default=default_database_url,
        help="PostgreSQL connection string (default from config)",
    )
    parser.add_argument(
        "--runs-root",
        default=default_runs_root,
        help="Root directory for run outputs",
    )
    parser.add_argument(
        "--modeling-run",
        default=default_modeling_run,
        help="Relative path to modeling run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without actually loading",
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point for the modeling data loader.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    args = parse_args()

    # Convert async URL to sync for pandas/SQLAlchemy
    sync_url = get_sync_database_url(args.database_url)
    logger.info(f"Database URL: {sync_url.split('@')[1] if '@' in sync_url else sync_url}")

    # Resolve paths
    runs_root = Path(args.runs_root)
    modeling_path = runs_root / args.modeling_run

    logger.info(f"Runs root: {runs_root}")
    logger.info(f"Modeling path: {modeling_path}")

    # Validate paths
    if not runs_root.exists():
        logger.error(f"Runs root not found: {runs_root}")
        return 1
    if not modeling_path.exists():
        logger.error(f"Modeling run not found: {modeling_path}")
        return 1

    if args.dry_run:
        logger.info("DRY RUN - would load from:")
        logger.info(f"  Run summary: {modeling_path / 'modeling' / 'run_summary.json'}")
        logger.info(f"  Experiments: {modeling_path / 'modeling' / 'experiments.json'}")
        return 0

    # Create engine and tables
    try:
        engine = create_engine(sync_url)
        create_tables(engine)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return 1

    # Extract run ID
    modeling_run_id = extract_run_id(modeling_path)
    logger.info(f"Modeling run_id: {modeling_run_id}")

    # Load data
    results = {}
    try:
        results["run_summary"] = load_run_summary(engine, modeling_path, modeling_run_id)
        results["experiments"] = load_experiments(engine, modeling_path, modeling_run_id)

    except Exception as e:
        logger.exception(f"Error loading data: {e}")
        return 1

    # Summary
    logger.info("=" * 50)
    logger.info("MODELING LOAD SUMMARY")
    logger.info("=" * 50)
    for table, count in results.items():
        logger.info(f"  {table}: {count:,} rows")
    total = sum(results.values())
    logger.info(f"  TOTAL: {total:,} rows")
    logger.info("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
