#!/usr/bin/env python3
"""Load Project Needle run outputs into PostgreSQL raw tables for dbt processing.

This script discovers and loads data from the insight graph runs
into PostgreSQL staging tables that dbt transforms into mart models.

Data Sources:
    - Node results: JSON files from runs/<graph>/<run_id>/results/nodes/
    - Contributions: JSONL files from runs/<graph>/<run_id>/analysis/contribution/
    - Classifications: JSONL from runs/<graph>/<run_id>/analysis/classification/classifications.jsonl

Tables Created (in 'public' schema):
    - raw_node_results: Full JSON content per node result file
    - raw_contributions: One row per contribution record
    - raw_classifications: One row per classification record (signal parent/sub classification)
    - raw_modeling_runs: Empty stub (populated by load_modeling_to_dbt.py when modeling runs)
    - raw_modeling_experiments: Empty stub (populated by load_modeling_to_dbt.py when modeling runs)

Usage:
    # Load all data (default paths from environment/config)
    UV_CACHE_DIR=../.uv-cache uv run python scripts/load_insight_graph_to_dbt.py

    # Load with custom paths
    UV_CACHE_DIR=../.uv-cache uv run python scripts/load_insight_graph_to_dbt.py \
        --runs-root /path/to/runs \
        --insight-graph-run my_run/20240101

Environment Variables:
    DATABASE_URL: PostgreSQL connection string (default: see config.py)
    RUNS_ROOT: Root directory for run outputs
    INSIGHT_GRAPH_RUN: Relative path to insight graph run

Idempotency:
    - Uses TRUNCATE before loading to ensure clean state
    - Re-running the script replaces all data with fresh load
    - Consider using partitioned tables for incremental loads in production

Author: Quality Compass Team
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

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


# Table DDL definitions
RAW_NODE_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS raw_node_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    file_path TEXT,
    json_data TEXT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_node_results_run_id ON raw_node_results(run_id);
"""

RAW_CONTRIBUTIONS_DDL = """
CREATE TABLE IF NOT EXISTS raw_contributions (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    file_path TEXT,
    json_data TEXT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_contributions_run_id ON raw_contributions(run_id);
"""

RAW_CLASSIFICATIONS_DDL = """
CREATE TABLE IF NOT EXISTS raw_classifications (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    file_path TEXT,
    json_data TEXT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_classifications_run_id ON raw_classifications(run_id);
"""

RAW_ENTITY_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS raw_entity_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    canonical_node_id VARCHAR(512) NOT NULL,
    file_path TEXT,
    json_data TEXT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_entity_results_run_id ON raw_entity_results(run_id);
CREATE INDEX IF NOT EXISTS idx_raw_entity_results_node_id ON raw_entity_results(canonical_node_id);
"""

# Modeling table stubs - created empty so dbt source tests pass even when
# the modeling stage is skipped. When modeling runs, load_modeling_to_dbt.py
# will truncate and reload these tables with actual data.
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

    Example:
        >>> get_sync_database_url("postgresql+asyncpg://user:pass@host/db")
        'postgresql://user:pass@host/db'
    """
    return async_url.replace("postgresql+asyncpg://", "postgresql://")


def create_tables(engine: Engine) -> None:
    """Create raw tables if they don't exist.

    Creates all raw tables including modeling stubs. This ensures dbt source
    tests pass even when the modeling stage is skipped.

    Args:
        engine: SQLAlchemy engine for database connection.

    Raises:
        SQLAlchemyError: If table creation fails.
    """
    logger.info("Creating raw tables if not exists...")
    with engine.connect() as conn:
        for ddl in [
            RAW_NODE_RESULTS_DDL,
            RAW_CONTRIBUTIONS_DDL,
            RAW_CLASSIFICATIONS_DDL,
            RAW_ENTITY_RESULTS_DDL,
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

    Raises:
        SQLAlchemyError: If truncation fails.
    """
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))
        conn.commit()
    logger.info(f"Truncated table: {table_name}")


def extract_run_id(run_path: Path) -> str:
    """Extract run_id from run path.

    The run_id is typically the last component for insight graph runs
    (e.g., '20251210170210').

    Args:
        run_path: Path to the run directory.

    Returns:
        Run identifier string.
    """
    # For insight graph: runs/<graph>/<timestamp>
    return run_path.name


def load_node_results(engine: Engine, insight_graph_path: Path, run_id: str) -> int:
    """Load node result files into raw_node_results table.

    Supports both legacy JSON format (*.json) and unified JSONL format (*.jsonl).
    For JSONL files, only the first line (node_metadata) is loaded as the node result.

    Args:
        engine: SQLAlchemy engine.
        insight_graph_path: Path to insight graph run directory.
        run_id: Run identifier.

    Returns:
        Number of rows loaded.

    Raises:
        SQLAlchemyError: If database insert fails.
    """
    nodes_dir = insight_graph_path / "results" / "nodes"
    if not nodes_dir.exists():
        logger.warning(f"Nodes directory not found: {nodes_dir}")
        return 0

    # Support both legacy JSON and unified JSONL formats
    jsonl_files = list(nodes_dir.glob("*.jsonl"))
    json_files = list(nodes_dir.glob("*.json"))
    all_files = jsonl_files + json_files
    logger.info(f"Found {len(jsonl_files)} JSONL + {len(json_files)} JSON node result files")

    if not all_files:
        return 0

    truncate_table(engine, "raw_node_results")

    loaded_at = datetime.now(UTC)
    loaded_count = 0

    for node_file in all_files:
        if node_file.suffix == ".jsonl":
            # JSONL format: first line is node_metadata
            with node_file.open(encoding="utf-8") as f:
                first_line = f.readline().strip()
                if not first_line:
                    logger.warning(f"Empty JSONL file: {node_file}")
                    continue
                json_content = first_line
        else:
            # Legacy JSON format: entire file is the node result
            json_content = node_file.read_text(encoding="utf-8")

        row = {
            "run_id": run_id,
            "file_path": str(node_file),
            "json_data": json_content,
            "loaded_at": loaded_at,
        }
        df = pd.DataFrame([row])
        with engine.begin() as conn:
            df.to_sql("raw_node_results", conn, if_exists="append", index=False)
        loaded_count += 1
        if loaded_count % 10 == 0:
            logger.info(f"Loaded {loaded_count}/{len(all_files)} node results")

    logger.info(f"Loaded {loaded_count} node results")
    return loaded_count


def load_contributions(engine: Engine, insight_graph_path: Path, run_id: str) -> int:
    """Load contribution JSONL files into raw_contributions table.

    Each line in a JSONL file becomes one row in the table.

    Args:
        engine: SQLAlchemy engine.
        insight_graph_path: Path to insight graph run directory.
        run_id: Run identifier.

    Returns:
        Number of rows loaded.

    Raises:
        SQLAlchemyError: If database insert fails.
    """
    contrib_dir = insight_graph_path / "analysis" / "contribution"
    if not contrib_dir.exists():
        logger.warning(f"Contributions directory not found: {contrib_dir}")
        return 0

    # Only load .jsonl files (not manifest/logs directories)
    jsonl_files = [f for f in contrib_dir.glob("*.jsonl") if f.is_file()]
    logger.info(f"Found {len(jsonl_files)} contribution files")

    if not jsonl_files:
        return 0

    truncate_table(engine, "raw_contributions")

    rows = []
    loaded_at = datetime.now(UTC)
    for jsonl_file in jsonl_files:
        with jsonl_file.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    rows.append(
                        {
                            "run_id": run_id,
                            "file_path": str(jsonl_file),
                            "json_data": line,
                            "loaded_at": loaded_at,
                        }
                    )

    if rows:
        # Load in batches to avoid memory issues
        batch_size = 10000
        with engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                df = pd.DataFrame(batch)
                df.to_sql("raw_contributions", conn, if_exists="append", index=False)
                logger.info(f"Loaded contributions batch {i // batch_size + 1}: {len(batch)} rows")

    logger.info(f"Loaded {len(rows)} total contributions")
    return len(rows)


def load_classifications(engine: Engine, insight_graph_path: Path, run_id: str) -> int:
    """Load classification JSONL files into raw_classifications table.

    The classification file contains signal classification outputs from the
    insight-graph-classify CLI, including parent_classification, sub_classification,
    priority_score, confidence, and contributing factors.

    Args:
        engine: SQLAlchemy engine.
        insight_graph_path: Path to insight graph run directory.
        run_id: Run identifier.

    Returns:
        Number of rows loaded.

    Raises:
        SQLAlchemyError: If database insert fails.
    """
    classification_file = insight_graph_path / "analysis" / "classification" / "classifications.jsonl"
    if not classification_file.exists():
        logger.warning(f"Classification file not found: {classification_file}")
        return 0

    truncate_table(engine, "raw_classifications")

    rows = []
    loaded_at = datetime.now(UTC)

    with classification_file.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:  # Skip empty lines
                rows.append(
                    {
                        "run_id": run_id,
                        "file_path": str(classification_file),
                        "json_data": line,
                        "loaded_at": loaded_at,
                    }
                )

    if rows:
        # Load in batches to avoid memory issues
        batch_size = 10000
        with engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                df = pd.DataFrame(batch)
                df.to_sql("raw_classifications", conn, if_exists="append", index=False)
                logger.info(f"Loaded classifications batch {i // batch_size + 1}: {len(batch)} rows")

    logger.info(f"Loaded {len(rows)} total classifications")
    return len(rows)


def load_entity_results(engine: Engine, insight_graph_path: Path, run_id: str) -> int:
    """Load entity result JSONL files into raw_entity_results table.

    Supports two formats:
    1. Unified JSONL format: Entity results are in results/nodes/*.jsonl files
       - First line is node_metadata (contains canonical_node_id)
       - Subsequent lines are entity results
    2. Legacy JSON format: Entity results embedded in results/nodes/*.json files
       - Falls back to extracting from node JSON if no JSONL files found

    Args:
        engine: SQLAlchemy engine.
        insight_graph_path: Path to insight graph run directory.
        run_id: Run identifier.

    Returns:
        Number of rows loaded.

    Raises:
        SQLAlchemyError: If database insert fails.
    """
    nodes_dir = insight_graph_path / "results" / "nodes"
    if not nodes_dir.exists():
        logger.warning(f"Nodes directory not found: {nodes_dir}")
        return 0

    # Check for unified JSONL format first
    jsonl_files = list(nodes_dir.glob("*.jsonl"))
    if jsonl_files:
        logger.info(f"Found {len(jsonl_files)} JSONL node files for entity extraction")
        return _load_entity_results_from_jsonl(engine, jsonl_files, run_id)

    # Fall back to legacy JSON format
    json_files = list(nodes_dir.glob("*.json"))
    if json_files:
        logger.info(f"Found {len(json_files)} legacy JSON node files for entity extraction")
        return _load_entity_results_from_legacy_json(engine, json_files, run_id)

    logger.warning("No node result files found for entity extraction")
    return 0


def _load_entity_results_from_jsonl(engine: Engine, jsonl_files: list[Path], run_id: str) -> int:
    """Load entity results from unified JSONL node files.

    Each JSONL file contains:
    - Line 1: {"type":"node_metadata", "canonical_node_id": "...", ...}
    - Lines 2+: Entity result JSON objects

    Args:
        engine: SQLAlchemy engine.
        jsonl_files: List of JSONL node files.
        run_id: Run identifier.

    Returns:
        Number of entity rows loaded.
    """
    import json as json_module

    truncate_table(engine, "raw_entity_results")

    rows = []
    loaded_at = datetime.now(UTC)

    for jsonl_file in jsonl_files:
        canonical_node_id = None

        with jsonl_file.open(encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                if line_num == 1:
                    # First line is node_metadata - extract canonical_node_id
                    try:
                        metadata = json_module.loads(line)
                        canonical_node_id = metadata.get("canonical_node_id", jsonl_file.stem)
                    except json_module.JSONDecodeError:
                        logger.warning(f"Failed to parse metadata in {jsonl_file}")
                        canonical_node_id = jsonl_file.stem
                    continue  # Skip metadata line for entity loading

                # Lines 2+ are entity results
                if canonical_node_id is None:
                    canonical_node_id = jsonl_file.stem

                rows.append(
                    {
                        "run_id": run_id,
                        "canonical_node_id": canonical_node_id,
                        "file_path": str(jsonl_file),
                        "json_data": line,
                        "loaded_at": loaded_at,
                    }
                )

    if rows:
        # Load in batches to avoid memory issues
        batch_size = 10000
        with engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                df = pd.DataFrame(batch)
                df.to_sql("raw_entity_results", conn, if_exists="append", index=False)
                logger.info(f"Loaded entity results batch {i // batch_size + 1}: {len(batch)} rows")

    logger.info(f"Loaded {len(rows)} total entity results from JSONL files")
    return len(rows)


def _load_entity_results_from_legacy_json(engine: Engine, json_files: list[Path], run_id: str) -> int:
    """Load entity results from legacy JSON node files.

    Legacy JSON format has entity_results embedded in each node JSON file.
    This function extracts them and loads to raw_entity_results.

    Args:
        engine: SQLAlchemy engine.
        json_files: List of JSON node files.
        run_id: Run identifier.

    Returns:
        Number of rows loaded.
    """
    import json as json_module

    logger.info(f"Extracting entity results from {len(json_files)} legacy JSON files")

    truncate_table(engine, "raw_entity_results")

    rows = []
    loaded_at = datetime.now(UTC)

    for json_file in json_files:
        try:
            node_data = json_module.loads(json_file.read_text(encoding="utf-8"))
            canonical_node_id = node_data.get("canonical_node_id", json_file.stem)
            entity_results = node_data.get("entity_results", [])

            for entity in entity_results:
                rows.append(
                    {
                        "run_id": run_id,
                        "canonical_node_id": str(canonical_node_id),
                        "file_path": str(json_file),
                        "json_data": json_module.dumps(entity),
                        "loaded_at": loaded_at,
                    }
                )
        except Exception as e:
            logger.warning(f"Failed to parse entity results from {json_file}: {e}")
            continue

    if rows:
        batch_size = 10000
        with engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                df = pd.DataFrame(batch)
                df.to_sql("raw_entity_results", conn, if_exists="append", index=False)
                logger.info(f"Loaded entity results batch {i // batch_size + 1}: {len(batch)} rows")

    logger.info(f"Loaded {len(rows)} entity results from legacy JSON files")
    return len(rows)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.

    Raises:
        SystemExit: If required arguments are missing or invalid.
    """
    # Import settings here to avoid import issues when running outside the backend
    try:
        from src.config import settings

        default_database_url = settings.DATABASE_URL
        default_runs_root = settings.RUNS_ROOT
        default_insight_graph_run = settings.INSIGHT_GRAPH_RUN
    except ImportError:
        # Running outside the backend package - use defaults
        default_database_url = "postgresql+asyncpg://postgres:postgres@localhost:5432/quality_compass"
        default_runs_root = "/home/ubuntu/repos/project_needle/runs"
        default_insight_graph_run = "inpatient_throughput_v2/20251210170210"

    parser = argparse.ArgumentParser(
        description="Load Project Needle run outputs into PostgreSQL for dbt processing.",
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
        "--insight-graph-run",
        default=default_insight_graph_run,
        help="Relative path to insight graph run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without actually loading",
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point for the data loader.

    Returns:
        Exit code (0 for success, 1 for error).

    Raises:
        None: All exceptions are caught and logged, returning exit code 1.
    """
    args = parse_args()

    # Convert async URL to sync for pandas/SQLAlchemy
    sync_url = get_sync_database_url(args.database_url)
    logger.info(f"Database URL: {sync_url.split('@')[1] if '@' in sync_url else sync_url}")

    # Resolve paths
    runs_root = Path(args.runs_root)
    insight_graph_path = runs_root / args.insight_graph_run

    logger.info(f"Runs root: {runs_root}")
    logger.info(f"Insight graph path: {insight_graph_path}")

    # Validate paths
    if not runs_root.exists():
        logger.error(f"Runs root not found: {runs_root}")
        return 1
    if not insight_graph_path.exists():
        logger.error(f"Insight graph run not found: {insight_graph_path}")
        return 1

    if args.dry_run:
        logger.info("DRY RUN - would load from:")
        logger.info(f"  Node results: {insight_graph_path / 'results' / 'nodes'}")
        logger.info(f"  Contributions: {insight_graph_path / 'analysis' / 'contribution'}")
        logger.info(f"  Classifications: {insight_graph_path / 'analysis' / 'classification' / 'classifications.jsonl'}")
        return 0

    # Create engine and tables
    try:
        engine = create_engine(sync_url)
        create_tables(engine)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return 1

    # Extract run ID
    insight_graph_run_id = extract_run_id(insight_graph_path)
    logger.info(f"Insight graph run_id: {insight_graph_run_id}")

    # Load data
    results = {}
    try:
        results["node_results"] = load_node_results(engine, insight_graph_path, insight_graph_run_id)
        results["entity_results"] = load_entity_results(engine, insight_graph_path, insight_graph_run_id)
        results["contributions"] = load_contributions(engine, insight_graph_path, insight_graph_run_id)
        results["classifications"] = load_classifications(engine, insight_graph_path, insight_graph_run_id)

    except Exception as e:
        logger.exception(f"Error loading data: {e}")
        return 1

    # Summary
    logger.info("=" * 50)
    logger.info("LOAD SUMMARY")
    logger.info("=" * 50)
    for table, count in results.items():
        logger.info(f"  {table}: {count:,} rows")
    total = sum(results.values())
    logger.info(f"  TOTAL: {total:,} rows")
    logger.info("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
