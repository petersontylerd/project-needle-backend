"""Shared fixtures for validation test suite.

These tests validate the dbt transformation layer by querying PostgreSQL tables.
They can run in two contexts:
1. Local development: Uses fixtures/runs/ from the project root
2. E2E containers: Uses RUNS_ROOT environment variable

Run `tdd-cycle --tier tier2` to hydrate fixtures before running validation tests locally.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader

# Path resolution varies by context:
# - Local: backend/tests/validation/conftest.py -> PROJECT_ROOT/fixtures/
# - Container: /app/tests/validation/conftest.py -> /app/fixtures/ (mounted)
#
# We detect container context by checking if /app/fixtures exists
_THIS_DIR = Path(__file__).resolve().parent
_CONTAINER_FIXTURES = Path("/app/fixtures")

if _CONTAINER_FIXTURES.exists():
    # Running in container - fixtures mounted at /app/fixtures
    FIXTURES_ROOT = _CONTAINER_FIXTURES
else:
    # Running locally - fixtures at project root
    # __file__ = backend/tests/validation/conftest.py
    # validation -> tests -> backend -> project-root (3 parents)
    PROJECT_ROOT = _THIS_DIR.parent.parent.parent
    FIXTURES_ROOT = PROJECT_ROOT / "fixtures"

# Source data path for CSV fixtures
SOURCE_DATA_PATH = FIXTURES_ROOT / "data" / "cdb_api"


def _get_latest_run(graph_name: str = "test_minimal") -> Path | None:
    """Find the most recent run for a graph.

    Checks RUNS_ROOT env var first (for E2E pipeline context), then falls back
    to fixtures directory.

    Args:
        graph_name: Name of the graph (default: test_minimal)

    Returns:
        Path to the latest run directory, or None if no runs exist.
    """
    runs_root = os.getenv("RUNS_ROOT")
    base_path = Path(runs_root) if runs_root else FIXTURES_ROOT / "runs"

    runs_dir = base_path / graph_name
    if not runs_dir.exists():
        return None

    latest: Path | None = None
    for candidate in runs_dir.iterdir():
        if not candidate.is_dir():
            continue
        name = candidate.name
        # Run IDs are YYYYMMDDHHMMSS (14 digits)
        if len(name) != 14 or not name.isdigit():
            continue
        # Only consider runs that have results
        results_dir = candidate / "results" / "nodes"
        if not results_dir.exists():
            continue
        if latest is None or name > latest.name:
            latest = candidate

    return latest


@pytest.fixture(scope="session")
def validation_run_path() -> Path:
    """Path to run under validation.

    Skips tests if no test_minimal run exists (e.g., in production environment).
    """
    run_path = _get_latest_run("test_minimal")
    if run_path is None:
        pytest.skip("No test_minimal run found. Skipping runtime-to-staging validation.")
    return run_path


@pytest.fixture(scope="session")
def validation_loader(validation_run_path: Path) -> ValidationDataLoader:
    """Provide loader for run data."""
    return ValidationDataLoader(run_path=validation_run_path, source_data_path=SOURCE_DATA_PATH)


@pytest.fixture(scope="session")
def all_classifications(validation_loader: ValidationDataLoader) -> list[dict[str, Any]]:
    """Load all classification records from the run."""
    return validation_loader.load_all_classifications()


@pytest.fixture(scope="session")
def source_data_path() -> Path:
    """Path to source CSV data."""
    return SOURCE_DATA_PATH
