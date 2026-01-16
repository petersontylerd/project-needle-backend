"""Pytest configuration and fixtures for backend tests."""

import os
import subprocess
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


def _check_alembic_migrations() -> None:
    """Verify database migrations are at head revision.

    Runs at test session start to catch stale migrations early.
    Skips check if SKIP_MIGRATION_CHECK=true is set.

    Raises:
        pytest.UsageError: If migrations are not at head revision.
    """
    # Skip check if explicitly disabled
    if os.environ.get("SKIP_MIGRATION_CHECK", "").lower() == "true":
        return

    error_msg = None

    try:
        # Get current revision
        current_result = subprocess.run(
            ["uv", "run", "alembic", "current"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__)),  # backend directory
            env={**os.environ, "UV_CACHE_DIR": os.environ.get("UV_CACHE_DIR", "../.uv-cache")},
        )
        current_output = current_result.stdout.strip()

        # Get head revision
        heads_result = subprocess.run(
            ["uv", "run", "alembic", "heads"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__)),  # backend directory
            env={**os.environ, "UV_CACHE_DIR": os.environ.get("UV_CACHE_DIR", "../.uv-cache")},
        )
        heads_output = heads_result.stdout.strip()

        # Extract revision IDs (format: "revision_id (head)" or just "revision_id")
        current_rev = current_output.split()[0] if current_output else ""
        head_rev = heads_output.split()[0] if heads_output else ""

        # Check if current is at head
        if current_rev and head_rev and "(head)" not in current_output:
            # Store error message to raise outside try block (so it's not caught by Exception handler)
            error_msg = (
                f"\n\n{'=' * 60}\n"
                f"DATABASE MIGRATIONS ARE STALE!\n"
                f"{'=' * 60}\n"
                f"Current revision: {current_rev}\n"
                f"Head revision:    {head_rev}\n"
                f"\nRun this command to fix:\n"
                f"  cd backend && SKIP_AGE_CHECK=true UV_CACHE_DIR=../.uv-cache uv run alembic upgrade head\n"
                f"\nOr set SKIP_MIGRATION_CHECK=true to skip this check.\n"
                f"{'=' * 60}\n"
            )
    except subprocess.TimeoutExpired:
        # Don't block tests if alembic check times out
        pass
    except FileNotFoundError:
        # uv not available, skip check
        pass
    except Exception:
        # Don't block tests on unexpected errors (e.g., database connection issues)
        pass

    if error_msg:
        raise pytest.UsageError(error_msg)


# Run migration check at module import time (before any tests)
_check_alembic_migrations()


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Create an async test client for the FastAPI app.

    Yields:
        AsyncClient: HTTP client for testing endpoints.

    Example:
        >>> async def test_health(client: AsyncClient):
        ...     response = await client.get("/health")
        ...     assert response.status_code == 200
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
