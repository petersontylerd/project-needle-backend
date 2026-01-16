"""Tests for module imports and type aliases.

These tests verify that all modules can be imported without errors,
which also helps achieve full code coverage for import-heavy modules.
"""

import pytest

pytestmark = pytest.mark.tier1


class TestModuleImports:
    """Tests for verifying module imports work correctly."""

    def test_dependencies_module_imports(self) -> None:
        """Test that dependencies module imports correctly."""
        from src.dependencies import DbSession

        # Verify the type alias is defined
        assert DbSession is not None

    def test_activity_router_imports(self) -> None:
        """Test that activity router module imports correctly."""
        from src.activity import router

        assert router is not None

    def test_signals_router_imports(self) -> None:
        """Test that signals router module imports correctly."""
        from src.signals import router

        assert router is not None

    def test_workflow_router_imports(self) -> None:
        """Test that workflow router module imports correctly."""
        from src.workflow import router

        assert router is not None

    def test_services_init_imports(self) -> None:
        """Test that services __init__ exports all services."""
        from src.services import (
            ContributionService,
            SignalGenerator,
            SimulatedMetricsService,
        )

        assert ContributionService is not None
        assert SignalGenerator is not None
        assert SimulatedMetricsService is not None

    def test_db_session_imports(self) -> None:
        """Test that db session module imports correctly."""
        from src.db.session import async_session_maker, get_async_db_session

        # These are factory functions/instances
        assert async_session_maker is not None
        assert get_async_db_session is not None
