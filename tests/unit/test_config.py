"""Tests for backend configuration module.

Tests the pydantic-settings based Settings class for environment
variable loading and validation.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from src.config import Settings

pytestmark = pytest.mark.tier1


class TestSettingsDefaults:
    """Test default values for Settings."""

    def test_database_url_default(self) -> None:
        """Verify DATABASE_URL has localhost default."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
            assert "localhost" in settings.DATABASE_URL
            assert "asyncpg" in settings.DATABASE_URL

    def test_debug_default_false(self) -> None:
        """Verify DEBUG defaults to False for security."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
            assert settings.DEBUG is False

    def test_runs_root_default(self) -> None:
        """Verify RUNS_ROOT has Docker-compatible default."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
            assert settings.RUNS_ROOT == "/data/runs"

    def test_app_title_default(self) -> None:
        """Verify APP_TITLE default."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
            assert settings.APP_TITLE == "Quality Compass API"


class TestSettingsEnvironmentOverride:
    """Test environment variable overrides."""

    def test_database_url_from_env(self) -> None:
        """Verify DATABASE_URL is read from environment."""
        test_url = "postgresql+asyncpg://user:pass@db:5432/testdb"
        with patch.dict(os.environ, {"DATABASE_URL": test_url}, clear=True):
            settings = Settings()
            assert test_url == settings.DATABASE_URL

    def test_debug_from_env(self) -> None:
        """Verify DEBUG is read from environment."""
        with patch.dict(os.environ, {"DEBUG": "true"}, clear=True):
            settings = Settings()
            assert settings.DEBUG is True

    def test_runs_root_from_env(self) -> None:
        """Verify RUNS_ROOT is read from environment."""
        with patch.dict(os.environ, {"RUNS_ROOT": "/custom/path"}, clear=True):
            settings = Settings()
            assert settings.RUNS_ROOT == "/custom/path"

    def test_cors_origins_from_env(self) -> None:
        """Verify CORS_ORIGINS is read from environment."""
        origins = "http://example.com,http://test.com"
        with patch.dict(os.environ, {"CORS_ORIGINS": origins}, clear=True):
            settings = Settings()
            assert origins == settings.CORS_ORIGINS


class TestCorsOriginsList:
    """Test cors_origins_list property."""

    def test_parses_comma_separated(self) -> None:
        """Verify cors_origins_list parses comma-separated origins."""
        origins = "http://a.com,http://b.com,http://c.com"
        with patch.dict(os.environ, {"CORS_ORIGINS": origins}, clear=True):
            settings = Settings()
            assert settings.cors_origins_list == [
                "http://a.com",
                "http://b.com",
                "http://c.com",
            ]

    def test_strips_whitespace(self) -> None:
        """Verify cors_origins_list strips whitespace."""
        origins = "http://a.com , http://b.com , http://c.com"
        with patch.dict(os.environ, {"CORS_ORIGINS": origins}, clear=True):
            settings = Settings()
            assert settings.cors_origins_list == [
                "http://a.com",
                "http://b.com",
                "http://c.com",
            ]

    def test_single_origin(self) -> None:
        """Verify cors_origins_list handles single origin."""
        with patch.dict(os.environ, {"CORS_ORIGINS": "http://only.com"}, clear=True):
            settings = Settings()
            assert settings.cors_origins_list == ["http://only.com"]


class TestSettingsExtraIgnored:
    """Test that extra environment variables are ignored."""

    def test_unknown_env_var_ignored(self) -> None:
        """Verify unknown environment variables don't cause errors."""
        with patch.dict(
            os.environ,
            {"UNKNOWN_SETTING": "value", "ANOTHER_UNKNOWN": "123"},
            clear=True,
        ):
            # Should not raise
            settings = Settings()
            assert settings.DEBUG is False  # Default still works
