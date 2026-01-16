"""Tests for ontology CLI commands."""

from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from src.ontology.cli import _display_stats, cli


class TestSyncCommand:
    """Tests for the sync command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create CLI test runner."""
        return CliRunner()

    def test_sync_dry_run_does_not_execute(self, runner: CliRunner) -> None:
        """Dry run should not call sync service."""
        with patch("src.ontology.cli._run_sync") as mock_sync:
            result = runner.invoke(cli, ["sync", "--dry-run"])

            assert result.exit_code == 0
            assert "DRY RUN" in result.output
            mock_sync.assert_not_called()

    def test_sync_calls_service(self, runner: CliRunner) -> None:
        """Sync should call the sync service."""
        mock_stats = {
            "domains": {"created": 5, "skipped": 0},
            "facilities": {"created": 10, "skipped": 2},
            "metrics": {"created": 15, "skipped": 0},
            "signals": {"created": 100, "skipped": 50},
            "edges": {"created": 200, "skipped": 0},
        }

        with patch("src.ontology.cli._run_sync", new_callable=AsyncMock) as mock_sync:
            mock_sync.return_value = mock_stats
            result = runner.invoke(cli, ["sync"])

            assert result.exit_code == 0
            assert "Sync complete!" in result.output
            mock_sync.assert_called_once()

    def test_sync_handles_error(self, runner: CliRunner) -> None:
        """Sync should handle errors gracefully."""
        with patch("src.ontology.cli._run_sync", new_callable=AsyncMock) as mock_sync:
            mock_sync.side_effect = RuntimeError("Database connection failed")
            result = runner.invoke(cli, ["sync"])

            assert result.exit_code == 1
            assert "Error: Database connection failed" in result.output

    def test_sync_verbose_flag(self, runner: CliRunner) -> None:
        """Verbose flag should be accepted."""
        with patch("src.ontology.cli._run_sync", new_callable=AsyncMock) as mock_sync:
            mock_sync.return_value = {"domains": {"created": 0, "skipped": 0}}
            result = runner.invoke(cli, ["sync", "--verbose"])

            assert result.exit_code == 0


class TestDisplayStats:
    """Tests for _display_stats function."""

    def test_displays_entity_counts(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Should display counts for each entity."""
        stats = {
            "domains": {"created": 5, "skipped": 2},
            "facilities": {"created": 10, "skipped": 0},
        }

        _display_stats(stats)

        captured = capsys.readouterr()
        assert "domains" in captured.out
        assert "facilities" in captured.out
        assert "TOTAL" in captured.out

    def test_calculates_totals(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Should calculate correct totals."""
        stats = {
            "a": {"created": 5, "skipped": 2},
            "b": {"created": 10, "skipped": 3},
        }

        _display_stats(stats)

        captured = capsys.readouterr()
        # Total created: 5 + 10 = 15
        # Total skipped: 2 + 3 = 5
        assert "15" in captured.out
        assert "5" in captured.out
