"""Tests for the SignalHydrator service.

Tests the signal hydration process from dbt mart tables to the application database.
The SignalHydrator queries fct_signals table populated by dbt.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.db.models import SignalDomain
from src.services.signal_hydrator import SignalHydrator


@pytest.fixture
def sample_fct_signal_row() -> dict[str, object]:
    """Sample row from fct_signals table (as aliased by FCT_SIGNALS_QUERY)."""
    return {
        # Core identifiers
        "signal_id": "abc123",
        "run_id": "20251210170210",
        "canonical_node_id": "losIndex__medicareId__aggregate_time_period",
        "temporal_node_id": "losIndex__medicareId__dischargeMonth",
        "system_name": "TEST_SYSTEM",
        "facility_id": "TEST001",
        "service_line": "Cardiovascular",
        "sub_service_line": "Cardiac Surgery",
        "metric_id": "losIndex",
        "metric_value": 1.25,
        "peer_mean": 1.08,
        # Statistical measures
        "percentile_rank": 75.0,
        "encounters": 1000,
        # Domain
        "domain": "Efficiency",
        "description": "LOS Index is moderately elevated",
        # Entity grouping (4 fields)
        "entity_dimensions": {},
        "entity_dimensions_hash": "d41d8cd98f00b204e9800998ecf8427e",
        "groupby_label": "Facility-wide",
        "group_value": "Facility-wide",
        # Temporal statistics
        "metric_trend_timeline": None,
        "trend_direction": None,
        # 9 Signal Type classification
        "simplified_signal_type": "baseline",
        "simplified_severity": 50,
        "simplified_severity_range": [40, 60],
        "simplified_inputs": {},
        "simplified_indicators": {},
        "simplified_reasoning": "Test reasoning",
        "simplified_severity_calculation": {},
        # Metadata
        "metadata": {"encounters": 1000},
        "metadata_per_period": None,
        # Peer percentile trends
        "peer_percentile_trends": None,
        # Timestamps
        "detected_at": None,
        "dbt_updated_at": None,
    }


class TestSignalHydratorInit:
    """Tests for SignalHydrator initialization."""

    def test_init_with_defaults(self) -> None:
        """Test that hydrator initializes with default settings."""
        hydrator = SignalHydrator()
        assert hydrator.run_id is None

    def test_init_with_run_id(self) -> None:
        """Test that hydrator accepts run_id parameter."""
        hydrator = SignalHydrator(run_id="20251210170210")
        assert hydrator.run_id == "20251210170210"


class TestDomainMapping:
    """Tests for domain string to enum mapping."""

    def test_map_efficiency(self) -> None:
        """Test mapping Efficiency domain."""
        hydrator = SignalHydrator()
        assert hydrator._map_domain("Efficiency") == SignalDomain.EFFICIENCY

    def test_map_safety(self) -> None:
        """Test mapping Safety domain."""
        hydrator = SignalHydrator()
        assert hydrator._map_domain("Safety") == SignalDomain.SAFETY

    def test_map_effectiveness(self) -> None:
        """Test mapping Effectiveness domain."""
        hydrator = SignalHydrator()
        assert hydrator._map_domain("Effectiveness") == SignalDomain.EFFECTIVENESS

    def test_map_unknown_defaults_to_efficiency(self) -> None:
        """Test that unknown domain defaults to EFFICIENCY."""
        hydrator = SignalHydrator()
        assert hydrator._map_domain("Unknown") == SignalDomain.EFFICIENCY
        assert hydrator._map_domain(None) == SignalDomain.EFFICIENCY


class TestQueryFctSignals:
    """Tests for querying fct_signals table."""

    @pytest.mark.asyncio
    async def test_query_returns_list_of_dicts(self, sample_fct_signal_row: dict[str, object]) -> None:
        """Test that query returns list of dictionaries."""
        hydrator = SignalHydrator()

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [tuple(sample_fct_signal_row.values())]
        mock_result.keys.return_value = list(sample_fct_signal_row.keys())
        mock_session.execute.return_value = mock_result

        result = await hydrator._query_fct_signals(mock_session)

        assert len(result) == 1
        assert result[0]["canonical_node_id"] == "losIndex__medicareId__aggregate_time_period"

    @pytest.mark.asyncio
    async def test_query_with_run_id_filter(self) -> None:
        """Test that run_id filter is applied to query."""
        hydrator = SignalHydrator(run_id="20251210170210")

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_session.execute.return_value = mock_result

        await hydrator._query_fct_signals(mock_session)

        # Check that the query included the run_id filter
        call_args = mock_session.execute.call_args[0][0]
        assert "20251210170210" in str(call_args)


class TestHydrateSignals:
    """Tests for the hydration process."""

    @pytest.mark.asyncio
    async def test_hydrate_returns_stats(self, sample_fct_signal_row: dict[str, object]) -> None:
        """Test that hydration returns statistics."""
        # Create separate sessions for query and upsert (hydrate_signals opens multiple sessions)
        mock_query_session = AsyncMock()
        mock_upsert_session = AsyncMock()

        # Mock fct_signals query result
        mock_fct_result = MagicMock()
        mock_fct_result.fetchall.return_value = [tuple(sample_fct_signal_row.values())]
        mock_fct_result.keys.return_value = list(sample_fct_signal_row.keys())
        mock_query_session.execute.return_value = mock_fct_result

        # Mock bulk upsert (no return value needed, just needs to not raise)
        mock_upsert_session.execute.return_value = MagicMock()

        # Create context managers for each session
        mock_query_context = AsyncMock()
        mock_query_context.__aenter__.return_value = mock_query_session
        mock_query_context.__aexit__.return_value = None

        mock_upsert_context = AsyncMock()
        mock_upsert_context.__aenter__.return_value = mock_upsert_session
        mock_upsert_context.__aexit__.return_value = None

        # Factory returns different contexts on successive calls
        mock_session_factory = MagicMock(side_effect=[mock_query_context, mock_upsert_context])

        hydrator = SignalHydrator(session_factory=mock_session_factory)
        stats = await hydrator.hydrate_signals()

        assert stats["signals_processed"] == 1
        assert stats["signals_created"] == 1
        assert stats["signals_skipped"] == 0

    @pytest.mark.asyncio
    async def test_hydrate_handles_empty_fct_signals(self) -> None:
        """Test hydration when fct_signals is empty."""
        hydrator = SignalHydrator()

        with patch("src.services.signal_hydrator.async_session_maker") as mock_session_maker:
            mock_context = AsyncMock()
            mock_session = AsyncMock()

            # Mock empty fct_signals query
            mock_fct_result = MagicMock()
            mock_fct_result.fetchall.return_value = []
            mock_fct_result.keys.return_value = []
            mock_session.execute.return_value = mock_fct_result

            mock_context.__aenter__.return_value = mock_session
            mock_context.__aexit__.return_value = None
            mock_session_maker.return_value = mock_context

            stats = await hydrator.hydrate_signals()

        assert stats["signals_processed"] == 0

    @pytest.mark.asyncio
    async def test_hydrate_handles_query_error(self) -> None:
        """Test hydration when fct_signals table doesn't exist."""
        hydrator = SignalHydrator()

        with patch("src.services.signal_hydrator.async_session_maker") as mock_session_maker:
            mock_context = AsyncMock()
            mock_session = AsyncMock()
            mock_session.execute.side_effect = Exception("relation fct_signals does not exist")

            mock_context.__aenter__.return_value = mock_session
            mock_context.__aexit__.return_value = None
            mock_session_maker.return_value = mock_context

            stats = await hydrator.hydrate_signals()

        assert stats["signals_processed"] == 0


class TestGetSignalCount:
    """Tests for signal count retrieval."""

    @pytest.mark.asyncio
    async def test_get_signal_count(self) -> None:
        """Test getting signal count from database."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.all.return_value = [("id1",), ("id2",), ("id3",)]
        mock_session.execute.return_value = mock_result
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        hydrator = SignalHydrator(session_factory=mock_session_factory)
        count = await hydrator.get_signal_count()

        assert count == 3


class TestGetFctSignalCount:
    """Tests for fct_signals count retrieval."""

    @pytest.mark.asyncio
    async def test_get_fct_signal_count(self) -> None:
        """Test getting count from fct_signals table."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = 100
        mock_session.execute.return_value = mock_result
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        hydrator = SignalHydrator(session_factory=mock_session_factory)
        count = await hydrator.get_fct_signal_count()

        assert count == 100

    @pytest.mark.asyncio
    async def test_get_fct_signal_count_handles_missing_table(self) -> None:
        """Test that missing table returns 0."""
        hydrator = SignalHydrator()

        with patch("src.services.signal_hydrator.async_session_maker") as mock_session_maker:
            mock_context = AsyncMock()
            mock_session = AsyncMock()
            mock_session.execute.side_effect = Exception("table does not exist")
            mock_context.__aenter__.return_value = mock_session
            mock_context.__aexit__.return_value = None
            mock_session_maker.return_value = mock_context

            count = await hydrator.get_fct_signal_count()

        assert count == 0


class TestGetTechnicalDetails:
    """Tests for get_technical_details method with entity-level grain.

    The get_technical_details method supports the entity_dimensions_hash parameter
    for precise entity lookup in the fct_signals table after the grain fix.
    """

    @pytest.mark.asyncio
    async def test_get_technical_details_accepts_entity_hash_parameter(self) -> None:
        """Test that get_technical_details accepts entity_dimensions_hash parameter.

        Verifies the method signature correctly accepts the new parameter for
        entity-level grain lookup.
        """
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        # Mock the query result with technical details
        mock_result = MagicMock()
        mock_mapping = {
            "canonical_node_id": "losIndex__medicareId__aggregate_time_period",
            "entity_dimensions_hash": "abc123hash",
            "statistical_methods": None,
            "simple_zscore": 1.5,
            "robust_zscore": 1.2,
            "latest_simple_zscore": None,
            "mean_simple_zscore": None,
            "latest_robust_zscore": None,
            "mean_robust_zscore": None,
            "percentile_rank": 75.0,
            "peer_std": 0.5,
            "peer_count": 100,
            "encounters": 1000,
            "global_metric_mean": 1.0,
            "global_metric_std": 0.3,
            "slope": 0.05,
            "slope_percentile": 60.0,
            "acceleration": 0.01,
            "trend_direction": "increasing",
            "momentum": "positive",
            "monthly_z_scores": None,
            "simple_zscore_anomaly": "moderately_high",
            "robust_zscore_anomaly": "slightly_high",
            "latest_simple_zscore_anomaly": None,
            "mean_simple_zscore_anomaly": None,
            "latest_robust_zscore_anomaly": None,
            "mean_robust_zscore_anomaly": None,
            "slope_anomaly": "normal",
            # 3D Matrix tiers (kept for technical display)
            "magnitude_tier": "moderate",
            "trajectory_tier": "stable",
            "consistency_tier": "consistent",
            "coefficient_of_variation": 0.15,
            # 9 Signal Type classification
            "simplified_signal_type": "emerging_risk",
            "simplified_severity": 45,
            "simplified_severity_range": [31, 50],
            "simplified_inputs": {"aggregate_zscore": 1.5, "slope_percentile": 60.0},
            "simplified_indicators": {"magnitude_level": "moderate"},
            "simplified_reasoning": "Emerging risk due to moderate deviation",
            "simplified_severity_calculation": {"base_severity": 40, "final_severity": 45},
        }
        mock_result.mappings.return_value.fetchone.return_value = mock_mapping
        mock_session.execute.return_value = mock_result
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        hydrator = SignalHydrator(session_factory=mock_session_factory)
        result = await hydrator.get_technical_details(
            canonical_node_id="losIndex__medicareId__aggregate_time_period",
            entity_dimensions_hash="abc123hash",
        )

        # Verify the method returns data
        assert result is not None
        assert result["entity_dimensions_hash"] == "abc123hash"

        # Verify the query was called with both parameters
        call_args = mock_session.execute.call_args
        params = call_args[0][1]  # Second positional arg is the params dict
        assert params["canonical_node_id"] == "losIndex__medicareId__aggregate_time_period"
        assert params["entity_dimensions_hash"] == "abc123hash"

    @pytest.mark.asyncio
    async def test_get_technical_details_entity_hash_none(self) -> None:
        """Test that get_technical_details works when entity_dimensions_hash is None.

        When entity_dimensions_hash is None, the query should still work and
        return the first matching row for backward compatibility.
        """
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        # Mock the query result
        mock_result = MagicMock()
        mock_mapping = {
            "canonical_node_id": "losIndex__medicareId__aggregate_time_period",
            "entity_dimensions_hash": "default_hash",
            "statistical_methods": None,
            "simple_zscore": 1.5,
            "robust_zscore": 1.2,
            "latest_simple_zscore": None,
            "mean_simple_zscore": None,
            "latest_robust_zscore": None,
            "mean_robust_zscore": None,
            "percentile_rank": 75.0,
            "peer_std": 0.5,
            "peer_count": 100,
            "encounters": 1000,
            "global_metric_mean": 1.0,
            "global_metric_std": 0.3,
            "slope": 0.05,
            "slope_percentile": 60.0,
            "acceleration": 0.01,
            "trend_direction": "increasing",
            "momentum": "positive",
            "monthly_z_scores": None,
            "simple_zscore_anomaly": "moderately_high",
            "robust_zscore_anomaly": "slightly_high",
            "latest_simple_zscore_anomaly": None,
            "mean_simple_zscore_anomaly": None,
            "latest_robust_zscore_anomaly": None,
            "mean_robust_zscore_anomaly": None,
            "slope_anomaly": "normal",
            # 3D Matrix tiers (kept for technical display)
            "magnitude_tier": "moderate",
            "trajectory_tier": "stable",
            "consistency_tier": "consistent",
            "coefficient_of_variation": 0.15,
            # 9 Signal Type classification
            "simplified_signal_type": "emerging_risk",
            "simplified_severity": 45,
            "simplified_severity_range": [31, 50],
            "simplified_inputs": {"aggregate_zscore": 1.5, "slope_percentile": 60.0},
            "simplified_indicators": {"magnitude_level": "moderate"},
            "simplified_reasoning": "Emerging risk due to moderate deviation",
            "simplified_severity_calculation": {"base_severity": 40, "final_severity": 45},
        }
        mock_result.mappings.return_value.fetchone.return_value = mock_mapping
        mock_session.execute.return_value = mock_result
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        hydrator = SignalHydrator(session_factory=mock_session_factory)
        result = await hydrator.get_technical_details(
            canonical_node_id="losIndex__medicareId__aggregate_time_period",
            entity_dimensions_hash=None,  # Explicitly None
        )

        # Verify the method returns data
        assert result is not None

        # When entity_dimensions_hash is None, the NO_HASH query is used which only has
        # canonical_node_id parameter (no entity_dimensions_hash parameter)
        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["canonical_node_id"] == "losIndex__medicareId__aggregate_time_period"
        assert "entity_dimensions_hash" not in params

    @pytest.mark.asyncio
    async def test_get_technical_details_not_found(self) -> None:
        """Test that get_technical_details returns None when no match found."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        # Mock the query result to return None (no match)
        mock_result = MagicMock()
        mock_result.mappings.return_value.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        hydrator = SignalHydrator(session_factory=mock_session_factory)
        result = await hydrator.get_technical_details(
            canonical_node_id="nonexistent_node",
            entity_dimensions_hash="nonexistent_hash",
        )

        assert result is None
