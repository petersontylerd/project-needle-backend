"""Tests for the ContributionService.

Tests the contribution data retrieval from dbt fct_contributions mart table.
The ContributionService queries fct_contributions and transforms data into
ContributionRecord and ContributionResponse objects for API output.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.contribution import ContributionRecord
from src.services.contribution_service import ContributionService, ContributionServiceError


@pytest.fixture
def sample_fct_contribution_row() -> dict[str, object]:
    """Sample row from fct_contributions table."""
    return {
        "contribution_id": "abc123",
        "run_id": "20251210170210",
        "parent_node_id": "losIndex__medicareId__aggregate_time_period",
        "child_node_id": "losIndex__medicareId__vizientServiceLine__aggregate_time_period",
        "parent_facility_id": "TEST001",
        "parent_service_line": None,
        "child_facility_id": "TEST001",
        "child_service_line": "Cardiovascular",
        "child_sub_service_line": "Cardiac Surgery",
        "metric_id": "losIndex",
        "contribution_method": "weighted_mean",
        "child_value": 1.25,
        "parent_value": 1.08,
        "weight_field": "encounters",
        "weight_value": 500,
        "weight_share": 0.35,
        "excess_over_parent": 0.17,
        "contribution_weight": 0.0595,
        "contribution_direction": "positive",
        "contribution_rank": 1,
        "contribution_pct": 15.5,
    }


class TestContributionServiceInit:
    """Tests for ContributionService initialization."""

    def test_init_with_defaults(self) -> None:
        """Test that service initializes with default settings."""
        service = ContributionService()
        assert service.run_id is None

    def test_init_with_run_id(self) -> None:
        """Test that service accepts run_id parameter."""
        service = ContributionService(run_id="20251210170210")
        assert service.run_id == "20251210170210"


class TestQueryContributionsByParent:
    """Tests for querying contributions by parent node."""

    @pytest.mark.asyncio
    async def test_query_returns_list_of_dicts(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that query returns list of dictionaries."""
        service = ContributionService()

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [tuple(sample_fct_contribution_row.values())]
        mock_result.keys.return_value = list(sample_fct_contribution_row.keys())
        mock_session.execute.return_value = mock_result

        result = await service._query_contributions_by_parent(
            mock_session,
            "losIndex__medicareId__aggregate_time_period",
            "TEST001",  # parent_facility_id
        )

        assert len(result) == 1
        assert result[0]["parent_node_id"] == "losIndex__medicareId__aggregate_time_period"

    @pytest.mark.asyncio
    async def test_query_with_run_id_filter(self) -> None:
        """Test that run_id filter is applied to query."""
        service = ContributionService(run_id="20251210170210")

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_session.execute.return_value = mock_result

        await service._query_contributions_by_parent(
            mock_session,
            "losIndex__medicareId__aggregate_time_period",
            "TEST001",  # parent_facility_id
        )

        # Check that the query included the run_id filter
        call_args = mock_session.execute.call_args[0][0]
        assert "run_id = :run_id" in str(call_args)


class TestGetContributionsForParent:
    """Tests for get_contributions_for_parent method."""

    @pytest.mark.asyncio
    async def test_returns_contribution_records(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that method returns list of ContributionRecord objects."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [tuple(sample_fct_contribution_row.values())]
        mock_result.keys.return_value = list(sample_fct_contribution_row.keys())
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service = ContributionService(session_factory=mock_session_factory)
        records = await service.get_contributions_for_parent(
            "losIndex__medicareId__aggregate_time_period",
            "TEST001",  # parent_facility_id - required for facility isolation
        )

        assert len(records) == 1
        assert isinstance(records[0], ContributionRecord)
        assert records[0].parent_node_id == "losIndex__medicareId__aggregate_time_period"

    @pytest.mark.asyncio
    async def test_respects_top_n_limit(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that top_n parameter limits results."""
        # Create 3 rows
        rows = [tuple(sample_fct_contribution_row.values()) for _ in range(3)]

        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_result.keys.return_value = list(sample_fct_contribution_row.keys())
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service = ContributionService(session_factory=mock_session_factory)
        records = await service.get_contributions_for_parent(
            "losIndex__medicareId__aggregate_time_period",
            "TEST001",  # parent_facility_id - required for facility isolation
            top_n=2,
        )

        assert len(records) == 2

    @pytest.mark.asyncio
    async def test_handles_query_error(self) -> None:
        """Test that query errors raise ContributionServiceError."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Database connection failed")

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service = ContributionService(session_factory=mock_session_factory)

        with pytest.raises(ContributionServiceError, match="Failed to query contributions"):
            await service.get_contributions_for_parent(
                "test_parent_node",
                "TEST001",  # parent_facility_id - required for facility isolation
            )


class TestGetTopContributorsGlobal:
    """Tests for get_top_contributors_global method."""

    @pytest.mark.asyncio
    async def test_returns_contribution_records(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that method returns list of ContributionRecord objects."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [tuple(sample_fct_contribution_row.values())]
        mock_result.keys.return_value = list(sample_fct_contribution_row.keys())
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service = ContributionService(session_factory=mock_session_factory)
        records = await service.get_top_contributors_global(top_n=5)

        assert len(records) == 1
        assert isinstance(records[0], ContributionRecord)

    @pytest.mark.asyncio
    async def test_handles_query_error_returns_empty(self) -> None:
        """Test that query errors return empty list (graceful degradation)."""
        service = ContributionService()

        with patch("src.services.contribution_service.async_session_maker") as mock_session_maker:
            mock_context = AsyncMock()
            mock_session = AsyncMock()
            mock_session.execute.side_effect = Exception("Database connection failed")

            mock_context.__aenter__.return_value = mock_session
            mock_context.__aexit__.return_value = None
            mock_session_maker.return_value = mock_context

            records = await service.get_top_contributors_global()

        assert records == []


class TestRowToRecord:
    """Tests for _row_to_record conversion."""

    def test_converts_all_fields(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that all row fields are correctly converted."""
        service = ContributionService()
        record = service._row_to_record(sample_fct_contribution_row)

        assert record.method == "weighted_mean"
        assert record.child_value == 1.25
        assert record.parent_value == 1.08
        assert record.weight_field == "encounters"
        assert record.weight_value == 500
        assert record.weight_share == 0.35
        assert record.excess_over_parent == 0.17
        assert record.parent_node_id == "losIndex__medicareId__aggregate_time_period"

    def test_reconstructs_parent_entity(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that parent_entity is reconstructed from denormalized fields."""
        service = ContributionService()
        record = service._row_to_record(sample_fct_contribution_row)

        assert record.parent_entity == {"medicareId": "TEST001"}

    def test_reconstructs_child_entity(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that child_entity is reconstructed from denormalized fields."""
        service = ContributionService()
        record = service._row_to_record(sample_fct_contribution_row)

        assert record.child_entity == {
            "medicareId": "TEST001",
            "vizientServiceLine": "Cardiovascular",
            "vizientSubServiceLine": "Cardiac Surgery",
        }

    def test_handles_null_child_entity_fields(self) -> None:
        """Test handling when all child entity fields are null."""
        service = ContributionService()
        row = {
            "contribution_id": "abc123",
            "run_id": "20251210170210",
            "parent_node_id": "test_node",
            "child_node_id": None,
            "parent_facility_id": "TEST001",
            "parent_service_line": None,
            "child_facility_id": None,
            "child_service_line": None,
            "child_sub_service_line": None,
            "metric_id": "losIndex",
            "contribution_method": "weighted_mean",
            "child_value": None,
            "parent_value": 1.08,
            "weight_field": "encounters",
            "weight_value": 500,
            "weight_share": 1.0,
            "excess_over_parent": None,
            "contribution_weight": 0.0,
            "contribution_direction": None,
            "contribution_rank": 1,
            "contribution_pct": None,
        }

        record = service._row_to_record(row)
        assert record.child_entity is None

    def test_converts_contribution_pct_to_contribution_value(self, sample_fct_contribution_row: dict[str, object]) -> None:
        """Test that contribution_pct is converted to contribution_value (decimal)."""
        service = ContributionService()
        record = service._row_to_record(sample_fct_contribution_row)

        # contribution_pct=15.5 -> contribution_value=0.155
        assert record.contribution_value == pytest.approx(0.155, rel=1e-3)

    def test_handles_missing_method_defaults(self) -> None:
        """Test that missing contribution_method defaults to weighted_mean."""
        service = ContributionService()
        row = {
            "contribution_id": "abc123",
            "run_id": "20251210170210",
            "parent_node_id": "test_node",
            "child_node_id": None,
            "parent_facility_id": None,
            "parent_service_line": None,
            "child_facility_id": None,
            "child_service_line": None,
            "child_sub_service_line": None,
            "metric_id": "losIndex",
            "contribution_method": None,  # Missing
            "child_value": 1.0,
            "parent_value": 1.0,
            "weight_field": None,
            "weight_value": None,
            "weight_share": None,
            "excess_over_parent": None,
            "contribution_weight": 0.0,
            "contribution_direction": None,
            "contribution_rank": 1,
            "contribution_pct": None,
        }

        record = service._row_to_record(row)
        assert record.method == "weighted_mean"
        assert record.weight_field == "encounters"


class TestEntityLabelExtraction:
    """Tests for entity label extraction."""

    def test_extract_vizient_service_line(self) -> None:
        """Test extraction of vizientServiceLine."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=1.10,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=0.50,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "vizientServiceLine": "Cardiology"},
        )

        label = service._extract_child_entity_label(record)
        assert label == "Cardiology"

    def test_extract_vizient_sub_service_line(self) -> None:
        """Test extraction of vizientSubServiceLine (takes precedence)."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=1.10,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=0.50,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={
                "medicareId": "AAA001",
                "vizientServiceLine": "Cardiology",
                "vizientSubServiceLine": "Interventional",
            },
        )

        label = service._extract_child_entity_label(record)
        assert label == "Interventional"

    def test_extract_fallback_to_non_parent_field(self) -> None:
        """Test fallback to first non-parent entity field."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=1.10,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=0.50,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "customDimension": "CustomValue"},
        )

        label = service._extract_child_entity_label(record)
        assert label == "CustomValue"

    def test_extract_root_for_no_child_entity(self) -> None:
        """Test 'Root' label for records without child_entity."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=None,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=1.0,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity=None,
        )

        label = service._extract_child_entity_label(record)
        assert label == "Root"


class TestDescriptionGeneration:
    """Tests for description generation."""

    def test_generate_description_above_parent(self) -> None:
        """Test description for above-parent contributor."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=1.10,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=0.50,
            excess_over_parent=0.05,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "vizientServiceLine": "Cardiology"},
        )

        response = service.to_response(record)

        assert "contributes" in response.description
        assert "above parent average" in response.description
        assert "50.0%" in response.description

    def test_generate_description_below_parent(self) -> None:
        """Test description for below-parent contributor."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=0.90,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=30,
            weight_share=0.30,
            excess_over_parent=-0.10,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "vizientServiceLine": "Surgery"},
        )

        response = service.to_response(record)

        assert "performs" in response.description
        assert "below parent average" in response.description
        assert "30.0%" in response.description

    def test_generate_description_no_excess(self) -> None:
        """Test description when excess_over_parent is None."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=None,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=0.50,
            excess_over_parent=None,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "vizientServiceLine": "ICU"},
        )

        response = service.to_response(record)

        assert "accounts for" in response.description
        assert "50.0%" in response.description

    def test_generate_description_exact_zero_excess(self) -> None:
        """Test description when excess_over_parent is exactly zero."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=1.00,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=40,
            weight_share=0.40,
            excess_over_parent=0.0,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "vizientServiceLine": "Cardiology"},
        )

        response = service.to_response(record)

        assert "matches parent average" in response.description
        assert "40.0%" in response.description


class TestResponseConversion:
    """Tests for ContributionRecord to ContributionResponse conversion."""

    def test_to_response_decimal_conversion(self) -> None:
        """Test decimal conversion in response."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=1.10,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=0.50,
            contribution_value=0.55,
            excess_over_parent=0.05,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "vizientServiceLine": "Cardiology"},
        )

        response = service.to_response(record)

        assert isinstance(response.child_value, Decimal)
        assert isinstance(response.parent_value, Decimal)
        assert isinstance(response.weight_value, Decimal)
        assert isinstance(response.weight_share_percent, Decimal)
        assert isinstance(response.contribution_value, Decimal)
        assert isinstance(response.excess_over_parent_percent, Decimal)

    def test_to_response_percent_calculation(self) -> None:
        """Test percentage calculations in response."""
        service = ContributionService()
        record = ContributionRecord(
            method="weighted_mean",
            child_value=1.10,
            parent_value=1.00,
            weight_field="encounters",
            weight_value=50,
            weight_share=0.50,
            contribution_value=0.55,
            excess_over_parent=0.05,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity={"medicareId": "AAA001", "vizientServiceLine": "Cardiology"},
        )

        response = service.to_response(record)

        # weight_share 0.50 -> 50.0%
        assert response.weight_share_percent == Decimal("50.0")
        # excess_over_parent 0.05 / parent_value 1.00 -> 5.0%
        assert response.excess_over_parent_percent == Decimal("5.0")

    def test_to_response_null_values(self) -> None:
        """Test response with null values."""
        service = ContributionService()
        record = ContributionRecord(
            method="test",
            child_value=None,
            parent_value=1.00,
            weight_field="count",
            weight_value=1.0,
            weight_share=1.0,
            contribution_value=None,
            excess_over_parent=None,
            parent_node_file="",
            parent_node_id="test",
            parent_entity={"medicareId": "AAA001"},
            child_entity=None,
        )

        response = service.to_response(record)

        assert response.child_value is None
        assert response.contribution_value is None
        assert response.excess_over_parent_percent is None


class TestTopContributors:
    """Tests for get_top_contributors method."""

    def test_get_top_contributors_by_excess(self) -> None:
        """Test getting top contributors by excess_over_parent."""
        service = ContributionService()
        records = [
            ContributionRecord(
                method="weighted_mean",
                child_value=1.10,
                parent_value=1.00,
                weight_field="encounters",
                weight_value=50,
                weight_share=0.50,
                excess_over_parent=0.05,
                parent_node_file="",
                parent_node_id="test",
                parent_entity={"medicareId": "AAA001"},
                child_entity={"vizientServiceLine": "Cardiology"},
            ),
            ContributionRecord(
                method="weighted_mean",
                child_value=0.95,
                parent_value=1.00,
                weight_field="encounters",
                weight_value=30,
                weight_share=0.30,
                excess_over_parent=-0.015,
                parent_node_file="",
                parent_node_id="test",
                parent_entity={"medicareId": "AAA001"},
                child_entity={"vizientServiceLine": "Neurology"},
            ),
            ContributionRecord(
                method="weighted_mean",
                child_value=0.825,
                parent_value=1.00,
                weight_field="encounters",
                weight_value=20,
                weight_share=0.20,
                excess_over_parent=-0.035,
                parent_node_file="",
                parent_node_id="test",
                parent_entity={"medicareId": "AAA001"},
                child_entity={"vizientServiceLine": "Orthopedics"},
            ),
        ]

        top_2 = service.get_top_contributors(records, top_n=2, sort_by="excess_over_parent")

        assert len(top_2) == 2
        # Should be sorted by absolute excess_over_parent (Cardiology=0.05, Orthopedics=-0.035)
        assert abs(top_2[0].excess_over_parent or 0) >= abs(top_2[1].excess_over_parent or 0)

    def test_get_top_contributors_by_weight_share(self) -> None:
        """Test getting top contributors by weight_share."""
        service = ContributionService()
        records = [
            ContributionRecord(
                method="weighted_mean",
                child_value=1.10,
                parent_value=1.00,
                weight_field="encounters",
                weight_value=50,
                weight_share=0.50,
                parent_node_file="",
                parent_node_id="test",
                parent_entity={},
                child_entity={"vizientServiceLine": "Cardiology"},
            ),
            ContributionRecord(
                method="weighted_mean",
                child_value=0.95,
                parent_value=1.00,
                weight_field="encounters",
                weight_value=30,
                weight_share=0.30,
                parent_node_file="",
                parent_node_id="test",
                parent_entity={},
                child_entity={"vizientServiceLine": "Neurology"},
            ),
        ]

        top_2 = service.get_top_contributors(records, top_n=2, sort_by="weight_share")

        assert len(top_2) == 2
        # Should be sorted by weight_share descending (0.50, 0.30)
        assert top_2[0].weight_share >= top_2[1].weight_share

    def test_get_top_contributors_empty_list(self) -> None:
        """Test with empty records list."""
        service = ContributionService()
        top = service.get_top_contributors([])
        assert len(top) == 0


class TestContributionServiceError:
    """Tests for ContributionServiceError exception."""

    def test_error_with_message_only(self) -> None:
        """Test error with message only."""
        error = ContributionServiceError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.parent_node_id is None

    def test_error_with_parent_node_id(self) -> None:
        """Test error with parent_node_id context."""
        error = ContributionServiceError("Test error", parent_node_id="test_node")
        assert "test_node" in str(error)
        assert error.parent_node_id == "test_node"


class TestDetermineHierarchyLevel:
    """Tests for _determine_hierarchy_level helper method."""

    def test_facility_level_when_no_service_line(self) -> None:
        """Test that facility-wide signals return 'facility' level."""
        service = ContributionService()

        # Mock signal with no service line
        signal = MagicMock()
        signal.service_line = None
        signal.sub_service_line = None

        level = service._determine_hierarchy_level(signal)
        assert level == "facility"

    def test_facility_level_when_service_line_is_facility_wide(self) -> None:
        """Test 'Facility-wide' service line returns 'facility' level."""
        service = ContributionService()

        signal = MagicMock()
        signal.service_line = "Facility-wide"
        signal.sub_service_line = None

        level = service._determine_hierarchy_level(signal)
        assert level == "facility"

    def test_service_line_level(self) -> None:
        """Test that service-line signals return 'service_line' level."""
        service = ContributionService()

        signal = MagicMock()
        signal.service_line = "Cardiology"
        signal.sub_service_line = None

        level = service._determine_hierarchy_level(signal)
        assert level == "service_line"

    def test_service_line_level_with_none_sub_service_line(self) -> None:
        """Test service line with 'None' string sub service line."""
        service = ContributionService()

        signal = MagicMock()
        signal.service_line = "Cardiology"
        signal.sub_service_line = "None"  # String "None", not Python None

        level = service._determine_hierarchy_level(signal)
        assert level == "service_line"

    def test_sub_service_line_level(self) -> None:
        """Test that sub-service-line signals return 'sub_service_line' level."""
        service = ContributionService()

        signal = MagicMock()
        signal.service_line = "Cardiology"
        signal.sub_service_line = "Interventional Cardiology"

        level = service._determine_hierarchy_level(signal)
        assert level == "sub_service_line"


class TestGetUpwardContribution:
    """Tests for get_upward_contribution method."""

    @pytest.mark.asyncio
    async def test_returns_contribution_record_when_found(self) -> None:
        """Test that method returns ContributionRecord when data exists."""
        sample_row = {
            "contribution_id": "upward123",
            "run_id": "20251210170210",
            "parent_node_id": "losIndex__medicareId__aggregate_time_period",
            "child_node_id": "losIndex__medicareId__vizientServiceLine__aggregate_time_period",
            "parent_facility_id": "TEST001",
            "parent_service_line": None,
            "child_facility_id": "TEST001",
            "child_service_line": "Cardiology",
            "child_sub_service_line": None,
            "metric_id": "losIndex",
            "contribution_method": "weighted_mean",
            "child_value": 1.25,
            "parent_value": 1.08,
            "weight_field": "encounters",
            "weight_value": 500,
            "weight_share": 0.35,
            "excess_over_parent": 0.17,
            "contribution_weight": 0.0595,
            "contribution_direction": "positive",
            "contribution_rank": 1,
            "contribution_pct": 15.5,
        }

        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [tuple(sample_row.values())]
        mock_result.keys.return_value = list(sample_row.keys())
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service = ContributionService(session_factory=mock_session_factory)
        record = await service.get_upward_contribution(
            child_facility_id="TEST001",
            child_service_line="Cardiology",
            child_sub_service_line=None,
            metric_id="losIndex",
        )

        assert record is not None
        assert isinstance(record, ContributionRecord)
        assert record.parent_node_id == "losIndex__medicareId__aggregate_time_period"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_data(self) -> None:
        """Test that method returns None when no contribution data exists."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service = ContributionService(session_factory=mock_session_factory)
        record = await service.get_upward_contribution(
            child_facility_id="TEST001",
            child_service_line="Cardiology",
            child_sub_service_line=None,
            metric_id="losIndex",
        )

        assert record is None

    @pytest.mark.asyncio
    async def test_handles_null_service_line(self) -> None:
        """Test that NULL service line is handled correctly in query."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service = ContributionService(session_factory=mock_session_factory)
        await service.get_upward_contribution(
            child_facility_id="TEST001",
            child_service_line=None,  # Facility-wide signal
            child_sub_service_line=None,
            metric_id="losIndex",
        )

        # Verify the query was executed
        assert mock_session.execute.called


class TestGetHierarchicalContributions:
    """Tests for get_hierarchical_contributions method."""

    @pytest.mark.asyncio
    async def test_facility_signal_has_no_upward_contribution(self) -> None:
        """Test that facility-wide signals have no upward contribution."""
        service = ContributionService()

        signal = MagicMock()
        signal.id = "signal-123"
        signal.facility_id = "TEST001"
        signal.service_line = None  # Facility-wide
        signal.sub_service_line = None
        signal.metric_id = "losIndex"
        signal.parent_node_id = "losIndex__medicareId__aggregate_time_period"

        with patch.object(service, "get_upward_contribution", new_callable=AsyncMock) as mock_upward:
            with patch.object(service, "get_contributions_for_parent", new_callable=AsyncMock) as mock_downward:
                mock_upward.return_value = None
                mock_downward.return_value = []

                upward, downward, level = await service.get_hierarchical_contributions(
                    signal=signal,
                    top_n=5,
                )

        assert upward is None
        assert level == "facility"
        # Upward should NOT be called for facility-level signals
        mock_upward.assert_not_called()

    @pytest.mark.asyncio
    async def test_service_line_signal_fetches_both_directions(self) -> None:
        """Test that service-line signals fetch both upward and downward."""
        service = ContributionService()

        signal = MagicMock()
        signal.id = "signal-123"
        signal.facility_id = "TEST001"
        signal.service_line = "Cardiology"
        signal.sub_service_line = None
        signal.metric_id = "losIndex"
        signal.parent_node_id = "losIndex__medicareId__vizientServiceLine__aggregate_time_period"

        mock_upward_record = MagicMock(spec=ContributionRecord)
        mock_downward_records = [MagicMock(spec=ContributionRecord)]

        with patch.object(service, "get_upward_contribution", new_callable=AsyncMock) as mock_upward:
            with patch.object(service, "get_contributions_for_parent", new_callable=AsyncMock) as mock_downward:
                mock_upward.return_value = mock_upward_record
                mock_downward.return_value = mock_downward_records

                upward, downward, level = await service.get_hierarchical_contributions(
                    signal=signal,
                    top_n=5,
                )

        assert upward is mock_upward_record
        assert downward == mock_downward_records
        assert level == "service_line"

        # Both methods should have been called
        mock_upward.assert_called_once()
        mock_downward.assert_called_once()

    @pytest.mark.asyncio
    async def test_downward_contributions_include_service_line_filter(self) -> None:
        """Test that downward contributions include parent_service_line filter."""
        service = ContributionService()

        signal = MagicMock()
        signal.id = "signal-123"
        signal.facility_id = "TEST001"
        signal.service_line = "Cardiology"
        signal.sub_service_line = None
        signal.metric_id = "losIndex"
        signal.parent_node_id = "losIndex__medicareId__vizientServiceLine__aggregate_time_period"

        with patch.object(service, "get_upward_contribution", new_callable=AsyncMock) as mock_upward:
            with patch.object(service, "get_contributions_for_parent", new_callable=AsyncMock) as mock_downward:
                mock_upward.return_value = None
                mock_downward.return_value = []

                await service.get_hierarchical_contributions(
                    signal=signal,
                    top_n=5,
                )

        # Verify that get_contributions_for_parent was called with service line
        mock_downward.assert_called_once()
        call_kwargs = mock_downward.call_args[1]
        assert call_kwargs.get("parent_service_line") == "Cardiology"


class TestServiceLineFiltering:
    """Tests for service line filtering in contribution queries."""

    @pytest.mark.asyncio
    async def test_downward_query_includes_parent_service_line(self) -> None:
        """Test that downward contributions query filters by parent_service_line."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service_with_session = ContributionService(session_factory=mock_session_factory)
        await service_with_session.get_contributions_for_parent(
            parent_node_id="losIndex__medicareId__vizientServiceLine__aggregate_time_period",
            parent_facility_id="TEST001",
            parent_service_line="Cardiology",
        )

        # Verify the query included parent_service_line filter
        call_args = mock_session.execute.call_args[0][0]
        query_str = str(call_args)
        assert "parent_service_line" in query_str

    @pytest.mark.asyncio
    async def test_facility_level_uses_null_service_line(self) -> None:
        """Test that facility-level queries use NULL for parent_service_line."""
        mock_context = AsyncMock()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_session.execute.return_value = mock_result

        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None
        mock_session_factory = MagicMock(return_value=mock_context)

        service_with_session = ContributionService(session_factory=mock_session_factory)
        await service_with_session.get_contributions_for_parent(
            parent_node_id="losIndex__medicareId__aggregate_time_period",
            parent_facility_id="TEST001",
            parent_service_line=None,  # Facility-wide
        )

        # Verify the query was executed with null handling
        call_args = mock_session.execute.call_args[0][0]
        query_str = str(call_args)
        # The query should handle NULL service line appropriately
        assert "parent_service_line" in query_str
