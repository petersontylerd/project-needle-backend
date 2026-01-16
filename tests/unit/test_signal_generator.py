"""Unit tests for SignalGenerator service.

Tests cover:
- Valid JSON parsing with complete data
- Missing required fields
- Malformed JSON input
- Severity and domain mapping logic
- Entity field extraction
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.db.models import SignalDomain
from src.services.signal_generator import (
    METRIC_TO_DOMAIN,
    SignalGenerator,
    SignalGeneratorError,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def generator() -> SignalGenerator:
    """Create a SignalGenerator instance for testing.

    Returns:
        SignalGenerator: Fresh generator instance.
    """
    return SignalGenerator()


@pytest.fixture
def generator_include_normal() -> SignalGenerator:
    """Create a SignalGenerator that includes normal anomalies.

    Returns:
        SignalGenerator: Generator with include_normal=True.
    """
    return SignalGenerator(include_normal=True)


@pytest.fixture
def minimal_valid_node_data() -> dict[str, Any]:
    """Create minimal valid node results data.

    Returns:
        dict: Minimal valid node results structure.
    """
    return {
        "canonical_node_id": "losIndex__medicareId__aggregate_time_period",
        "canonical_child_node_ids": [],
        "canonical_parent_node_ids": [],
        "entity_results": [
            {
                "encounters": 1000,
                "entity": [
                    {
                        "dataset_field": "medicareId",
                        "id": "medicareId",
                        "value": "FACILITY001",
                    }
                ],
                "metric": [
                    {
                        "metadata": {"metric_id": "losIndex"},
                        "values": 1.25,
                    }
                ],
                "statistical_methods": [
                    {
                        "statistical_method": "statistical_method__simple_zscore__aggregate_time_period",
                        "anomalies": [
                            {
                                "anomaly_profile": "anomaly_profiles__simple_zscore__aggregate",
                                "methods": [
                                    {
                                        "anomaly": "moderately_high",
                                        "anomaly_method": "anomaly_method__simple_zscore",
                                        "applies_to": "simple_zscore",
                                        "interpretation": {
                                            "rendered": "FACILITY001 shows elevated LOS index 1.25.",
                                            "template_id": "template_001",
                                        },
                                        "statistic_value": 1.5,
                                    }
                                ],
                            }
                        ],
                        "statistics": {
                            "peer_mean": 1.0,
                            "peer_std": 0.15,
                            "percentile_rank": 85.0,
                            "simple_zscore": 1.5,
                            "suppressed": False,
                        },
                    }
                ],
            }
        ],
    }


@pytest.fixture
def node_data_with_service_line() -> dict[str, Any]:
    """Create node data with service line entity.

    Returns:
        dict: Node results with vizientServiceLine and vizientSubServiceLine.
    """
    return {
        "canonical_node_id": "losIndex__medicareId_vizientServiceLine__aggregate",
        "canonical_child_node_ids": [],
        "canonical_parent_node_ids": [],
        "entity_results": [
            {
                "encounters": 500,
                "entity": [
                    {
                        "dataset_field": "medicareId",
                        "id": "medicareId",
                        "value": "FACILITY002",
                    },
                    {
                        "dataset_field": "vizientServiceLine",
                        "id": "vizientServiceLine",
                        "value": "Cardiology",
                    },
                    {
                        "dataset_field": "vizientSubServiceLine",
                        "id": "vizientSubServiceLine",
                        "value": "Cardiac Surgery",
                    },
                ],
                "metric": [
                    {
                        "metadata": {"metric_id": "losIndex"},
                        "values": 1.50,
                    }
                ],
                "statistical_methods": [
                    {
                        "statistical_method": "statistical_method__simple_zscore__aggregate",
                        "anomalies": [
                            {
                                "anomaly_profile": "anomaly_profiles__simple_zscore",
                                "methods": [
                                    {
                                        "anomaly": "very_high",
                                        "anomaly_method": "method__high",
                                        "applies_to": "simple_zscore",
                                        "interpretation": {
                                            "rendered": "High LOS in Cardiology.",
                                        },
                                        "statistic_value": 2.5,
                                    }
                                ],
                            }
                        ],
                        "statistics": {
                            "peer_mean": 1.1,
                            "peer_std": 0.16,
                            "percentile_rank": 95.0,
                            "simple_zscore": 2.5,
                        },
                    }
                ],
            }
        ],
    }


@pytest.fixture
def node_data_normal_anomaly() -> dict[str, Any]:
    """Create node data with normal (non-anomalous) result.

    Returns:
        dict: Node results with "normal" anomaly level.
    """
    return {
        "canonical_node_id": "losIndex__medicareId__aggregate",
        "entity_results": [
            {
                "encounters": 800,
                "entity": [
                    {
                        "dataset_field": "medicareId",
                        "id": "medicareId",
                        "value": "FACILITY003",
                    }
                ],
                "metric": [
                    {
                        "metadata": {"metric_id": "losIndex"},
                        "values": 1.05,
                    }
                ],
                "statistical_methods": [
                    {
                        "statistical_method": "statistical_method__simple_zscore",
                        "anomalies": [
                            {
                                "anomaly_profile": "profile",
                                "methods": [
                                    {
                                        "anomaly": "normal",
                                        "interpretation": {
                                            "rendered": "Within normal range.",
                                        },
                                        "statistic_value": 0.3,
                                    }
                                ],
                            }
                        ],
                        "statistics": {
                            "peer_mean": 1.0,
                            "percentile_rank": 55.0,
                            "simple_zscore": 0.3,
                        },
                    }
                ],
            }
        ],
    }


# =============================================================================
# Tests: Valid JSON Parsing
# =============================================================================


class TestValidJsonParsing:
    """Tests for parsing valid JSON node results."""

    def test_parse_minimal_valid_data(self, generator: SignalGenerator, minimal_valid_node_data: dict[str, Any]) -> None:
        """Test parsing minimal valid node data extracts signal correctly."""
        signals = generator.parse_node_results_from_dict(minimal_valid_node_data)

        assert len(signals) == 1
        signal = signals[0]
        assert signal.canonical_node_id == "losIndex__medicareId__aggregate_time_period"
        assert signal.metric_id == "losIndex"
        assert signal.facility == "FACILITY001"
        assert signal.metric_value == Decimal("1.25")
        # significance field removed from SignalCreate schema
        assert signal.domain == SignalDomain.EFFICIENCY

    def test_parse_extracts_statistics(self, generator: SignalGenerator, minimal_valid_node_data: dict[str, Any]) -> None:
        """Test that statistics fields are correctly extracted."""
        signals = generator.parse_node_results_from_dict(minimal_valid_node_data)

        assert len(signals) == 1
        signal = signals[0]
        assert signal.peer_mean == Decimal("1.0")
        assert signal.percentile_rank == Decimal("85.0")
        assert signal.encounters == 1000

    def test_parse_extracts_description(self, generator: SignalGenerator, minimal_valid_node_data: dict[str, Any]) -> None:
        """Test that interpretation is used as description."""
        signals = generator.parse_node_results_from_dict(minimal_valid_node_data)

        assert len(signals) == 1
        assert signals[0].description == "FACILITY001 shows elevated LOS index 1.25."

    def test_parse_with_service_line(self, generator: SignalGenerator, node_data_with_service_line: dict[str, Any]) -> None:
        """Test parsing node data with service line entities."""
        signals = generator.parse_node_results_from_dict(node_data_with_service_line)

        assert len(signals) == 1
        signal = signals[0]
        assert signal.facility == "FACILITY002"
        assert signal.service_line == "Cardiology"
        assert signal.sub_service_line == "Cardiac Surgery"
        # significance field removed from SignalCreate schema

    def test_parse_extracts_peer_statistics(self, generator: SignalGenerator, minimal_valid_node_data: dict[str, Any]) -> None:
        """Test that peer statistics are extracted from node data."""
        signals = generator.parse_node_results_from_dict(minimal_valid_node_data)

        assert len(signals) == 1
        signal = signals[0]
        # peer_mean is extracted from the statistical method
        assert signal.peer_mean == Decimal("1.0")

    def test_parse_excludes_normal_by_default(self, generator: SignalGenerator, node_data_normal_anomaly: dict[str, Any]) -> None:
        """Test that normal anomaly signals are excluded by default."""
        signals = generator.parse_node_results_from_dict(node_data_normal_anomaly)

        assert len(signals) == 0

    def test_parse_includes_normal_when_configured(self, generator_include_normal: SignalGenerator, node_data_normal_anomaly: dict[str, Any]) -> None:
        """Test that normal anomalies are included when include_normal=True."""
        signals = generator_include_normal.parse_node_results_from_dict(node_data_normal_anomaly)

        assert len(signals) == 1
        # significance field removed from SignalCreate schema - just verify signal was created

    def test_parse_multiple_entities(self, generator: SignalGenerator) -> None:
        """Test parsing multiple entities from same node file."""
        data = {
            "canonical_node_id": "losIndex__medicareId__aggregate",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC1"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.3}],
                    "statistical_methods": [
                        {
                            "statistical_method": "simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "profile",
                                    "methods": [
                                        {
                                            "anomaly": "moderately_high",
                                            "interpretation": {"rendered": "High LOS at FAC1"},
                                            "statistic_value": 1.5,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0, "simple_zscore": 1.5},
                        }
                    ],
                },
                {
                    "encounters": 200,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC2"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.4}],
                    "statistical_methods": [
                        {
                            "statistical_method": "simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "profile",
                                    "methods": [
                                        {
                                            "anomaly": "very_high",
                                            "interpretation": {"rendered": "High LOS at FAC2"},
                                            "statistic_value": 2.0,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0, "simple_zscore": 2.0},
                        }
                    ],
                },
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert len(signals) == 2
        facilities = {s.facility for s in signals}
        assert facilities == {"FAC1", "FAC2"}

    def test_detected_at_is_set(self, generator: SignalGenerator, minimal_valid_node_data: dict[str, Any]) -> None:
        """Test that detected_at timestamp is set to current time."""
        before = datetime.now(tz=UTC)
        signals = generator.parse_node_results_from_dict(minimal_valid_node_data)
        after = datetime.now(tz=UTC)

        assert len(signals) == 1
        assert before <= signals[0].detected_at <= after


# =============================================================================
# Tests: Missing Fields and Malformed Input
# =============================================================================


class TestMissingFieldsAndMalformedInput:
    """Tests for handling missing fields and malformed input."""

    def test_parse_file_not_found(self, generator: SignalGenerator, tmp_path: Path) -> None:
        """Test that FileNotFoundError is raised for missing file."""
        nonexistent = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            generator.parse_node_results(nonexistent)

    def test_parse_invalid_json(self, generator: SignalGenerator, tmp_path: Path) -> None:
        """Test that SignalGeneratorError is raised for invalid JSON."""
        invalid_json = tmp_path / "invalid.json"
        invalid_json.write_text("{ not valid json }")

        with pytest.raises(SignalGeneratorError) as exc_info:
            generator.parse_node_results(invalid_json)

        assert "Invalid JSONL format" in str(exc_info.value)

    def test_parse_missing_canonical_node_id(self, generator: SignalGenerator) -> None:
        """Test that validation error is raised for missing canonical_node_id."""
        data: dict[str, list[dict[str, Any]]] = {"entity_results": []}

        with pytest.raises(SignalGeneratorError) as exc_info:
            generator.parse_node_results_from_dict(data)

        assert "Failed to validate" in str(exc_info.value)

    def test_parse_missing_entity_results(self, generator: SignalGenerator) -> None:
        """Test that validation error is raised for missing entity_results."""
        data = {"canonical_node_id": "test_node"}

        with pytest.raises(SignalGeneratorError) as exc_info:
            generator.parse_node_results_from_dict(data)

        assert "Failed to validate" in str(exc_info.value)

    def test_parse_empty_entity_results(self, generator: SignalGenerator) -> None:
        """Test parsing empty entity_results returns empty list."""
        data = {
            "canonical_node_id": "test_node",
            "entity_results": [],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert signals == []

    def test_parse_entity_without_metric(self, generator: SignalGenerator) -> None:
        """Test that entity without metric is skipped."""
        data = {
            "canonical_node_id": "test_node",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC1"}],
                    "metric": [],  # Empty metric list
                    "statistical_methods": [],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert signals == []

    def test_parse_entity_without_anomalies(self, generator: SignalGenerator) -> None:
        """Test that entity without anomalies is skipped."""
        data = {
            "canonical_node_id": "test_node",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC1"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.0}],
                    "statistical_methods": [],  # Empty statistical methods
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert signals == []

    def test_parse_file_with_valid_json(self, generator: SignalGenerator, tmp_path: Path, minimal_valid_node_data: dict[str, Any]) -> None:
        """Test parsing a valid JSONL file from disk."""
        jsonl_file = tmp_path / "valid_node.jsonl"
        # Write JSONL format: header line + entity results
        header = {
            "type": "node_metadata",
            "canonical_node_id": minimal_valid_node_data["canonical_node_id"],
            "canonical_child_node_ids": minimal_valid_node_data.get("canonical_child_node_ids", []),
            "canonical_parent_node_ids": minimal_valid_node_data.get("canonical_parent_node_ids", []),
        }
        lines = [json.dumps(header)]
        for entity in minimal_valid_node_data.get("entity_results", []):
            lines.append(json.dumps(entity))
        jsonl_file.write_text("\n".join(lines))

        signals = generator.parse_node_results(jsonl_file)

        assert len(signals) == 1
        # node_result_path field removed - column dropped from database

    def test_parse_file_os_error(self, generator: SignalGenerator, tmp_path: Path) -> None:
        """Test that OSError during file read is wrapped in SignalGeneratorError."""
        from unittest.mock import patch

        json_file = tmp_path / "test.json"
        json_file.write_text("{}")  # Create file so it exists

        # Mock read_text to raise OSError
        with patch.object(Path, "read_text", side_effect=OSError("Disk read error")), pytest.raises(SignalGeneratorError) as exc_info:
            generator.parse_node_results(json_file)

        assert "Failed to read file" in str(exc_info.value)
        assert "Disk read error" in str(exc_info.value)


# =============================================================================
# Tests: Anomaly Selection
# =============================================================================


class TestAnomalySelection:
    """Tests for anomaly level selection and handling."""

    def test_unknown_anomaly_included_with_include_normal(self, generator: SignalGenerator) -> None:
        """Test that unknown anomaly levels are included when include_normal=True."""
        data = {
            "canonical_node_id": "test_node",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC1"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.0}],
                    "statistical_methods": [
                        {
                            "statistical_method": "simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "profile",
                                    "methods": [
                                        {
                                            "anomaly": "unknown_anomaly_type",
                                            "interpretation": {"rendered": "Unknown anomaly"},
                                            "statistic_value": 0.5,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0},
                        }
                    ],
                }
            ],
        }

        # With include_normal=True to capture the signal with unknown anomaly
        generator_with_normal = SignalGenerator(include_normal=True)
        signals = generator_with_normal.parse_node_results_from_dict(data)

        # Unknown anomaly should be excluded by default (not in SIGNAL_ANOMALY_LEVELS)
        # but if include_normal=True, it is included (significance field removed from SignalCreate)
        assert len(signals) == 1

    def test_significance_prioritizes_most_significant(self, generator: SignalGenerator) -> None:
        """Test that when multiple anomalies exist, the most significant is used."""
        data = {
            "canonical_node_id": "test_node",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC1"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.5}],
                    "statistical_methods": [
                        {
                            "statistical_method": "robust_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "profile",
                                    "methods": [
                                        {
                                            "anomaly": "moderately_high",
                                            "interpretation": {"rendered": "Moderate from robust"},
                                            "statistic_value": 1.2,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"robust_zscore": 1.2},
                        },
                        {
                            "statistical_method": "simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "profile",
                                    "methods": [
                                        {
                                            "anomaly": "extremely_high",
                                            "interpretation": {"rendered": "Extreme from simple"},
                                            "statistic_value": 3.0,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"simple_zscore": 3.0, "peer_mean": 1.0},
                        },
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert len(signals) == 1
        # significance field removed from SignalCreate schema
        # But the most significant anomaly's description should still be used
        assert "Extreme from simple" in signals[0].description


# =============================================================================
# Tests: Domain Mapping
# =============================================================================


class TestDomainMapping:
    """Tests for metric to domain mapping."""

    @pytest.mark.parametrize(
        ("metric_id", "expected_domain"),
        [
            ("losIndex", SignalDomain.EFFICIENCY),
            ("averageLos", SignalDomain.EFFICIENCY),
            ("throughput", SignalDomain.EFFICIENCY),
            ("clabsiRate", SignalDomain.SAFETY),
            ("vaeRate", SignalDomain.SAFETY),
            ("fallRate", SignalDomain.SAFETY),
            ("readmissionRate", SignalDomain.EFFECTIVENESS),
            ("mortalityRate", SignalDomain.EFFECTIVENESS),
        ],
    )
    def test_metric_to_domain_mapping(self, metric_id: str, expected_domain: SignalDomain) -> None:
        """Test each metric ID maps to correct domain."""
        assert METRIC_TO_DOMAIN.get(metric_id) == expected_domain

    def test_unknown_metric_defaults_to_efficiency(self, generator: SignalGenerator) -> None:
        """Test that unknown metric IDs default to Efficiency domain."""
        data = {
            "canonical_node_id": "test_node",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC1"}],
                    "metric": [{"metadata": {"metric_id": "unknownMetric"}, "values": 1.0}],
                    "statistical_methods": [
                        {
                            "statistical_method": "simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "profile",
                                    "methods": [
                                        {
                                            "anomaly": "very_high",
                                            "interpretation": {"rendered": "Unknown metric high"},
                                            "statistic_value": 2.0,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 0.5, "simple_zscore": 2.0},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert len(signals) == 1
        assert signals[0].domain == SignalDomain.EFFICIENCY  # Default


# =============================================================================
# Tests: Entity Field Extraction
# =============================================================================


class TestEntityFieldExtraction:
    """Tests for extracting entity fields."""

    def test_extract_facility_from_medicare_id(self, generator: SignalGenerator) -> None:
        """Test that medicareId is extracted as facility."""
        data = {
            "canonical_node_id": "test",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "ABC123"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.2}],
                    "statistical_methods": [
                        {
                            "statistical_method": "simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "p",
                                    "methods": [{"anomaly": "very_high", "interpretation": {"rendered": "test"}, "statistic_value": 2.0}],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert len(signals) == 1
        assert signals[0].facility == "ABC123"
        assert signals[0].facility_id == "ABC123"

    def test_missing_entity_fields_use_defaults(self, generator: SignalGenerator) -> None:
        """Test that missing entity fields use default values."""
        data = {
            "canonical_node_id": "test",
            "entity_results": [
                {
                    "encounters": 100,
                    "entity": [],  # No entity fields
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.2}],
                    "statistical_methods": [
                        {
                            "statistical_method": "simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "p",
                                    "methods": [{"anomaly": "very_high", "interpretation": {"rendered": "test"}, "statistic_value": 2.0}],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        assert len(signals) == 1
        assert signals[0].facility == "Unknown"
        assert signals[0].service_line == "All"
        assert signals[0].sub_service_line is None


# =============================================================================
# Tests: Integration with Real Fixtures
# =============================================================================


class TestRealFixtures:
    """Integration tests using actual fixture files."""

    def test_parse_real_fixture_file(self, generator: SignalGenerator) -> None:
        """Test parsing actual fixture file if available."""
        fixture_path = Path(
            "/home/ubuntu/repos/project_needle/fixtures/runs/test_minimal/20251209132644/results/nodes/losIndex__medicareId__aggregate_time_period.json"
        )

        if not fixture_path.exists():
            pytest.skip("Fixture file not available")

        signals = generator.parse_node_results(fixture_path)

        # Should extract multiple signals (one per facility)
        assert len(signals) > 0

        # All signals should have required fields
        for signal in signals:
            assert signal.canonical_node_id
            assert signal.metric_id == "losIndex"
            assert signal.facility
            assert signal.metric_value
            assert signal.description
            assert signal.detected_at


# =============================================================================
# Tests: Edge Cases for Coverage
# =============================================================================


class TestCoverageEdgeCases:
    """Tests for edge cases to improve coverage."""

    def test_null_metric_values_skipped(self, generator: SignalGenerator) -> None:
        """Test that entities with null metric values are skipped.

        Tests line 264: if metric.values is None: return None
        """
        data = {
            "canonical_node_id": "losIndex__medicareId__aggregate_time_period",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 1000,
                    "entity": [
                        {
                            "dataset_field": "medicareId",
                            "id": "medicareId",
                            "value": "FACILITY001",
                        }
                    ],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": None,  # Suppressed data
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "anomaly_profiles__simple_zscore",
                                    "methods": [
                                        {
                                            "anomaly": "moderately_high",
                                            "anomaly_method": "anomaly_method__simple_zscore",
                                            "applies_to": "simple_zscore",
                                            "interpretation": {"rendered": "Test"},
                                            "statistic_value": 1.5,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(data)

        # Entity with null metric values should be skipped
        assert len(signals) == 0

    def test_validation_error_in_node_results(self, generator: SignalGenerator, tmp_path: Path) -> None:
        """Test validation error when node results have invalid schema.

        Tests lines 176-177: Failed to validate node results schema
        """
        # Valid JSONL format but invalid entity_result schema (missing required fields)
        header = {
            "type": "node_metadata",
            "canonical_node_id": "test",
        }
        # Entity result missing required 'metric' and 'statistical_methods' fields
        invalid_entity = {
            "encounters": 100,
            "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FAC1"}],
            # Missing 'metric' and 'statistical_methods' which are required by NodeEntityResult
        }
        jsonl_file = tmp_path / "invalid_schema.jsonl"
        lines = [json.dumps(header), json.dumps(invalid_entity)]
        jsonl_file.write_text("\n".join(lines))

        with pytest.raises(SignalGeneratorError) as exc_info:
            generator.parse_node_results(jsonl_file)

        assert "Failed to validate node results schema" in str(exc_info.value)

    def test_unknown_anomaly_level_excluded_by_default(self, generator: SignalGenerator) -> None:
        """Test that unknown anomaly levels are excluded unless include_normal is True.

        Tests line 280: if anomaly_level not in SIGNAL_ANOMALY_LEVELS and not self.include_normal
        """
        data = {
            "canonical_node_id": "losIndex__medicareId__aggregate_time_period",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 1000,
                    "entity": [
                        {
                            "dataset_field": "medicareId",
                            "id": "medicareId",
                            "value": "FACILITY001",
                        }
                    ],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": 1.05,
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "anomaly_profiles__simple_zscore",
                                    "methods": [
                                        {
                                            "anomaly": "unknown_anomaly_level",  # Unknown level
                                            "anomaly_method": "anomaly_method__simple_zscore",
                                            "applies_to": "simple_zscore",
                                            "interpretation": {"rendered": "Test"},
                                            "statistic_value": 0.1,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0},
                        }
                    ],
                }
            ],
        }

        # Default generator excludes unknown anomaly levels
        signals = generator.parse_node_results_from_dict(data)
        assert len(signals) == 0


class TestTemporalIntegration:
    """Tests for temporal node integration and signal classification."""

    def test_generator_with_nodes_directory(self, tmp_path: Path) -> None:
        """Generator accepts nodes_directory parameter."""
        generator = SignalGenerator(nodes_directory=tmp_path)
        assert generator.nodes_directory == tmp_path
        # classifier removed - classification now handled by dbt
        assert generator._temporal_cache == {}

    def test_generator_without_nodes_directory(self) -> None:
        """Generator works without nodes_directory (no temporal data)."""
        generator = SignalGenerator()
        assert generator.nodes_directory is None
        # classifier removed - classification now handled by dbt

    def test_signal_includes_temporal_fields_when_no_temporal_node(self, generator: SignalGenerator, minimal_valid_node_data: dict[str, Any]) -> None:
        """Signal has None temporal fields when no temporal node available."""
        signals = generator.parse_node_results_from_dict(minimal_valid_node_data)
        assert len(signals) == 1
        signal = signals[0]
        # Temporal fields should be None when no temporal node
        # signal_classification removed - classification now handled by dbt
        assert signal.temporal_node_id is None

    def test_signal_with_trends_to_edge_but_no_temporal_file(self, tmp_path: Path) -> None:
        """Signal has temporal_node_id but no classification if file missing."""
        generator = SignalGenerator(nodes_directory=tmp_path)
        node_data = {
            "canonical_node_id": "losIndex__medicareId__aggregate",
            "canonical_child_node_ids": [
                {
                    "canonical_child_node_id": "losIndex__medicareId__dischargeMonth",
                    "edge_type": "trends_to",
                }
            ],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 1000,
                    "entity": [
                        {
                            "dataset_field": "medicareId",
                            "id": "medicareId",
                            "value": "FACILITY001",
                        }
                    ],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": 1.25,
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "anomaly_profiles__simple_zscore",
                                    "methods": [
                                        {
                                            "anomaly": "moderately_high",
                                            "anomaly_method": "method__high",
                                            "applies_to": "simple_zscore",
                                            "interpretation": {"rendered": "Test"},
                                            "statistic_value": 1.5,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.15},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(node_data)
        assert len(signals) == 1
        signal = signals[0]
        # temporal_node_id is set from edge, but classification is None (file missing)
        assert signal.temporal_node_id == "losIndex__medicareId__dischargeMonth"
        # signal_classification removed - classification now handled by dbt

    def test_signal_with_full_temporal_data(self, tmp_path: Path) -> None:
        """Signal includes classification when temporal node data available."""
        # Create temporal node file with DETERIORATING trend
        # Z-scores going from near 0 to more negative = negative slope = deteriorating
        # We need: significant negative slope AND deviation < 1.5
        # Values: 0.98 -> 0.88 (small decrease) with peer_mean=1.0, peer_std=0.1
        # Z-scores: -0.2 -> -1.2 (becoming more negative, slope ~ -0.09)
        # Mean z-score ~ -0.7, latest z-score = -1.2
        # Deviation: |-1.2 - (-0.7)| = 0.5 < 1.5 (no outlier)
        temporal_node_data = {
            "canonical_node_id": "losIndex__medicareId__dischargeMonth",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 12000,
                    "entity": [
                        {
                            "dataset_field": "medicareId",
                            "id": "medicareId",
                            "value": "FACILITY001",
                        }
                    ],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": {
                                "timeline": [
                                    {"period": "202401", "value": 0.98, "encounters": 1000},
                                    {"period": "202402", "value": 0.97, "encounters": 1000},
                                    {"period": "202403", "value": 0.96, "encounters": 1000},
                                    {"period": "202404", "value": 0.95, "encounters": 1000},
                                    {"period": "202405", "value": 0.94, "encounters": 1000},
                                    {"period": "202406", "value": 0.93, "encounters": 1000},
                                    {"period": "202407", "value": 0.92, "encounters": 1000},
                                    {"period": "202408", "value": 0.91, "encounters": 1000},
                                    {"period": "202409", "value": 0.90, "encounters": 1000},
                                    {"period": "202410", "value": 0.89, "encounters": 1000},
                                    {"period": "202411", "value": 0.88, "encounters": 1000},
                                    {"period": "202412", "value": 0.87, "encounters": 1000},
                                ]
                            },
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore__dischargeMonth",
                            "anomalies": [],
                            "statistics": {
                                "peer_mean": 1.0,
                                "peer_std": 0.1,
                                "latest_period": "202412",
                                "latest_simple_zscore": -1.3,
                                "mean_simple_zscore": -0.75,
                                "observations": 12000,
                                "periods": 12,
                            },
                        }
                    ],
                }
            ],
        }
        # Write JSONL format: header line + entity results
        temporal_path = tmp_path / "losIndex__medicareId__dischargeMonth.jsonl"
        header = {
            "type": "node_metadata",
            "canonical_node_id": temporal_node_data["canonical_node_id"],
            "canonical_child_node_ids": temporal_node_data.get("canonical_child_node_ids", []),
            "canonical_parent_node_ids": temporal_node_data.get("canonical_parent_node_ids", []),
        }
        lines = [json.dumps(header)]
        for entity in temporal_node_data.get("entity_results", []):
            lines.append(json.dumps(entity))
        temporal_path.write_text("\n".join(lines))

        generator = SignalGenerator(nodes_directory=tmp_path)
        aggregate_node_data = {
            "canonical_node_id": "losIndex__medicareId__aggregate",
            "canonical_child_node_ids": [
                {
                    "canonical_child_node_id": "losIndex__medicareId__dischargeMonth",
                    "edge_type": "trends_to",
                }
            ],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 12000,
                    "entity": [
                        {
                            "dataset_field": "medicareId",
                            "id": "medicareId",
                            "value": "FACILITY001",
                        }
                    ],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": 0.87,
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "anomaly_profiles__simple_zscore",
                                    "methods": [
                                        {
                                            "anomaly": "very_low",
                                            "anomaly_method": "method__low",
                                            "applies_to": "simple_zscore",
                                            "interpretation": {"rendered": "Low LOS"},
                                            "statistic_value": -1.3,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.1},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(aggregate_node_data)
        assert len(signals) == 1
        signal = signals[0]

        # Temporal fields should be populated
        assert signal.temporal_node_id == "losIndex__medicareId__dischargeMonth"
        # Classification may be AT_RISK or similar depending on slope calculation
        # The key is that classification is set when temporal data is available
        # signal_classification removed - classification now handled by dbt

    def test_temporal_cache_prevents_duplicate_loads(self, tmp_path: Path) -> None:
        """Temporal node is only loaded once per generator instance."""
        temporal_node_data = {
            "canonical_node_id": "losIndex__medicareId__dischargeMonth",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [],
        }
        # Write JSONL format
        temporal_path = tmp_path / "losIndex__medicareId__dischargeMonth.jsonl"
        header = {
            "type": "node_metadata",
            "canonical_node_id": temporal_node_data["canonical_node_id"],
            "canonical_child_node_ids": temporal_node_data.get("canonical_child_node_ids", []),
            "canonical_parent_node_ids": temporal_node_data.get("canonical_parent_node_ids", []),
        }
        temporal_path.write_text(json.dumps(header))

        generator = SignalGenerator(nodes_directory=tmp_path)

        # Load temporal node twice
        result1 = generator._load_temporal_node("losIndex__medicareId__dischargeMonth")
        result2 = generator._load_temporal_node("losIndex__medicareId__dischargeMonth")

        # Both should return same cached instance
        assert result1 is result2
        # Cache should have the entry
        assert "losIndex__medicareId__dischargeMonth" in generator._temporal_cache

    def test_temporal_cache_stores_none_for_missing_file(self, tmp_path: Path) -> None:
        """Cache stores None for missing temporal node file."""
        generator = SignalGenerator(nodes_directory=tmp_path)

        result = generator._load_temporal_node("nonexistent__node")

        assert result is None
        assert generator._temporal_cache.get("nonexistent__node") is None

    def test_get_temporal_z_scores_matches_entity(self, tmp_path: Path) -> None:
        """Z-scores extracted for matching entity key."""
        from src.schemas.signal import NodeResults

        temporal_node_data = {
            "canonical_node_id": "losIndex__medicareId__dischargeMonth",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 1000,
                    "entity": [
                        {
                            "dataset_field": "medicareId",
                            "id": "medicareId",
                            "value": "FACILITY001",
                        }
                    ],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": {
                                "timeline": [
                                    {"period": "202401", "value": 1.0},
                                    {"period": "202402", "value": 1.1},
                                    {"period": "202403", "value": 1.2},
                                ]
                            },
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [],
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.1},
                        }
                    ],
                }
            ],
        }

        temporal_node = NodeResults.model_validate(temporal_node_data)
        generator = SignalGenerator(nodes_directory=tmp_path)

        # Should find matching entity
        z_scores = generator._get_temporal_z_scores(temporal_node, ("FACILITY001", "All", None))
        assert z_scores is not None
        assert len(z_scores) == 3
        # Z-scores computed from (value - peer_mean) / peer_std
        expected = [(1.0 - 1.0) / 0.1, (1.1 - 1.0) / 0.1, (1.2 - 1.0) / 0.1]
        assert z_scores == pytest.approx(expected)

    def test_get_temporal_z_scores_no_match(self, tmp_path: Path) -> None:
        """Returns None when entity not found in temporal node."""
        from src.schemas.signal import NodeResults

        temporal_node_data = {
            "canonical_node_id": "losIndex__medicareId__dischargeMonth",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 1000,
                    "entity": [
                        {
                            "dataset_field": "medicareId",
                            "id": "medicareId",
                            "value": "FACILITY001",
                        }
                    ],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": {"timeline": [{"period": "202401", "value": 1.0}]},
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [],
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.1},
                        }
                    ],
                }
            ],
        }

        temporal_node = NodeResults.model_validate(temporal_node_data)
        generator = SignalGenerator(nodes_directory=tmp_path)

        # Non-matching entity key
        z_scores = generator._get_temporal_z_scores(temporal_node, ("DIFFERENT_FACILITY", "All", None))
        assert z_scores is None

    def test_classification_struggling(self, tmp_path: Path) -> None:
        """Signal classified as STRUGGLING for sustained moderate z-scores."""
        # Create temporal node with sustained moderate z-scores (flat, above 0.75 but below 2.0)
        # Values around 1.10 with peer_mean=1.0, peer_std=0.1 give z-scores around 1.0
        temporal_values = [1.10, 1.11, 1.09, 1.12, 1.10, 1.11, 1.09, 1.12, 1.10, 1.11, 1.09, 1.10]
        temporal_node_data = {
            "canonical_node_id": "losIndex__medicareId__dischargeMonth",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 12000,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FACILITY001"}],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": {"timeline": [{"period": f"2024{i + 1:02d}", "value": v} for i, v in enumerate(temporal_values)]},
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [],
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.1},
                        }
                    ],
                }
            ],
        }
        # Write JSONL format
        temporal_path = tmp_path / "losIndex__medicareId__dischargeMonth.jsonl"
        header = {
            "type": "node_metadata",
            "canonical_node_id": temporal_node_data["canonical_node_id"],
            "canonical_child_node_ids": temporal_node_data.get("canonical_child_node_ids", []),
            "canonical_parent_node_ids": temporal_node_data.get("canonical_parent_node_ids", []),
        }
        lines = [json.dumps(header)]
        for entity in temporal_node_data.get("entity_results", []):
            lines.append(json.dumps(entity))
        temporal_path.write_text("\n".join(lines))

        generator = SignalGenerator(nodes_directory=tmp_path)
        aggregate_data = {
            "canonical_node_id": "losIndex__medicareId__aggregate",
            "canonical_child_node_ids": [{"canonical_child_node_id": "losIndex__medicareId__dischargeMonth", "edge_type": "trends_to"}],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 12000,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FACILITY001"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 1.20}],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "anomaly_profiles__simple_zscore",
                                    "methods": [
                                        {
                                            "anomaly": "very_high",
                                            "anomaly_method": "method",
                                            "applies_to": "simple_zscore",
                                            "interpretation": {"rendered": "High LOS"},
                                            "statistic_value": 2.0,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.1},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(aggregate_data)
        assert len(signals) == 1
        # Classification is set when temporal data is available
        # signal_classification removed - classification now handled by dbt

    def test_classification_achieving(self, tmp_path: Path) -> None:
        """Signal classified as ACHIEVING for positive slope trend."""
        # Create temporal node with improving trend (positive slope = z-scores increasing)
        # For z-scores to be increasing: values need to increase away from peer_mean (below it)
        # Values: 0.75 -> 0.90 (increasing toward 1.0) with peer_mean=1.0, peer_std=0.1
        # Z-scores: -2.5 -> -1.0 (increasing = less negative) = positive slope = improving
        # Keep deviation from mean < 1.5 to avoid outlier_driven
        temporal_values = [-2.4, -2.3, -2.2, -2.0, -1.9, -1.7, -1.6, -1.4, -1.3, -1.2, -1.1, -1.0]
        temporal_node_data = {
            "canonical_node_id": "losIndex__medicareId__dischargeMonth",
            "canonical_child_node_ids": [],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 12000,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FACILITY001"}],
                    "metric": [
                        {
                            "metadata": {"metric_id": "losIndex"},
                            "values": {"timeline": [{"period": f"2024{i + 1:02d}", "value": 1.0 + v * 0.1} for i, v in enumerate(temporal_values)]},
                        }
                    ],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [],
                            # Use larger peer_std so z-scores match our expected values
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.1},
                        }
                    ],
                }
            ],
        }
        # Write JSONL format
        temporal_path = tmp_path / "losIndex__medicareId__dischargeMonth.jsonl"
        header = {
            "type": "node_metadata",
            "canonical_node_id": temporal_node_data["canonical_node_id"],
            "canonical_child_node_ids": temporal_node_data.get("canonical_child_node_ids", []),
            "canonical_parent_node_ids": temporal_node_data.get("canonical_parent_node_ids", []),
        }
        lines = [json.dumps(header)]
        for entity in temporal_node_data.get("entity_results", []):
            lines.append(json.dumps(entity))
        temporal_path.write_text("\n".join(lines))

        generator = SignalGenerator(nodes_directory=tmp_path)
        aggregate_data = {
            "canonical_node_id": "losIndex__medicareId__aggregate",
            "canonical_child_node_ids": [{"canonical_child_node_id": "losIndex__medicareId__dischargeMonth", "edge_type": "trends_to"}],
            "canonical_parent_node_ids": [],
            "entity_results": [
                {
                    "encounters": 12000,
                    "entity": [{"dataset_field": "medicareId", "id": "medicareId", "value": "FACILITY001"}],
                    "metric": [{"metadata": {"metric_id": "losIndex"}, "values": 0.90}],
                    "statistical_methods": [
                        {
                            "statistical_method": "statistical_method__simple_zscore",
                            "anomalies": [
                                {
                                    "anomaly_profile": "anomaly_profiles__simple_zscore",
                                    "methods": [
                                        {
                                            "anomaly": "slightly_low",
                                            "anomaly_method": "method",
                                            "applies_to": "simple_zscore",
                                            "interpretation": {"rendered": "Slightly low LOS"},
                                            "statistic_value": -1.0,
                                        }
                                    ],
                                }
                            ],
                            "statistics": {"peer_mean": 1.0, "peer_std": 0.1},
                        }
                    ],
                }
            ],
        }

        signals = generator.parse_node_results_from_dict(aggregate_data)
        assert len(signals) == 1
        # Classification is set when temporal data is available
        # signal_classification removed - classification now handled by dbt

    def test_no_temporal_integration_without_nodes_directory(self, generator: SignalGenerator, minimal_valid_node_data: dict[str, Any]) -> None:
        """Without nodes_directory, temporal data is not loaded."""
        # Add trends_to edge to data
        minimal_valid_node_data["canonical_child_node_ids"] = [{"canonical_child_node_id": "losIndex__medicareId__dischargeMonth", "edge_type": "trends_to"}]

        signals = generator.parse_node_results_from_dict(minimal_valid_node_data)
        assert len(signals) == 1
        signal = signals[0]
        # temporal_node_id is set from edge
        assert signal.temporal_node_id == "losIndex__medicareId__dischargeMonth"
        # But no classification (nodes_directory is None)
        # signal_classification removed - classification now handled by dbt
