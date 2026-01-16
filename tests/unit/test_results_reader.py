"""Unit tests for ResultsReaderService."""

import json
from pathlib import Path

import pytest

from src.runs.services.results_reader import ResultsReaderService


class TestResultsReaderService:
    """Tests for JSONL results reading."""

    @pytest.fixture
    def sample_jsonl(self, tmp_path: Path) -> Path:
        """Create sample JSONL file."""
        jsonl_path = tmp_path / "test.jsonl"
        records = [
            {"type": "node_metadata", "canonical_node_id": "test_node"},
            {
                "entity": [{"id": "facility", "value": "FAC001"}],
                "encounters": 1000,
                "metric": [{"values": 1.5}],
                "statistical_methods": [
                    {
                        "statistical_method": "simple_zscore",
                        "statistics": {"simple_zscore": 0.5, "percentile_rank": 60.0},
                        "anomalies": [{"methods": [{"anomaly": "normal"}]}],
                    }
                ],
            },
            {
                "entity": [{"id": "facility", "value": "FAC002"}],
                "encounters": 2000,
                "metric": [{"values": 2.1}],
                "statistical_methods": [
                    {
                        "statistical_method": "simple_zscore",
                        "statistics": {"simple_zscore": 1.2, "percentile_rank": 75.0},
                        "anomalies": [{"methods": [{"anomaly": "slightly_high"}]}],
                    }
                ],
            },
            {
                "entity": [{"id": "facility", "value": "FAC003"}],
                "encounters": 500,
                "metric": [{"values": 0.8}],
                "statistical_methods": [
                    {
                        "statistical_method": "simple_zscore",
                        "statistics": {"simple_zscore": -0.8, "percentile_rank": 30.0},
                        "anomalies": [{"methods": [{"anomaly": "slightly_low"}]}],
                    }
                ],
            },
        ]
        with jsonl_path.open("w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return jsonl_path

    def test_read_results_pagination(self, sample_jsonl: Path) -> None:
        """Test reading results with pagination."""
        service = ResultsReaderService()
        result = service.read_results(sample_jsonl, offset=0, limit=2)

        assert result.total_count == 3  # Excludes metadata line
        assert len(result.results) == 2
        assert result.offset == 0
        assert result.limit == 2

    def test_read_results_offset(self, sample_jsonl: Path) -> None:
        """Test reading results with offset."""
        service = ResultsReaderService()
        result = service.read_results(sample_jsonl, offset=1, limit=10)

        assert result.total_count == 3
        assert len(result.results) == 2  # 2 remaining after offset=1
        assert result.offset == 1

    def test_read_results_entity_extraction(self, sample_jsonl: Path) -> None:
        """Test that entity fields are extracted correctly."""
        service = ResultsReaderService()
        result = service.read_results(sample_jsonl, offset=0, limit=1)

        entity = result.results[0]
        assert entity.entity_id == "FAC001"
        assert entity.encounters == 1000
        assert entity.metric_value == 1.5

    def test_read_results_file_not_found(self, tmp_path: Path) -> None:
        """Test handling of missing file."""
        service = ResultsReaderService()
        result = service.read_results(tmp_path / "nonexistent.jsonl", 0, 10)

        assert result.total_count == 0
        assert len(result.results) == 0
