"""Tests for validation data loading utilities."""

from pathlib import Path

from tests.validation.helpers.data_loaders import ValidationDataLoader


class TestValidationDataLoader:
    """Tests for ValidationDataLoader."""

    def test_loader_initializes_with_paths(self) -> None:
        """Loader accepts run and source data paths."""
        loader = ValidationDataLoader(
            run_path=Path("/tmp/fake_run"),
            source_data_path=Path("/tmp/fake_source"),
        )
        assert loader.run_path == Path("/tmp/fake_run")
        assert loader.source_data_path == Path("/tmp/fake_source")

    def test_load_node_results_returns_list(self, tmp_path: Path) -> None:
        """load_node_results returns list of entity records."""
        # Create mock node file
        nodes_dir = tmp_path / "results" / "nodes"
        nodes_dir.mkdir(parents=True)
        node_file = nodes_dir / "test_node.jsonl"
        node_file.write_text(
            '{"type": "node_metadata", "canonical_node_id": "test"}\n{"entity": [{"id": "medicareId", "value": "001"}], "metric": [{"values": 1.5}]}\n'
        )

        loader = ValidationDataLoader(run_path=tmp_path, source_data_path=tmp_path)
        results = loader.load_node_results("test_node")

        assert len(results) == 1
        assert results[0]["metric"][0]["values"] == 1.5

    def test_load_raw_csv_reads_pipe_delimited(self, tmp_path: Path) -> None:
        """load_raw_csv handles pipe-delimited CSV files."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("medicareId|losIndex|encounters\n001|1.5|100\n002|2.0|200\n")

        loader = ValidationDataLoader(run_path=tmp_path, source_data_path=tmp_path)
        df = loader.load_raw_csv("test.csv")

        assert len(df) == 2
        assert list(df.columns) == ["medicareId", "losIndex", "encounters"]
        assert df.iloc[0]["losIndex"] == 1.5
