"""Data loading utilities for validation tests.

Loads real run data and source CSVs for first-principles validation.
NO imports from production code - this is intentional.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


class ValidationDataLoader:
    """Load complete datasets for validation testing.

    This loader provides access to:
    - Node result files (JSONL format)
    - Classification records (JSONL format)
    - Contribution records (JSONL format)
    - Raw source CSV files (pipe-delimited)
    """

    def __init__(
        self,
        run_path: Path,
        source_data_path: Path,
    ) -> None:
        """Initialize loader with paths to run and source data.

        Args:
            run_path: Path to the run directory containing results/nodes/, analysis/, etc.
            source_data_path: Path to source CSV data directory.
        """
        self.run_path = run_path
        self.source_data_path = source_data_path

    def load_raw_csv(self, filename: str) -> pd.DataFrame:
        """Load a source CSV file (pipe-delimited).

        Args:
            filename: Name of CSV file to load.

        Returns:
            DataFrame with CSV contents.
        """
        return pd.read_csv(self.source_data_path / filename, delimiter="|")

    def load_node_results(self, node_id: str) -> list[dict[str, Any]]:
        """Load entity results from a node JSONL file.

        Args:
            node_id: Node identifier (without .jsonl extension).

        Returns:
            List of entity result records (excludes node_metadata header).
        """
        # Try JSONL first, then JSON
        jsonl_path = self.run_path / "results" / "nodes" / f"{node_id}.jsonl"
        json_path = self.run_path / "results" / "nodes" / f"{node_id}.json"

        if jsonl_path.exists():
            return self._load_jsonl_node(jsonl_path)
        elif json_path.exists():
            return self._load_json_node(json_path)
        else:
            raise FileNotFoundError(f"Node file not found: {node_id}")

    def _load_jsonl_node(self, path: Path) -> list[dict[str, Any]]:
        """Load node results from JSONL format."""
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                # Skip metadata header
                if record.get("type") == "node_metadata":
                    continue
                records.append(record)
        return records

    def _load_json_node(self, path: Path) -> list[dict[str, Any]]:
        """Load node results from JSON format."""
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("entity_results", [])

    def load_node_metadata(self, node_id: str) -> dict[str, Any] | None:
        """Load metadata header from a node JSONL file.

        Args:
            node_id: Node identifier (without .jsonl extension).

        Returns:
            Metadata dict if found, None otherwise.
        """
        jsonl_path = self.run_path / "results" / "nodes" / f"{node_id}.jsonl"
        json_path = self.run_path / "results" / "nodes" / f"{node_id}.json"

        if jsonl_path.exists():
            with jsonl_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    if record.get("type") == "node_metadata":
                        return record
            return None
        elif json_path.exists():
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("metadata")
        else:
            return None

    def load_all_classifications(self) -> list[dict[str, Any]]:
        """Load all classification records from the run.

        Returns:
            List of all classification records.
        """
        path = self.run_path / "analysis" / "classification" / "classifications.jsonl"
        if not path.exists():
            return []

        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def load_contributions(self, parent_node_id: str, metric_id: str) -> list[dict[str, Any]]:
        """Load contribution records for a parent node.

        Args:
            parent_node_id: Parent node identifier.
            metric_id: Metric identifier.

        Returns:
            List of contribution records.
        """
        # Contribution files are in: analysis/contribution/{metric}/{parent_node_id}.{metric_id}.jsonl
        path = self.run_path / "analysis" / "contribution" / metric_id / f"{parent_node_id}.{metric_id}.jsonl"
        if not path.exists():
            return []

        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def iter_node_files(self) -> list[Path]:
        """List all node result files in the run.

        Returns:
            Sorted list of node file paths.
        """
        nodes_dir = self.run_path / "results" / "nodes"
        if not nodes_dir.exists():
            return []
        return sorted(nodes_dir.glob("*.jsonl")) + sorted(nodes_dir.glob("*.json"))

    def iter_contribution_files(self) -> list[Path]:
        """List all contribution files in the run.

        Returns:
            Sorted list of contribution file paths.
        """
        contribution_dir = self.run_path / "analysis" / "contribution"
        if not contribution_dir.exists():
            return []
        return sorted(contribution_dir.glob("*.jsonl"))

    def load_contribution_file(self, filename: str) -> list[dict[str, Any]]:
        """Load contribution records from a specific file.

        Args:
            filename: Contribution filename (with .jsonl extension).

        Returns:
            List of contribution records.
        """
        path = self.run_path / "analysis" / "contribution" / filename
        if not path.exists():
            return []

        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def load_all_raw_csvs(self) -> dict[str, pd.DataFrame]:
        """Load all CSV files from source data directory.

        Returns:
            Dict mapping filename (without extension) to DataFrame.
        """
        result: dict[str, pd.DataFrame] = {}
        for csv_path in self.source_data_path.glob("*.csv"):
            result[csv_path.stem] = self.load_raw_csv(csv_path.name)
        return result
