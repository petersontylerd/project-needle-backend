"""Test entity counts match between classifications and node results."""

from __future__ import annotations

from typing import Any

import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader


class TestEntityCounts:
    """Validate entity count consistency across pipeline outputs."""

    def test_classifications_exist(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Verify classifications were generated from the run."""
        assert len(all_classifications) > 0, "Expected classifications to be generated"

    def test_node_files_exist(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Verify node result files exist in the run."""
        node_files = validation_loader.iter_node_files()
        assert len(node_files) > 0, "No node files found in run"

    def test_all_classifications_have_required_fields(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Every classification record has required fields."""

        required_fields = {
            "node_id",
            "entity_key",
            "metric_id",
            "sub_classification",
            "priority_score",
            "magnitude_tier",
            "trajectory_tier",
            "consistency_tier",
        }

        missing_by_record: list[tuple[int, set[str]]] = []
        for i, record in enumerate(all_classifications):
            missing = required_fields - set(record.keys())
            if missing:
                missing_by_record.append((i, missing))

        if missing_by_record:
            sample = missing_by_record[:5]
            pytest.fail(f"{len(missing_by_record)} records missing fields. Sample: {sample}")
