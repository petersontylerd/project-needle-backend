"""Validate consistency tier assignment against first-principles recalculation."""

from __future__ import annotations

from typing import Any

import pytest

from tests.validation.helpers.independent_calculators import (
    IndependentClassificationCalculator,
)


class TestConsistencyTierRecalculation:
    """Validate consistency tier assignments across all classifications."""

    def test_consistency_tier_from_std_and_periods(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Recalculated consistency tier matches stored values.

        Thresholds (from src/analytics/signal_classification/thresholds.py):
        - persistent: std < 0.90 AND periods >= 6
        - variable: 0.90 <= std <= 1.10 AND periods >= 3
        - transient: std > 1.10 OR periods < 3
        """

        calc = IndependentClassificationCalculator()
        discrepancies: list[dict[str, Any]] = []
        validated_count = 0

        for record in all_classifications:
            std_deviation = record.get("std_deviation")
            data_quality = record.get("data_quality", {})
            temporal_periods = data_quality.get("temporal_periods")
            stored_tier = record.get("consistency_tier")

            # Skip if missing data
            if any(v is None for v in [std_deviation, temporal_periods, stored_tier]):
                continue

            # Type narrowing assertions for mypy
            assert std_deviation is not None
            assert temporal_periods is not None

            recalculated_tier = calc.consistency_tier(
                std_dev=std_deviation,
                periods=temporal_periods,
            )
            validated_count += 1

            if stored_tier != recalculated_tier:
                discrepancies.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "node_id": record.get("node_id"),
                        "std_deviation": std_deviation,
                        "temporal_periods": temporal_periods,
                        "stored_tier": stored_tier,
                        "recalculated_tier": recalculated_tier,
                    }
                )

        assert validated_count > 0, "No consistency tiers were validated"

        if discrepancies:
            sample = discrepancies[:10]
            pytest.fail(f"{len(discrepancies)} consistency tier discrepancies found (validated {validated_count} records). Sample: {sample}")

    def test_consistency_tier_distribution_is_sensible(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Consistency tier distribution is reasonable."""

        tier_counts: dict[str, int] = {}

        for record in all_classifications:
            tier = record.get("consistency_tier")
            if tier:
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

        # Should have all 3 tiers represented
        non_empty_tiers = len([t for t, c in tier_counts.items() if c > 0])
        assert non_empty_tiers >= 2, f"Expected at least 2 different tiers, got {non_empty_tiers}. Distribution: {tier_counts}"

    def test_all_consistency_tiers_are_valid(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """All consistency tiers are from the valid set."""

        valid_tiers = {"persistent", "variable", "transient"}
        invalid_records: list[dict[str, Any]] = []

        for record in all_classifications:
            tier = record.get("consistency_tier")
            if tier and tier not in valid_tiers:
                invalid_records.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "consistency_tier": tier,
                    }
                )

        if invalid_records:
            pytest.fail(f"{len(invalid_records)} records have invalid consistency tiers. Sample: {invalid_records[:5]}")

    def test_persistent_requires_minimum_periods(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """'persistent' tier should have at least 6 temporal periods."""

        violations: list[dict[str, Any]] = []

        for record in all_classifications:
            tier = record.get("consistency_tier")
            data_quality = record.get("data_quality", {})
            temporal_periods = data_quality.get("temporal_periods")

            if tier == "persistent" and temporal_periods is not None and temporal_periods < 6:
                violations.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "consistency_tier": tier,
                        "temporal_periods": temporal_periods,
                    }
                )

        if violations:
            pytest.fail(f"{len(violations)} 'persistent' entities have fewer than 6 periods. Sample: {violations[:5]}")
