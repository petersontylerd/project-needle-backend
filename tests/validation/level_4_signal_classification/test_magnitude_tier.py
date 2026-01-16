"""Validate magnitude tier assignment against first-principles recalculation."""

from __future__ import annotations

from typing import Any

import pytest

from tests.validation.helpers.independent_calculators import (
    IndependentClassificationCalculator,
)


class TestMagnitudeTierRecalculation:
    """Validate magnitude tier assignments across all classifications."""

    def test_magnitude_tier_from_percentile_rank(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Recalculated magnitude tier matches stored values.

        Thresholds (from src/analytics/signal_classification/thresholds.py):
        - critical: >= 99.0
        - severe: 95.0 - 98.9
        - elevated: 85.0 - 94.9
        - marginal: 75.0 - 84.9
        - expected: 25.0 - 74.9
        - favorable: 10.0 - 24.9
        - excellent: < 10.0
        """

        calc = IndependentClassificationCalculator()
        discrepancies: list[dict[str, Any]] = []
        validated_count = 0

        for record in all_classifications:
            # Extract percentile_rank from contributing_factors
            percentile_rank = None
            for factor in record.get("contributing_factors", []):
                if factor.get("factor") == "percentile_rank":
                    percentile_rank = factor.get("value")
                    break

            stored_tier = record.get("magnitude_tier")

            # Skip if missing data
            if percentile_rank is None or stored_tier is None:
                continue

            recalculated_tier = calc.magnitude_tier(percentile_rank)
            validated_count += 1

            if stored_tier != recalculated_tier:
                discrepancies.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "node_id": record.get("node_id"),
                        "percentile_rank": percentile_rank,
                        "stored_tier": stored_tier,
                        "recalculated_tier": recalculated_tier,
                    }
                )

        if validated_count == 0:
            pytest.skip("No magnitude tiers with percentile_rank data to validate")

        if discrepancies:
            sample = discrepancies[:10]
            pytest.fail(f"{len(discrepancies)} magnitude tier discrepancies found (validated {validated_count} records). Sample: {sample}")

    def test_magnitude_tier_distribution_has_variety(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Magnitude tier distribution has multiple tiers represented.

        We expect at least a few different tiers to be represented
        in a valid classification output.
        """

        tier_counts: dict[str, int] = {}

        for record in all_classifications:
            tier = record.get("magnitude_tier")
            if tier:
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

        if not tier_counts:
            pytest.skip("No magnitude tiers found in classifications")

        # Should have multiple tiers represented - at least 2 for variety
        non_empty_tiers = len([t for t, c in tier_counts.items() if c > 0])
        assert non_empty_tiers >= 2, f"Expected at least 2 different tiers, got {non_empty_tiers}. Distribution: {tier_counts}"

    def test_all_magnitude_tiers_are_valid(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """All magnitude tiers are from the valid set."""

        valid_tiers = {"critical", "severe", "elevated", "marginal", "expected", "favorable", "excellent"}
        invalid_records: list[dict[str, Any]] = []

        for record in all_classifications:
            tier = record.get("magnitude_tier")
            if tier and tier not in valid_tiers:
                invalid_records.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "magnitude_tier": tier,
                    }
                )

        if invalid_records:
            pytest.fail(f"{len(invalid_records)} records have invalid magnitude tiers. Sample: {invalid_records[:5]}")
