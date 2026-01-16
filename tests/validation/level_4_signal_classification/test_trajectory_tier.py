"""Validate trajectory tier assignment against first-principles recalculation."""

from __future__ import annotations

from typing import Any

import pytest


class TestTrajectoryTierRecalculation:
    """Validate trajectory tier assignments across all classifications."""

    def test_trajectory_tier_consistent_with_slope_percentile(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Trajectory tier is consistent with slope percentile direction.

        Note: Rapid tiers (rapidly_deteriorating, rapidly_improving) require
        an is_accelerating flag that's not in the classification output.
        We validate that:
        - High percentiles (>= 70) => deteriorating or rapidly_deteriorating
        - Low percentiles (< 30) => improving or rapidly_improving
        - Middle percentiles (30-70) => stable
        """

        violations: list[dict[str, Any]] = []
        validated_count = 0

        for record in all_classifications:
            # Extract slope_percentile from contributing_factors
            slope_percentile = None
            for factor in record.get("contributing_factors", []):
                if factor.get("factor") == "slope_percentile":
                    slope_percentile = factor.get("value")
                    break

            stored_tier = record.get("trajectory_tier")

            # Skip if missing data
            if slope_percentile is None or stored_tier is None:
                continue

            validated_count += 1

            # Validate tier is consistent with percentile direction
            is_high = slope_percentile >= 70
            is_low = slope_percentile < 30

            if is_high and stored_tier not in {"deteriorating", "rapidly_deteriorating"}:
                violations.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "slope_percentile": slope_percentile,
                        "stored_tier": stored_tier,
                        "expected": "deteriorating or rapidly_deteriorating",
                    }
                )
            elif is_low and stored_tier not in {"improving", "rapidly_improving"}:
                violations.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "slope_percentile": slope_percentile,
                        "stored_tier": stored_tier,
                        "expected": "improving or rapidly_improving",
                    }
                )
            elif not is_high and not is_low and stored_tier != "stable":
                violations.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "slope_percentile": slope_percentile,
                        "stored_tier": stored_tier,
                        "expected": "stable",
                    }
                )

        if validated_count == 0:
            pytest.skip("No trajectory tiers with slope_percentile data to validate")

        if violations:
            sample = violations[:10]
            pytest.fail(f"{len(violations)} trajectory tier violations found (validated {validated_count} records). Sample: {sample}")

    def test_trajectory_tier_distribution_has_variety(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Trajectory tier distribution has multiple tiers represented.

        We expect at least a couple different tiers to be represented.
        """

        tier_counts: dict[str, int] = {}

        for record in all_classifications:
            tier = record.get("trajectory_tier")
            if tier:
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

        if not tier_counts:
            pytest.skip("No trajectory tiers found in classifications")

        # Should have at least 2 tiers represented
        non_empty_tiers = len([t for t, c in tier_counts.items() if c > 0])
        assert non_empty_tiers >= 2, f"Expected at least 2 different tiers, got {non_empty_tiers}. Distribution: {tier_counts}"

    def test_all_trajectory_tiers_are_valid(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """All trajectory tiers are from the valid set."""

        valid_tiers = {"rapidly_deteriorating", "deteriorating", "stable", "improving", "rapidly_improving"}
        invalid_records: list[dict[str, Any]] = []

        for record in all_classifications:
            tier = record.get("trajectory_tier")
            if tier and tier not in valid_tiers:
                invalid_records.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "trajectory_tier": tier,
                    }
                )

        if invalid_records:
            pytest.fail(f"{len(invalid_records)} records have invalid trajectory tiers. Sample: {invalid_records[:5]}")
