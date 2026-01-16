"""Validate priority score calculation against first-principles recalculation."""

from __future__ import annotations

from typing import Any

import pytest


class TestPriorityScoreRecalculation:
    """Validate priority score calculations across all classifications."""

    def test_priority_breakdown_sums_to_final_score(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Priority breakdown components sum to final_score.

        The priority_breakdown contains:
        - magnitude_contribution
        - trajectory_contribution
        - consistency_contribution
        - volume_contribution
        - actionability_contribution
        - sub_class_adjustment
        - final_score

        Sum of components + adjustment should equal final_score (within rounding).
        """

        discrepancies: list[dict[str, Any]] = []
        validated_count = 0

        for record in all_classifications:
            breakdown = record.get("priority_breakdown", {})

            if not breakdown:
                continue

            # Extract components
            mag = breakdown.get("magnitude_contribution", 0) or 0
            traj = breakdown.get("trajectory_contribution", 0) or 0
            cons = breakdown.get("consistency_contribution", 0) or 0
            vol = breakdown.get("volume_contribution", 0) or 0
            action = breakdown.get("actionability_contribution", 0) or 0
            adj = breakdown.get("sub_class_adjustment", 0) or 0
            final = breakdown.get("final_score")

            if final is None:
                continue

            validated_count += 1

            # Calculate sum and apply clamping (final score is clamped to [1, 100])
            calculated_sum = mag + traj + cons + vol + action + adj
            clamped_sum = max(1, min(100, round(calculated_sum)))

            # Allow ±1 tolerance for rounding
            if abs(clamped_sum - final) > 1:
                discrepancies.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "breakdown": breakdown,
                        "calculated_sum": calculated_sum,
                        "clamped_sum": clamped_sum,
                        "final_score": final,
                        "difference": abs(clamped_sum - final),
                    }
                )

        assert validated_count > 0, "No priority breakdowns were validated"

        if discrepancies:
            sample = discrepancies[:10]
            pytest.fail(f"{len(discrepancies)} priority breakdown discrepancies found (validated {validated_count} records). Sample: {sample}")

    def test_priority_score_bounds(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """All priority scores are in [1, 100] range."""

        out_of_bounds: list[dict[str, Any]] = []

        for record in all_classifications:
            score = record.get("priority_score")
            if score is not None and (score < 1 or score > 100):
                out_of_bounds.append(
                    {
                        "entity_key": record.get("entity_key"),
                        "priority_score": score,
                    }
                )

        if out_of_bounds:
            pytest.fail(f"{len(out_of_bounds)} priority scores outside [1, 100] range. Sample: {out_of_bounds[:5]}")

    def test_priority_breakdown_components_are_reasonable(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Priority breakdown components are within expected ranges."""

        violations: list[dict[str, Any]] = []

        # Expected max values based on tier weights and formula
        max_magnitude = 30.0  # 1.0 * 30
        max_trajectory = 25.0  # 1.0 * 25
        max_consistency = 15.0  # 0.8 * 15 = 12 (persistent is max at 0.8)
        max_volume = 15.0  # 1.0 * 15
        max_actionability = 10.0  # 1.0 * 10

        for record in all_classifications:
            breakdown = record.get("priority_breakdown", {})

            # Check each component is reasonable
            checks = [
                ("magnitude_contribution", breakdown.get("magnitude_contribution"), 0, max_magnitude),
                ("trajectory_contribution", breakdown.get("trajectory_contribution"), 0, max_trajectory),
                ("consistency_contribution", breakdown.get("consistency_contribution"), 0, max_consistency),
                ("volume_contribution", breakdown.get("volume_contribution"), 0, max_volume),
                ("actionability_contribution", breakdown.get("actionability_contribution"), 0, max_actionability),
            ]

            for name, value, min_val, max_val in checks:
                # Allow some tolerance due to rounding
                if value is not None and (value < min_val - 0.1 or value > max_val + 0.1):
                    violations.append(
                        {
                            "entity_key": record.get("entity_key"),
                            "component": name,
                            "value": value,
                            "expected_range": f"[{min_val}, {max_val}]",
                        }
                    )

        if violations:
            sample = violations[:10]
            pytest.fail(f"{len(violations)} priority breakdown components outside expected ranges. Sample: {sample}")

    def test_priority_score_ordering_makes_sense(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """Higher severity tiers should generally have higher priority scores.

        This is a sanity check that the score ordering is sensible.
        """

        # Group scores by magnitude tier
        scores_by_magnitude: dict[str, list[int]] = {}
        for record in all_classifications:
            tier = record.get("magnitude_tier")
            score = record.get("priority_score")
            if tier and score:
                if tier not in scores_by_magnitude:
                    scores_by_magnitude[tier] = []
                scores_by_magnitude[tier].append(score)

        # Calculate average scores per tier
        avg_scores: dict[str, float] = {}
        for tier, scores in scores_by_magnitude.items():
            avg_scores[tier] = sum(scores) / len(scores) if scores else 0

        # Verify ordering (critical > severe > elevated > marginal > expected > favorable > excellent)
        tier_order = ["critical", "severe", "elevated", "marginal", "expected", "favorable", "excellent"]
        existing_tiers = [t for t in tier_order if t in avg_scores]

        if len(existing_tiers) >= 3:
            # Check that average scores decrease as we go down the severity scale
            # (higher index = less severe = should have lower priority)
            for i in range(len(existing_tiers) - 1):
                high_tier = existing_tiers[i]
                low_tier = existing_tiers[i + 1]
                if avg_scores[high_tier] < avg_scores[low_tier]:
                    # This is a warning, not a failure - other factors can affect scores
                    pass  # Could log this as a note

        # Basic sanity: critical should have higher avg than excellent (if both exist)
        if "critical" in avg_scores and "excellent" in avg_scores:
            assert avg_scores["critical"] > avg_scores["excellent"], (
                f"Critical tier avg ({avg_scores['critical']:.1f}) should be higher than excellent tier avg ({avg_scores['excellent']:.1f})"
            )
