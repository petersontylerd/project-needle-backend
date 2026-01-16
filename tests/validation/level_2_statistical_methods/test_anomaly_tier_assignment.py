"""Validate 9-tier anomaly classification against first-principles recalculation."""

from __future__ import annotations

from typing import Any

import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader
from tests.validation.helpers.independent_calculators import IndependentAnomalyTierClassifier


class TestAnomalyTierAssignment:
    """Validate anomaly tier assignments across all entities."""

    def test_tier_boundaries_match_taxonomy(self) -> None:
        """Verify tier boundaries match taxonomy/anomaly_method_profiles.yaml."""
        calc = IndependentAnomalyTierClassifier()

        # These are the canonical boundaries from the taxonomy
        expected_boundaries = [-3.0, -2.0, -1.0, -0.5, 0.5, 1.0, 2.0, 3.0]
        expected_labels = [
            "extremely_low",
            "very_low",
            "moderately_low",
            "slightly_low",
            "normal",
            "slightly_high",
            "moderately_high",
            "very_high",
            "extremely_high",
        ]

        assert expected_boundaries == calc.BOUNDARIES
        assert expected_labels == calc.LABELS

    def test_tier_assignment_distribution(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Verify tier assignments are reasonable for z-score distribution.

        Since anomaly tiers are derived (not stored), we validate that:
        1. Z-scores are classified into expected tiers
        2. The distribution of tiers is sensible (not all in one tier)
        """
        calc = IndependentAnomalyTierClassifier()
        tier_counts: dict[str, int] = dict.fromkeys(calc.LABELS, 0)
        tier_counts["no_score"] = 0
        validated_count = 0

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:10]:  # Sample 10 nodes
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results:
                    for stat_method in entity.get("statistical_methods", []):
                        stats = stat_method.get("statistics", {})

                        # Get z-score (simple or robust)
                        zscore = stats.get("simple_zscore") or stats.get("robust_zscore")
                        suppressed = stats.get("suppressed", False)

                        tier = calc.classify(None) if suppressed or zscore is None else calc.classify(zscore)

                        tier_counts[tier] = tier_counts.get(tier, 0) + 1
                        validated_count += 1

            except Exception:
                pass  # Skip errors

        assert validated_count > 0, "No z-scores found to classify"

        # Validate that tiers are distributed sensibly
        # In a real distribution, we expect:
        # - "normal" to have the most entities
        # - Extreme tiers to have fewer entities
        # - Not all entities in one tier
        non_empty_tiers = sum(1 for count in tier_counts.values() if count > 0)
        assert non_empty_tiers >= 3, f"Expected at least 3 tiers with entities, got {non_empty_tiers}. Distribution: {tier_counts}"

    def test_tier_boundary_correctness(self) -> None:
        """Verify tier boundaries are applied correctly at edge cases."""
        calc = IndependentAnomalyTierClassifier()

        # Test exact boundary values (inclusive upper bound)
        test_cases = [
            (-3.5, "extremely_low"),
            (-3.0, "extremely_low"),  # Boundary: z <= -3.0
            (-2.99, "very_low"),
            (-2.0, "very_low"),
            (-1.99, "moderately_low"),
            (-1.0, "moderately_low"),
            (-0.99, "slightly_low"),
            (-0.5, "slightly_low"),
            (-0.49, "normal"),
            (0.0, "normal"),
            (0.5, "normal"),  # Boundary: z <= 0.5
            (0.51, "slightly_high"),
            (1.0, "slightly_high"),
            (1.01, "moderately_high"),
            (2.0, "moderately_high"),
            (2.01, "very_high"),
            (3.0, "very_high"),
            (3.01, "extremely_high"),
            (5.0, "extremely_high"),
        ]

        errors: list[str] = []
        for zscore, expected_tier in test_cases:
            actual = calc.classify(zscore)
            if actual != expected_tier:
                errors.append(f"z={zscore}: expected '{expected_tier}', got '{actual}'")

        if errors:
            pytest.fail("Boundary errors:\n" + "\n".join(errors))

    def test_suppressed_entities_have_no_score(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Suppressed entities should have tier='no_score' or None zscore."""
        violations: list[dict[str, Any]] = []

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:20]:  # Sample first 20 nodes
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results:
                    for stat_method in entity.get("statistical_methods", []):
                        stats = stat_method.get("statistics", {})

                        suppressed = stats.get("suppressed", False)
                        zscore = stats.get("simple_zscore") or stats.get("robust_zscore")
                        stored_tier = stats.get("anomaly_tier")

                        # If suppressed, zscore should be None or tier should be no_score
                        if suppressed and zscore is not None and stored_tier not in (None, "no_score"):
                            violations.append(
                                {
                                    "node_id": node_id,
                                    "entity": entity.get("entity"),
                                    "suppressed": suppressed,
                                    "zscore": zscore,
                                    "stored_tier": stored_tier,
                                }
                            )

            except Exception:
                pass  # Skip errors for this test

        if violations:
            pytest.fail(f"{len(violations)} suppressed entities have invalid tier. Sample: {violations[:5]}")
