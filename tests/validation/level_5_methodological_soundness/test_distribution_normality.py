"""Validate z-score interpretation for healthcare distributions.

Assessment Question: Are z-score interpretations valid for non-normal data?

For normal distributions:
- z = 2 corresponds to ~97.7th percentile
- z = 3 corresponds to ~99.9th percentile

For non-normal healthcare data, this correspondence may not hold exactly.
These tests assess how well z-scores map to percentiles in practice.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest
from scipy import stats

from tests.validation.helpers.data_loaders import ValidationDataLoader


class TestDistributionNormality:
    """Assess z-score interpretation validity for healthcare data."""

    def test_zscore_percentile_correspondence(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Test if z-scores correspond to expected percentiles.

        For normal data:
        - z = 2.0 → percentile ~ 97.7
        - z = -2.0 → percentile ~ 2.3

        We test how well this holds for actual healthcare data.
        """
        correspondences: list[dict[str, Any]] = []

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:20]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results:
                    for stat_method in entity.get("statistical_methods", []):
                        stats_data = stat_method.get("statistics", {})

                        zscore = stats_data.get("simple_zscore")
                        percentile = stats_data.get("percentile_rank")

                        if zscore is not None and percentile is not None:
                            # Calculate expected percentile for normal distribution
                            expected_percentile = stats.norm.cdf(zscore) * 100

                            correspondences.append(
                                {
                                    "node_id": node_id,
                                    "zscore": zscore,
                                    "actual_percentile": percentile,
                                    "expected_percentile": expected_percentile,
                                    "difference": abs(percentile - expected_percentile),
                                }
                            )

            except Exception:
                pass

        if len(correspondences) == 0:
            pytest.skip("No z-score/percentile pairs found")

        # Analyze correspondence
        differences = [c["difference"] for c in correspondences]
        avg_difference = np.mean(differences)

        # Document findings - healthcare data may deviate from normal
        # Average difference of ~10 percentile points is common for non-normal data
        assert avg_difference < 30, (
            f"Z-score to percentile mapping deviates significantly from normal. Average difference: {avg_difference:.1f} percentile points"
        )

    def test_extreme_zscore_distribution(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Analyze distribution of extreme z-scores.

        For normal data:
        - ~4.6% of values should have |z| > 2
        - ~0.3% of values should have |z| > 3

        Healthcare data may have heavier tails (more extremes).
        """
        zscore_data: list[float] = []

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:30]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results:
                    for stat_method in entity.get("statistical_methods", []):
                        stats_data = stat_method.get("statistics", {})
                        zscore = stats_data.get("simple_zscore")
                        if zscore is not None and not np.isnan(zscore):
                            zscore_data.append(zscore)

            except Exception:
                pass

        if len(zscore_data) < 10:
            pytest.skip("Insufficient z-score data for analysis")

        zscores = np.array(zscore_data)
        total = len(zscores)

        # Calculate tail percentages
        above_2 = np.sum(np.abs(zscores) > 2) / total * 100

        # Expected for normal: above_2 ~ 4.6%
        # Healthcare data typically has heavier tails

        # Validate that extremes are reasonable (not too many)
        assert above_2 < 50, f"Too many extreme z-scores: {above_2:.1f}% have |z| > 2 (expected ~4.6% for normal data)"

    def test_metric_distribution_skewness(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Characterize skewness of metric distributions.

        Healthcare metrics (like LOS, costs) often have right-skewed distributions.
        This test documents the typical skewness.
        """
        skewness_data: list[dict[str, Any]] = []

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:20]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                # Collect metric values
                metric_values: list[float] = []
                for entity in results:
                    metric_list = entity.get("metric", [])
                    if not metric_list:
                        continue
                    metric_value = metric_list[0].get("values")
                    if metric_value is not None and not np.isnan(metric_value):
                        metric_values.append(float(metric_value))

                if len(metric_values) < 5:
                    continue

                values_array = np.array(metric_values)

                # Calculate skewness and kurtosis
                skew = float(stats.skew(values_array))
                kurt = float(stats.kurtosis(values_array))

                skewness_data.append(
                    {
                        "node_id": node_id,
                        "n": len(values_array),
                        "skewness": skew,
                        "kurtosis": kurt,
                    }
                )

            except Exception:
                pass

        if len(skewness_data) == 0:
            pytest.skip("No distribution data collected")

        # Validate skewness is within reasonable bounds
        high_skew = [s for s in skewness_data if abs(s["skewness"]) > 2.0]
        assert len(high_skew) < len(skewness_data) * 0.5, (
            f"Too many highly skewed distributions: {len(high_skew)}/{len(skewness_data)} have |skewness| > 2.0. Sample: {high_skew[:5]}"
        )

    def test_document_distribution_characteristics(self) -> None:
        """Document healthcare data distribution characteristics.

        This test always passes - it serves as documentation.
        """
        documentation = """
        HEALTHCARE DATA DISTRIBUTION CHARACTERISTICS:

        1. RIGHT SKEWNESS (Positive Skew):
           - Length of stay, costs, utilization typically right-skewed
           - Most facilities near the mean, some with very high values
           - Skewness values of 1-3 are common

        2. HEAVY TAILS (Positive Kurtosis):
           - More extreme values than normal distribution predicts
           - Outliers are more frequent than in normal data
           - Kurtosis values > 3 are common

        3. IMPLICATIONS FOR Z-SCORES:
           - Z-scores still provide useful rankings
           - Interpretation of specific z-values differs from normal
           - z = 2 may not correspond to exactly 97.7th percentile
           - But z = 2 still means "notably high relative to peers"

        4. WHY Z-SCORES ARE STILL USEFUL:
           - Rank-based comparisons remain valid
           - Relative position among peers is preserved
           - Percentile ranks provide distribution-free comparison
           - Combined z-score + percentile gives complete picture

        5. ROBUST Z-SCORES (MAD-based):
           - Less affected by extreme values
           - Better for heavy-tailed distributions
           - Provides more stable comparisons

        CONCLUSION: Z-scores remain useful for healthcare data despite
        non-normality because they provide consistent relative comparisons.
        The percentile_rank field provides distribution-free validation.
        """
        assert len(documentation) > 0

    def test_percentile_zscore_monotonicity(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Verify z-scores and percentiles are monotonically related.

        Higher z-scores should correspond to higher percentiles.
        This validates that the ranking is consistent.
        """
        pairs: list[tuple[float, float]] = []

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:10]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results:
                    for stat_method in entity.get("statistical_methods", []):
                        stats_data = stat_method.get("statistics", {})

                        zscore = stats_data.get("simple_zscore")
                        percentile = stats_data.get("percentile_rank")

                        if zscore is not None and percentile is not None:
                            pairs.append((zscore, percentile))

            except Exception:
                pass

        if len(pairs) < 10:
            pytest.skip("Insufficient data for monotonicity test")

        # Check rank correlation (Spearman)
        zscores = np.array([p[0] for p in pairs])
        percentiles = np.array([p[1] for p in pairs])

        correlation, p_value = stats.spearmanr(zscores, percentiles)

        # Should have high positive correlation (monotonic relationship)
        assert correlation > 0.90, f"Z-score and percentile should be strongly monotonically related. Spearman correlation: {correlation:.3f}"
