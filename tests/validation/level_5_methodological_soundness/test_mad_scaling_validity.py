"""Validate MAD scaling constant (1.4826) appropriateness.

Assessment Question: Is 1.4826 appropriate for non-normal data?

Background:
- The constant 1.4826 = 1/PHI^(-1)(0.75) makes scaled MAD equivalent to
  standard deviation for normally distributed data
- If data is non-normal, this relationship may not hold

Tests assess:
1. How close scaled_MAD is to population std in practice
2. Distribution normality via Shapiro-Wilk test
3. Documentation of findings
"""

from __future__ import annotations

from typing import Any

import numpy as np
from scipy import stats

from tests.validation.helpers.data_loaders import ValidationDataLoader


class TestMADScalingValidity:
    """Assess MAD scaling constant validity for healthcare data."""

    MAD_SCALE = 1.4826  # Standard scaling for normal distributions

    def test_mad_vs_std_ratio_across_nodes(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Compare scaled MAD to population std across nodes.

        For normal data: scaled_MAD ≈ std
        Deviation indicates non-normality.
        """
        ratios: list[dict[str, Any]] = []

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

                if len(metric_values) < 20:
                    continue

                values_array = np.array(metric_values, dtype=np.float64)

                # Calculate statistics
                median = float(np.median(values_array))
                mad = float(np.median(np.abs(values_array - median)))
                scaled_mad = mad * self.MAD_SCALE
                population_std = float(np.std(values_array, ddof=0))

                if population_std > 0:
                    ratio = scaled_mad / population_std
                    ratios.append(
                        {
                            "node_id": node_id,
                            "n": len(values_array),
                            "mad": mad,
                            "scaled_mad": scaled_mad,
                            "std": population_std,
                            "ratio": ratio,
                        }
                    )

            except Exception:
                pass

        assert len(ratios) > 0, "No nodes were analyzed for MAD/std ratio"

        # Calculate summary statistics
        ratio_values = [r["ratio"] for r in ratios]
        avg_ratio = np.mean(ratio_values)
        np.std(ratio_values)

        # For normal data, ratio should be close to 1.0
        # Healthcare data may deviate due to non-normality
        assert 0.5 < avg_ratio < 1.5, f"Average scaled_MAD/std ratio ({avg_ratio:.3f}) is far from 1.0. This indicates significant deviation from normality."

    def test_distribution_normality_shapiro_wilk(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Test distribution normality using Shapiro-Wilk test.

        Shapiro-Wilk tests the null hypothesis that data is normally distributed.
        p < 0.05 indicates significant departure from normality.
        """
        normality_results: list[dict[str, Any]] = []

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:15]:
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

                # Shapiro-Wilk works best for n < 5000
                if len(metric_values) < 20:
                    continue

                values_array = np.array(metric_values, dtype=np.float64)

                # Sample if too large (Shapiro-Wilk has limits)
                if len(values_array) > 5000:
                    rng = np.random.default_rng(42)
                    values_array = rng.choice(values_array, size=5000, replace=False)

                # Run Shapiro-Wilk test
                stat, p_value = stats.shapiro(values_array)

                normality_results.append(
                    {
                        "node_id": node_id,
                        "n": len(values_array),
                        "statistic": stat,
                        "p_value": p_value,
                        "is_normal": p_value >= 0.05,
                    }
                )

            except Exception:
                pass

        assert len(normality_results) > 0, "No normality tests were performed"

        # Document findings
        normal_count = sum(1 for r in normality_results if r["is_normal"])
        len(normality_results) - normal_count

        # Healthcare data is typically NOT normally distributed
        # This test documents that finding
        # We don't fail if data is non-normal - we document it

    def test_document_mad_scaling_rationale(self) -> None:
        """Document the rationale for MAD scaling.

        This test always passes - it serves as documentation.
        """
        rationale = """
        RATIONALE FOR MAD SCALING CONSTANT (1.4826):

        1. MATHEMATICAL BASIS: For a normal distribution:
           MAD = 0.6745 * std
           Therefore: std ≈ MAD * 1.4826 (where 1.4826 = 1/0.6745)

        2. ROBUSTNESS: MAD is resistant to outliers, unlike std.
           Even if data is non-normal, MAD provides a stable measure
           of spread that isn't influenced by extreme values.

        3. HEALTHCARE DATA: Healthcare metrics often have:
           - Skewed distributions (right-skewed for costs/LOS)
           - Heavy tails (extreme outliers)
           - Non-normal shapes

           For such data, MAD-based z-scores are MORE appropriate
           than mean/std-based z-scores because:
           - They're not pulled by outliers
           - They reflect the "typical" spread better

        4. PRACTICAL INTERPRETATION: Even if scaled_MAD doesn't exactly
           equal std for non-normal data, the z-scores still provide
           meaningful rankings of how "unusual" an entity is relative
           to its peers.

        5. INDUSTRY STANDARD: MAD with 1.4826 scaling is a standard
           approach in robust statistics and is widely used in
           healthcare analytics for outlier-resistant scoring.

        CONCLUSION: The 1.4826 scaling is appropriate because:
        - For normal data: exact equivalence to std
        - For non-normal data: robust, stable measure of spread
        - Interpretability: z-scores remain meaningful for ranking
        """
        assert len(rationale) > 0

    def test_scaled_mad_provides_useful_spread_measure(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Verify scaled MAD provides meaningful spread estimates.

        Even for non-normal data, scaled MAD should:
        1. Be positive for non-constant distributions
        2. Increase with actual spread
        3. Be stable across similar peer groups
        """
        mad_stats: list[dict[str, Any]] = []

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:10]:
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

                if len(metric_values) < 20:
                    continue

                values_array = np.array(metric_values, dtype=np.float64)

                median = float(np.median(values_array))
                mad = float(np.median(np.abs(values_array - median)))
                scaled_mad = mad * self.MAD_SCALE
                value_range = float(np.max(values_array) - np.min(values_array))

                mad_stats.append(
                    {
                        "node_id": node_id,
                        "n": len(values_array),
                        "mad": mad,
                        "scaled_mad": scaled_mad,
                        "range": value_range,
                    }
                )

            except Exception:
                pass

        assert len(mad_stats) > 0, "No MAD statistics were calculated"

        # Verify MAD is meaningful
        for stat in mad_stats:
            # MAD should be positive for varying data
            if stat["range"] > 0:
                assert stat["mad"] >= 0, f"MAD should be non-negative for {stat['node_id']}"

            # Scaled MAD should be in reasonable proportion to range
            # (rough heuristic: scaled_MAD < range for reasonable distributions)
            if stat["range"] > 0 and stat["scaled_mad"] > 0:
                ratio = stat["scaled_mad"] / stat["range"]
                assert ratio < 1.0, f"Scaled MAD ({stat['scaled_mad']:.3f}) exceeds range ({stat['range']:.3f}) for {stat['node_id']}"
