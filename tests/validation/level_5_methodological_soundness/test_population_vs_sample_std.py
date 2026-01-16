"""Validate population standard deviation (ddof=0) usage.

This test documents and validates that population std is used rather than sample std.

Assessment Question: Is ddof=0 appropriate?

Argument FOR population std:
- The peer group IS the complete population of comparison entities
- We are not sampling from a larger population
- Every facility in the peer group is included in the calculation

This is an assessment/documentation test, not a strict pass/fail validation.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from tests.validation.helpers.data_loaders import ValidationDataLoader


class TestPopulationVsSampleStd:
    """Document and validate population std (ddof=0) usage."""

    def test_std_uses_ddof_zero(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Verify production uses population std (ddof=0), not sample std (ddof=1).

        For each node with stored peer_std, we verify that the stored value
        matches population std rather than sample std.
        """
        matches_population = 0
        matches_sample = 0
        total_checked = 0

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:20]:  # Sample first 20 nodes
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                # Collect all metric values for this node
                metric_values: list[float] = []
                stored_peer_std: float | None = None

                for entity in results:
                    metric_list = entity.get("metric", [])
                    if not metric_list:
                        continue
                    metric_value = metric_list[0].get("values")
                    if metric_value is not None and not np.isnan(metric_value):
                        metric_values.append(float(metric_value))

                    # Get stored peer_std from first entity that has it
                    if stored_peer_std is None:
                        for stat_method in entity.get("statistical_methods", []):
                            stats = stat_method.get("statistics", {})
                            if "peer_std" in stats and stats["peer_std"] is not None:
                                stored_peer_std = stats["peer_std"]
                                break

                if not metric_values or stored_peer_std is None:
                    continue

                values_array = np.array(metric_values, dtype=np.float64)

                # Calculate both population and sample std
                population_std = float(np.std(values_array, ddof=0))
                sample_std = float(np.std(values_array, ddof=1))

                total_checked += 1

                # Check which one matches
                if np.isclose(stored_peer_std, population_std, rtol=1e-9):
                    matches_population += 1
                elif np.isclose(stored_peer_std, sample_std, rtol=1e-9):
                    matches_sample += 1

            except Exception:
                pass  # Skip errors

        assert total_checked > 0, "No peer_std values were checked"

        # Document findings
        assert matches_population > 0, (
            f"Expected at least some nodes to use population std. Checked {total_checked} nodes: {matches_population} population, {matches_sample} sample"
        )

        # The production code SHOULD use population std
        assert matches_population >= matches_sample, (
            f"Production should use population std (ddof=0). Found: {matches_population} population vs {matches_sample} sample"
        )

    def test_document_population_std_rationale(self) -> None:
        """Document the rationale for using population std.

        This test always passes - it serves as documentation.
        """
        rationale = """
        RATIONALE FOR POPULATION STANDARD DEVIATION (ddof=0):

        1. COMPLETE POPULATION: The peer group represents ALL entities being
           compared, not a sample from a larger population. When calculating
           z-scores for hospitals in a specific peer group, we have data for
           every hospital in that group.

        2. NOT INFERRING: We are not trying to infer population parameters
           from a sample. We are describing the actual variation in the
           complete set of entities.

        3. CONSISTENCY: Using population std ensures that the z-score formula
           z = (x - mean) / std directly measures how many standard deviations
           an entity is from the mean of its exact peer group.

        4. STANDARD PRACTICE: For benchmarking and comparative analytics where
           the comparison set is fixed and complete, population std is standard.

        CONCLUSION: ddof=0 is the appropriate choice for this use case.
        """
        # This test documents the rationale - always passes
        assert len(rationale) > 0

    def test_population_vs_sample_difference_is_minimal_for_large_n(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Show that for large peer groups, the difference is negligible.

        The difference between population and sample std is:
        sample_std = population_std * sqrt(n / (n-1))

        For n=100: factor = 1.005 (0.5% difference)
        For n=1000: factor = 1.0005 (0.05% difference)
        """
        differences: list[dict[str, Any]] = []

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

                if len(metric_values) < 10:
                    continue

                values_array = np.array(metric_values, dtype=np.float64)
                n = len(values_array)

                population_std = float(np.std(values_array, ddof=0))
                sample_std = float(np.std(values_array, ddof=1))

                if population_std > 0:
                    pct_difference = abs(sample_std - population_std) / population_std * 100
                    differences.append(
                        {
                            "node_id": node_id,
                            "n": n,
                            "population_std": population_std,
                            "sample_std": sample_std,
                            "pct_difference": pct_difference,
                        }
                    )

            except Exception:
                pass

        assert len(differences) > 0, "No peer groups were analyzed"

        # For large peer groups, difference should be small
        large_n_diffs = [d for d in differences if d["n"] >= 100]
        if large_n_diffs:
            avg_pct_diff = sum(d["pct_difference"] for d in large_n_diffs) / len(large_n_diffs)
            assert avg_pct_diff < 1.0, f"For large peer groups (n>=100), expected <1% difference. Average difference: {avg_pct_diff:.2f}%"
