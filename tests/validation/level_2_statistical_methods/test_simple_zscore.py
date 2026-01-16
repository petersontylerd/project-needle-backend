"""Validate simple z-score calculations against first-principles recalculation."""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader
from tests.validation.helpers.independent_calculators import (
    IndependentSimpleZScoreCalculator,
)


class TestSimpleZScoreRecalculation:
    """Validate simple z-score calculations across all entities."""

    RTOL = 1e-9  # Relative tolerance for floating point comparison

    def test_peer_mean_matches_recalculated_aggregate_nodes(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Recalculated peer means match stored values for aggregate nodes.

        Aggregate nodes (pattern: *__aggregate_time_period) have all entities
        in a single peer group, so peer_mean = mean of all metric values.
        """
        calc = IndependentSimpleZScoreCalculator()
        discrepancies: list[dict[str, Any]] = []
        validated_nodes = 0

        node_files = validation_loader.iter_node_files()

        # Filter to aggregate nodes only (uniform peer groups)
        aggregate_nodes = [p for p in node_files if "__aggregate_time_period" in p.stem and "_" not in p.stem.split("__")[1]]

        for node_path in aggregate_nodes[:5]:  # Sample first 5 aggregate nodes
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                # Collect all metric values for this node's peer distribution
                metric_values: list[float] = []
                stored_peer_mean: float | None = None

                for entity in results:
                    metric_list = entity.get("metric", [])
                    if not metric_list:
                        continue
                    metric_value = metric_list[0].get("values")
                    if metric_value is not None:
                        try:
                            val = float(metric_value)
                            if not np.isnan(val):
                                metric_values.append(val)
                        except (TypeError, ValueError):
                            continue

                    # Get stored peer_mean (all should be the same in aggregate nodes)
                    if stored_peer_mean is None:
                        for stat_method in entity.get("statistical_methods", []):
                            stats = stat_method.get("statistics", {})
                            if "peer_mean" in stats and stats["peer_mean"] is not None:
                                stored_peer_mean = stats["peer_mean"]
                                break

                if not metric_values or stored_peer_mean is None:
                    continue

                # Recalculate peer mean
                values_array = np.array(metric_values, dtype=np.float64)
                recalculated_mean = calc.peer_mean(values_array)
                validated_nodes += 1

                if not np.isclose(stored_peer_mean, recalculated_mean, rtol=self.RTOL):
                    discrepancies.append(
                        {
                            "node_id": node_id,
                            "stored_peer_mean": stored_peer_mean,
                            "recalculated_peer_mean": recalculated_mean,
                            "difference": abs(stored_peer_mean - recalculated_mean),
                            "entity_count": len(metric_values),
                        }
                    )

            except Exception as e:
                discrepancies.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_nodes > 0, "No aggregate nodes were validated"

        if discrepancies:
            sample = discrepancies[:5]
            pytest.fail(
                f"{len(discrepancies)} peer mean discrepancies found (validated {validated_nodes} nodes). Sample: {json.dumps(sample, indent=2, default=str)}"
            )

    def test_population_std_ddof_zero(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Verify population std (ddof=0) is used, not sample std (ddof=1).

        Uses aggregate nodes where all entities share the same peer group.
        """
        calc = IndependentSimpleZScoreCalculator()

        node_files = validation_loader.iter_node_files()
        if not node_files:
            pytest.skip("No node files available")

        # Find an aggregate node (uniform peer group)
        aggregate_nodes = [p for p in node_files if "__aggregate_time_period" in p.stem and "_" not in p.stem.split("__")[1]]

        if not aggregate_nodes:
            pytest.skip("No aggregate nodes available")

        node_path = aggregate_nodes[0]
        node_id = node_path.stem
        results = validation_loader.load_node_results(node_id)

        # Collect metric values
        metric_values: list[float] = []
        stored_peer_std: float | None = None

        for entity in results:
            metric_list = entity.get("metric", [])
            if not metric_list:
                continue
            metric_value = metric_list[0].get("values")
            if metric_value is not None:
                try:
                    val = float(metric_value)
                    if not np.isnan(val):
                        metric_values.append(val)
                except (TypeError, ValueError):
                    continue

            # Get stored peer_std
            if stored_peer_std is None:
                for stat_method in entity.get("statistical_methods", []):
                    stats = stat_method.get("statistics", {})
                    if "peer_std" in stats and stats["peer_std"] is not None:
                        stored_peer_std = stats["peer_std"]
                        break

        if not metric_values or stored_peer_std is None:
            pytest.skip("No peer_std found in node results")

        values_array = np.array(metric_values, dtype=np.float64)

        # Calculate both population and sample std
        population_std = calc.peer_std(values_array)  # ddof=0
        sample_std = float(np.std(values_array, ddof=1))  # ddof=1

        # Verify stored value matches population std, not sample std
        matches_population = np.isclose(stored_peer_std, population_std, rtol=self.RTOL)
        matches_sample = np.isclose(stored_peer_std, sample_std, rtol=self.RTOL)

        assert matches_population, f"Stored peer_std ({stored_peer_std}) should match population std ({population_std}), not sample std ({sample_std})"
        assert not matches_sample or population_std == sample_std, "Stored std matches sample std but should match population std"
