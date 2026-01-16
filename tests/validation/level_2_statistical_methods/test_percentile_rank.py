"""Validate percentile rank calculations against first-principles recalculation."""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader
from tests.validation.helpers.independent_calculators import IndependentPercentileCalculator


class TestPercentileRankRecalculation:
    """Validate percentile rank calculations across all entities."""

    RTOL = 1e-9  # Relative tolerance

    def test_percentile_rank_midpoint_method(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Recalculated percentile ranks match stored values for aggregate nodes.

        Uses aggregate nodes where all entities form a single peer group.
        The midpoint method formula: percentile = 100 * (below + 0.5 * equal) / n
        """
        calc = IndependentPercentileCalculator()
        discrepancies: list[dict[str, Any]] = []
        validated_count = 0

        node_files = validation_loader.iter_node_files()

        # Filter to aggregate nodes only
        aggregate_nodes = [p for p in node_files if "__aggregate_time_period" in p.stem and "_" not in p.stem.split("__")[1]]

        for node_path in aggregate_nodes[:3]:  # Sample first 3 aggregate nodes
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                # Collect all metric values for peer distribution
                metric_values: list[float] = []
                entities_with_percentile: list[tuple[dict[str, Any], float, float]] = []

                for entity in results:
                    metric_list = entity.get("metric", [])
                    if not metric_list:
                        continue
                    metric_value = metric_list[0].get("values")
                    if metric_value is None:
                        continue
                    try:
                        val = float(metric_value)
                        if np.isnan(val):
                            continue
                        metric_values.append(val)
                    except (TypeError, ValueError):
                        continue

                    # Check for stored percentile_rank
                    for stat_method in entity.get("statistical_methods", []):
                        stats = stat_method.get("statistics", {})
                        if "percentile_rank" in stats and stats["percentile_rank"] is not None:
                            entities_with_percentile.append((entity, val, stats["percentile_rank"]))
                            break

                if not metric_values or not entities_with_percentile:
                    continue

                values_array = np.array(metric_values, dtype=np.float64)

                # Validate each entity's percentile rank
                for entity, metric_value, stored_percentile in entities_with_percentile:
                    recalculated = calc.percentile_rank(metric_value, values_array)
                    validated_count += 1

                    if not np.isclose(stored_percentile, recalculated, rtol=self.RTOL):
                        discrepancies.append(
                            {
                                "node_id": node_id,
                                "entity": entity.get("entity"),
                                "metric_value": metric_value,
                                "stored_percentile": stored_percentile,
                                "recalculated_percentile": recalculated,
                                "difference": abs(stored_percentile - recalculated),
                            }
                        )

            except Exception as e:
                discrepancies.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        if discrepancies:
            sample = discrepancies[:5]
            pytest.fail(
                f"{len(discrepancies)} percentile rank discrepancies found "
                f"(validated {validated_count} entities). "
                f"Sample: {json.dumps(sample, indent=2, default=str)}"
            )

        assert validated_count > 0, "No percentile ranks validated"

    def test_percentile_rank_bounds(
        self,
        all_classifications: list[dict[str, Any]],
    ) -> None:
        """All percentile ranks are in valid [0, 100] range."""

        out_of_bounds: list[dict[str, Any]] = []

        for record in all_classifications:
            # Check if classification has percentile data
            if "percentile_rank" in record:
                pr = record["percentile_rank"]
                if pr is not None and (pr < 0 or pr > 100):
                    out_of_bounds.append(
                        {
                            "entity_key": record.get("entity_key"),
                            "percentile_rank": pr,
                        }
                    )

        if out_of_bounds:
            pytest.fail(f"{len(out_of_bounds)} percentile ranks out of [0, 100] range. Sample: {out_of_bounds[:5]}")
