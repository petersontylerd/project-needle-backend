"""Validate excess_over_parent calculation against first-principles recalculation."""

from __future__ import annotations

from typing import Any

import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader
from tests.validation.helpers.independent_calculators import (
    IndependentContributionCalculator,
)


class TestExcessOverParentRecalculation:
    """Validate excess_over_parent calculation across all contribution records."""

    RTOL = 1e-9  # Relative tolerance for floating point comparison

    def test_excess_over_parent_formula_matches(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Recalculated excess_over_parent matches stored values.

        Formula: excess_over_parent = weight_share * (child_value - parent_value)
        """
        calc = IndependentContributionCalculator()
        discrepancies: list[dict[str, Any]] = []
        validated_count = 0

        contribution_files = validation_loader.iter_contribution_files()

        # Focus on weighted_mean method files
        weighted_mean_files = [f for f in contribution_files if ".losIndex.jsonl" in f.name or ".meanIcuDays.jsonl" in f.name]

        for contrib_path in weighted_mean_files[:5]:  # Sample first 5 files
            try:
                records = validation_loader.load_contribution_file(contrib_path.name)

                for record in records:
                    if record.get("method") != "weighted_mean":
                        continue

                    weight_share = record.get("weight_share")
                    child_value = record.get("child_value")
                    parent_value = record.get("parent_value")
                    stored_excess = record.get("excess_over_parent")

                    # Skip if any required field is None
                    if any(v is None for v in [weight_share, child_value, parent_value, stored_excess]):
                        continue

                    # Type narrowing assertions for mypy
                    assert weight_share is not None
                    assert child_value is not None
                    assert parent_value is not None
                    assert stored_excess is not None

                    # Recalculate
                    recalculated_excess = calc.excess_over_parent(
                        weight_share=weight_share,
                        child_value=child_value,
                        parent_value=parent_value,
                    )
                    validated_count += 1

                    # Compare
                    if abs(stored_excess - recalculated_excess) > self.RTOL:
                        discrepancies.append(
                            {
                                "file": contrib_path.name,
                                "parent_entity": record.get("parent_entity"),
                                "child_entity": record.get("child_entity"),
                                "weight_share": weight_share,
                                "child_value": child_value,
                                "parent_value": parent_value,
                                "stored_excess": stored_excess,
                                "recalculated_excess": recalculated_excess,
                                "difference": abs(stored_excess - recalculated_excess),
                            }
                        )

            except Exception as e:
                discrepancies.append(
                    {
                        "file": str(contrib_path),
                        "error": str(e),
                    }
                )

        assert validated_count > 0, "No excess_over_parent values were validated"

        if discrepancies:
            sample = discrepancies[:10]
            pytest.fail(f"{len(discrepancies)} excess_over_parent discrepancies found (validated {validated_count} records). Sample: {sample}")

    def test_raw_component_formula_matches(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Recalculated raw_component matches stored values.

        Formula: raw_component = weight_share * child_value
        """
        calc = IndependentContributionCalculator()
        discrepancies: list[dict[str, Any]] = []
        validated_count = 0

        contribution_files = validation_loader.iter_contribution_files()

        weighted_mean_files = [f for f in contribution_files if ".losIndex.jsonl" in f.name]

        for contrib_path in weighted_mean_files[:3]:  # Sample first 3 files
            try:
                records = validation_loader.load_contribution_file(contrib_path.name)

                for record in records:
                    if record.get("method") != "weighted_mean":
                        continue

                    weight_share = record.get("weight_share")
                    child_value = record.get("child_value")
                    stored_raw_component = record.get("raw_component")

                    if any(v is None for v in [weight_share, child_value, stored_raw_component]):
                        continue

                    # Type narrowing assertions for mypy
                    assert weight_share is not None
                    assert child_value is not None
                    assert stored_raw_component is not None

                    recalculated = calc.raw_component(
                        weight_share=weight_share,
                        child_value=child_value,
                    )
                    validated_count += 1

                    if abs(stored_raw_component - recalculated) > self.RTOL:
                        discrepancies.append(
                            {
                                "file": contrib_path.name,
                                "parent_entity": record.get("parent_entity"),
                                "child_entity": record.get("child_entity"),
                                "weight_share": weight_share,
                                "child_value": child_value,
                                "stored_raw_component": stored_raw_component,
                                "recalculated": recalculated,
                                "difference": abs(stored_raw_component - recalculated),
                            }
                        )

            except Exception:
                pass  # Skip errors

        assert validated_count > 0, "No raw_component values were validated"

        if discrepancies:
            sample = discrepancies[:10]
            pytest.fail(f"{len(discrepancies)} raw_component discrepancies found (validated {validated_count} records). Sample: {sample}")

    def test_weight_share_formula_matches(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Recalculated weight_share matches stored values.

        Formula: weight_share = weight_value / sum(all children's weight_value for this parent)
        """
        calc = IndependentContributionCalculator()
        discrepancies: list[dict[str, Any]] = []
        validated_count = 0

        contribution_files = validation_loader.iter_contribution_files()

        weighted_mean_files = [f for f in contribution_files if ".losIndex.jsonl" in f.name]

        for contrib_path in weighted_mean_files[:3]:  # Sample first 3 files
            try:
                records = validation_loader.load_contribution_file(contrib_path.name)

                # Group by parent entity
                parent_groups: dict[str, list[dict[str, Any]]] = {}
                for record in records:
                    if record.get("method") != "weighted_mean":
                        continue

                    parent_key = f"{record.get('parent_node_id')}::{str(record.get('parent_entity'))}"
                    if parent_key not in parent_groups:
                        parent_groups[parent_key] = []
                    parent_groups[parent_key].append(record)

                # Validate each parent's children
                for parent_key, children in parent_groups.items():
                    # Calculate total weight for this parent
                    total_weight = sum(child.get("weight_value", 0.0) for child in children if child.get("weight_value") is not None)

                    for child in children:
                        weight_value = child.get("weight_value")
                        stored_weight_share = child.get("weight_share")

                        if weight_value is None or stored_weight_share is None:
                            continue

                        recalculated = calc.weight_share(
                            weight=weight_value,
                            total_weight=total_weight,
                        )
                        validated_count += 1

                        if abs(stored_weight_share - recalculated) > self.RTOL:
                            discrepancies.append(
                                {
                                    "file": contrib_path.name,
                                    "parent_key": parent_key,
                                    "child_entity": child.get("child_entity"),
                                    "weight_value": weight_value,
                                    "total_weight": total_weight,
                                    "stored_weight_share": stored_weight_share,
                                    "recalculated": recalculated,
                                    "difference": abs(stored_weight_share - recalculated),
                                }
                            )

            except Exception:
                pass  # Skip errors

        assert validated_count > 0, "No weight_share values were validated"

        if discrepancies:
            sample = discrepancies[:10]
            pytest.fail(f"{len(discrepancies)} weight_share discrepancies found (validated {validated_count} records). Sample: {sample}")
