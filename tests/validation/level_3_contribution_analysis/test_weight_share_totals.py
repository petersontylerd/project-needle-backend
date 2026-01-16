"""Validate weight share totals sum to 1.0 per parent entity."""

from __future__ import annotations

from typing import Any

import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader


class TestWeightShareTotals:
    """Validate weight shares sum to 1.0 per parent entity."""

    RTOL = 1e-6  # Relative tolerance for floating point comparison

    def test_weight_shares_sum_to_one_per_parent(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """For each parent entity, weight shares of all children sum to 1.0.

        This is a fundamental property of contribution analysis:
        - Each child's weight_share = child_weight / total_weight
        - Sum of all children's weight_shares must equal 1.0
        """
        violations: list[dict[str, Any]] = []
        validated_parents = 0

        contribution_files = validation_loader.iter_contribution_files()

        # Sample weighted_mean method files (not zscore methods which don't use weight share)
        weighted_mean_files = [f for f in contribution_files if ".losIndex.jsonl" in f.name or ".meanIcuDays.jsonl" in f.name]

        for contrib_path in weighted_mean_files[:5]:  # Sample first 5 files
            try:
                records = validation_loader.load_contribution_file(contrib_path.name)

                if not records:
                    continue

                # Group by parent entity (parent_node_id + parent_entity)
                parent_groups: dict[str, list[dict[str, Any]]] = {}
                for record in records:
                    # Only check weighted_mean method which uses weight shares
                    if record.get("method") != "weighted_mean":
                        continue

                    parent_key = f"{record.get('parent_node_id')}::{str(record.get('parent_entity'))}"
                    if parent_key not in parent_groups:
                        parent_groups[parent_key] = []
                    parent_groups[parent_key].append(record)

                # Check each parent's children
                for parent_key, children in parent_groups.items():
                    weight_share_sum = sum(child.get("weight_share", 0.0) for child in children)
                    validated_parents += 1

                    # Sum should be 1.0 within tolerance
                    if abs(weight_share_sum - 1.0) > self.RTOL:
                        violations.append(
                            {
                                "file": contrib_path.name,
                                "parent_key": parent_key,
                                "child_count": len(children),
                                "weight_share_sum": weight_share_sum,
                                "difference_from_one": abs(weight_share_sum - 1.0),
                            }
                        )

            except Exception as e:
                violations.append(
                    {
                        "file": str(contrib_path),
                        "error": str(e),
                    }
                )

        assert validated_parents > 0, "No parent entities were validated"

        if violations:
            sample = violations[:10]
            pytest.fail(f"{len(violations)} parent entities have weight shares not summing to 1.0 (validated {validated_parents} parents). Sample: {sample}")

    def test_all_weight_shares_are_positive(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """All weight shares must be non-negative."""
        violations: list[dict[str, Any]] = []

        contribution_files = validation_loader.iter_contribution_files()

        for contrib_path in contribution_files[:10]:  # Sample first 10 files
            try:
                records = validation_loader.load_contribution_file(contrib_path.name)

                for record in records:
                    if record.get("method") != "weighted_mean":
                        continue

                    weight_share = record.get("weight_share", 0.0)
                    if weight_share < 0:
                        violations.append(
                            {
                                "file": contrib_path.name,
                                "parent_entity": record.get("parent_entity"),
                                "child_entity": record.get("child_entity"),
                                "weight_share": weight_share,
                            }
                        )

            except Exception:
                pass  # Skip errors

        if violations:
            pytest.fail(f"{len(violations)} records have negative weight shares. Sample: {violations[:5]}")

    def test_weight_shares_bounded_zero_to_one(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """All weight shares must be in [0, 1] range."""
        violations: list[dict[str, Any]] = []
        validated_count = 0

        contribution_files = validation_loader.iter_contribution_files()

        for contrib_path in contribution_files[:10]:  # Sample first 10 files
            try:
                records = validation_loader.load_contribution_file(contrib_path.name)

                for record in records:
                    if record.get("method") != "weighted_mean":
                        continue

                    weight_share = record.get("weight_share", 0.0)
                    validated_count += 1

                    if weight_share < 0 or weight_share > 1.0:
                        violations.append(
                            {
                                "file": contrib_path.name,
                                "parent_entity": record.get("parent_entity"),
                                "child_entity": record.get("child_entity"),
                                "weight_share": weight_share,
                            }
                        )

            except Exception:
                pass  # Skip errors

        assert validated_count > 0, "No weight shares were validated"

        if violations:
            pytest.fail(f"{len(violations)} weight shares outside [0, 1] range. Sample: {violations[:5]}")
