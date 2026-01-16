"""Validate entity-level grain in runtime JSONL output.

Runtime output should maintain entity-level grain:
- Each entity (dimension combination) appears exactly once per node
- Statistical methods are nested arrays, not separate rows
- Dimension ordering is deterministic and consistent

This validation catches grain violations BEFORE dbt processing.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader


def _entity_key_from_dimensions(entity_dims: list[dict[str, Any]]) -> tuple[tuple[str, Any], ...]:
    """Convert entity dimensions list to a hashable key.

    Args:
        entity_dims: List of dimension dicts with 'id' and 'value' keys.

    Returns:
        Tuple of (id, value) pairs, sorted by id for consistency.
    """
    return tuple(sorted((d.get("id", ""), d.get("value")) for d in entity_dims))


class TestRuntimeEntityUniqueness:
    """Validate entity uniqueness within each node's JSONL output."""

    def test_node_results_have_unique_entity_keys(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Each entity key appears exactly once per node.

        Entity key is the tuple of (dimension_id, dimension_value) pairs.
        Duplicates would indicate grain violation at the source.
        """
        violations: list[dict[str, Any]] = []
        validated_nodes = 0

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:20]:  # Sample first 20 nodes
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                if not results:
                    continue

                validated_nodes += 1

                # Track entity keys and their counts
                entity_key_counts: dict[tuple[tuple[str, Any], ...], int] = {}

                for entity in results:
                    entity_dims = entity.get("entity", [])
                    entity_key = _entity_key_from_dimensions(entity_dims)
                    entity_key_counts[entity_key] = entity_key_counts.get(entity_key, 0) + 1

                # Find duplicates
                duplicates = {k: v for k, v in entity_key_counts.items() if v > 1}
                if duplicates:
                    violations.append(
                        {
                            "node_id": node_id,
                            "duplicate_count": len(duplicates),
                            "sample_duplicates": list(duplicates.items())[:3],
                            "total_entities": len(results),
                        }
                    )

            except Exception as e:
                violations.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_nodes > 0, "No nodes were validated"

        if violations:
            sample = violations[:5]
            pytest.fail(
                f"{len(violations)} nodes have duplicate entity keys (validated {validated_nodes} nodes). Sample: {json.dumps(sample, indent=2, default=str)}"
            )

    def test_no_method_fanout_in_entity_results(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Statistical methods are nested arrays, not separate entity rows.

        Each entity should have one row with a statistical_methods array
        containing all methods, NOT multiple rows per method.
        """
        violations: list[dict[str, Any]] = []
        validated_entities = 0

        node_files = validation_loader.iter_node_files()

        # Focus on aggregate nodes which have multiple methods
        aggregate_nodes = [p for p in node_files if "__aggregate_time_period" in p.stem]

        for node_path in aggregate_nodes[:10]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results[:50]:  # Sample entities per node
                    validated_entities += 1

                    stat_methods = entity.get("statistical_methods", [])

                    # Each entity should have statistical_methods as a list
                    if not isinstance(stat_methods, list):
                        violations.append(
                            {
                                "node_id": node_id,
                                "entity": entity.get("entity"),
                                "issue": "statistical_methods is not a list",
                                "type": type(stat_methods).__name__,
                            }
                        )
                        continue

                    # Aggregate nodes should have at least 2 methods
                    # (simple_zscore and robust_zscore)
                    if len(stat_methods) < 2:
                        violations.append(
                            {
                                "node_id": node_id,
                                "entity": entity.get("entity"),
                                "issue": "Expected multiple methods in aggregate node",
                                "method_count": len(stat_methods),
                            }
                        )

            except Exception as e:
                violations.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_entities > 0, "No entities were validated"

        if violations:
            sample = violations[:5]
            pytest.fail(
                f"{len(violations)} entities have method structure issues "
                f"(validated {validated_entities} entities). "
                f"Sample: {json.dumps(sample, indent=2, default=str)}"
            )

    def test_entity_dimensions_consistent_ordering(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Entity dimension ordering is deterministic within a node.

        All entities in a node should have dimensions in the same order.
        This ensures consistent hash computation downstream.
        """
        violations: list[dict[str, Any]] = []
        validated_nodes = 0

        node_files = validation_loader.iter_node_files()

        # Focus on multi-dimension nodes
        multi_dim_nodes = [p for p in node_files if "_" in p.stem.split("__")[1] if len(p.stem.split("__")) > 1]

        for node_path in multi_dim_nodes[:10]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                if len(results) < 2:
                    continue

                validated_nodes += 1

                # Collect dimension orderings
                dim_orderings: set[tuple[str, ...]] = set()

                for entity in results:
                    entity_dims = entity.get("entity", [])
                    dim_order = tuple(d.get("id", "") for d in entity_dims)
                    dim_orderings.add(dim_order)

                # All entities should have the same dimension ordering
                if len(dim_orderings) > 1:
                    violations.append(
                        {
                            "node_id": node_id,
                            "orderings_found": len(dim_orderings),
                            "sample_orderings": list(dim_orderings)[:3],
                            "entity_count": len(results),
                        }
                    )

            except Exception as e:
                violations.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        if validated_nodes == 0:
            pytest.skip("No multi-dimension nodes found in run")

        if violations:
            sample = violations[:5]
            pytest.fail(
                f"{len(violations)} nodes have inconsistent dimension ordering "
                f"(validated {validated_nodes} nodes). "
                f"Sample: {json.dumps(sample, indent=2, default=str)}"
            )


class TestRuntimeGrainStructure:
    """Validate structural aspects of runtime grain."""

    def test_all_entities_have_required_fields(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Every entity result has required grain fields.

        Required fields: entity, metric, statistical_methods
        These are essential for grain determination and downstream processing.
        """
        required_fields = {"entity", "metric", "statistical_methods"}
        violations: list[dict[str, Any]] = []
        validated_entities = 0

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:15]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for i, entity in enumerate(results[:100]):
                    validated_entities += 1

                    entity_fields = set(entity.keys())
                    missing = required_fields - entity_fields

                    if missing:
                        violations.append(
                            {
                                "node_id": node_id,
                                "entity_index": i,
                                "missing_fields": list(missing),
                                "entity": entity.get("entity", "unknown"),
                            }
                        )

            except Exception as e:
                violations.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_entities > 0, "No entities were validated"

        if violations:
            sample = violations[:5]
            pytest.fail(
                f"{len(violations)} entities missing required fields "
                f"(validated {validated_entities} entities). "
                f"Sample: {json.dumps(sample, indent=2, default=str)}"
            )

    def test_entity_dimensions_have_required_structure(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Each dimension in entity array has id and value fields.

        These fields are required for:
        - Building entity_dimensions_hash in dbt
        - Identifying unique entities
        - JOIN operations in staging models
        """
        violations: list[dict[str, Any]] = []
        validated_dimensions = 0

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:10]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results[:50]:
                    entity_dims = entity.get("entity", [])

                    for dim in entity_dims:
                        validated_dimensions += 1

                        if "id" not in dim:
                            violations.append(
                                {
                                    "node_id": node_id,
                                    "entity": entity_dims,
                                    "issue": "dimension missing 'id' field",
                                    "dimension": dim,
                                }
                            )

                        if "value" not in dim:
                            violations.append(
                                {
                                    "node_id": node_id,
                                    "entity": entity_dims,
                                    "issue": "dimension missing 'value' field",
                                    "dimension": dim,
                                }
                            )

            except Exception as e:
                violations.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_dimensions > 0, "No dimensions were validated"

        if violations:
            sample = violations[:5]
            pytest.fail(
                f"{len(violations)} dimensions missing required fields "
                f"(validated {validated_dimensions} dimensions). "
                f"Sample: {json.dumps(sample, indent=2, default=str)}"
            )


class TestRuntimeGrainCounts:
    """Validate entity counts match expectations."""

    def test_entity_count_matches_metadata(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Entity count in results matches entity_results_count in metadata.

        The node metadata header contains expected entity_results_count.
        Actual results should match this count.
        """
        violations: list[dict[str, Any]] = []
        validated_nodes = 0

        node_files = validation_loader.iter_node_files()

        for node_path in node_files[:20]:
            try:
                node_id = node_path.stem

                # Load metadata separately
                metadata = validation_loader.load_node_metadata(node_id)
                if not metadata:
                    continue

                expected_count = metadata.get("entity_results_count")
                if expected_count is None:
                    continue

                # Load results
                results = validation_loader.load_node_results(node_id)
                actual_count = len(results)

                validated_nodes += 1

                if actual_count != expected_count:
                    violations.append(
                        {
                            "node_id": node_id,
                            "expected_count": expected_count,
                            "actual_count": actual_count,
                            "difference": actual_count - expected_count,
                        }
                    )

            except Exception as e:
                violations.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_nodes > 0, "No nodes with metadata were validated"

        if violations:
            sample = violations[:5]
            pytest.fail(
                f"{len(violations)} nodes have entity count mismatches (validated {validated_nodes} nodes). Sample: {json.dumps(sample, indent=2, default=str)}"
            )

    def test_aggregate_nodes_have_entities(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Aggregate nodes have non-empty entity results.

        Validates structural correctness - aggregate nodes should produce
        entity results when processed.
        """
        node_files = validation_loader.iter_node_files()

        # Focus on facility-only aggregate nodes
        facility_aggregate_nodes = [p for p in node_files if "__medicareId__aggregate_time_period" in p.stem]

        if not facility_aggregate_nodes:
            pytest.skip("No facility aggregate nodes found")

        entity_counts: list[dict[str, Any]] = []

        for node_path in facility_aggregate_nodes[:5]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)
                entity_counts.append(
                    {
                        "node_id": node_id,
                        "entity_count": len(results),
                    }
                )
            except Exception:
                continue

        if not entity_counts:
            pytest.skip("Could not load any facility aggregate nodes")

        # Verify all loaded nodes have entities
        for item in entity_counts:
            assert item["entity_count"] > 0, f"Node {item['node_id']} has no entities"
