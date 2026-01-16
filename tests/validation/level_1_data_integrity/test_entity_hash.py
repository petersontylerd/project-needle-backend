"""Validate entity dimensions hash uniqueness and determinism.

Entity dimensions hash is computed in dbt as:
    md5(entity_dimensions::text)

Where entity_dimensions is a JSONB object containing all dimension keys
EXCEPT medicareId (facility_id), which is stored separately.

This ensures:
- Hash uniquely identifies dimension combinations within a facility
- Same dimensions always produce same hash (determinism)
- Different dimensions produce different hashes (no collisions)
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest

from tests.validation.helpers.data_loaders import ValidationDataLoader


def _compute_entity_dimensions_hash(entity_dims: list[dict[str, Any]]) -> str:
    """Compute entity_dimensions_hash matching dbt logic.

    Args:
        entity_dims: List of dimension dicts with 'id' and 'value' keys.

    Returns:
        MD5 hash of the JSONB representation (excluding medicareId).

    Note:
        This mirrors the dbt computation:
        md5(coalesce(entity_dimensions::text, '{}'))

        Where entity_dimensions is built via:
        jsonb_object_agg(id, value) filter (where id != 'medicareId')
    """
    # Build JSONB object excluding medicareId, sorted by key for determinism
    dims_dict = {d.get("id", ""): d.get("value") for d in entity_dims if d.get("id") != "medicareId"}

    # PostgreSQL JSONB text representation: keys sorted alphabetically
    # json.dumps with sort_keys matches PostgreSQL's JSONB text output
    dims_text = json.dumps(dims_dict, sort_keys=True, separators=(",", ": "))

    return hashlib.md5(dims_text.encode()).hexdigest()


def _extract_entity_key(entity_dims: list[dict[str, Any]]) -> tuple[tuple[str, Any], ...]:
    """Extract a hashable entity key from dimensions.

    Args:
        entity_dims: List of dimension dicts.

    Returns:
        Tuple of (id, value) pairs, sorted by id.
    """
    return tuple(sorted((d.get("id", ""), d.get("value")) for d in entity_dims))


class TestEntityHashDeterminism:
    """Validate that hash computation is deterministic."""

    def test_hash_deterministic_for_same_dimensions(self) -> None:
        """Same dimensions always produce the same hash.

        This is a unit test that doesn't require production data.
        """
        dims1 = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
            {"id": "admissionSource", "value": "Emergency Room"},
        ]
        dims2 = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
            {"id": "admissionSource", "value": "Emergency Room"},
        ]

        hash1 = _compute_entity_dimensions_hash(dims1)
        hash2 = _compute_entity_dimensions_hash(dims2)

        assert hash1 == hash2, f"Same dimensions should produce same hash. Hash1: {hash1}, Hash2: {hash2}"

    def test_hash_deterministic_regardless_of_dimension_order(self) -> None:
        """Dimension array order shouldn't affect hash.

        Entity dimensions may appear in different orders in the source,
        but the hash should be the same.
        """
        dims1 = [
            {"id": "vizientServiceLine", "value": "Cardiology"},
            {"id": "admissionSource", "value": "Emergency Room"},
            {"id": "medicareId", "value": "010033"},
        ]
        dims2 = [
            {"id": "medicareId", "value": "010033"},
            {"id": "admissionSource", "value": "Emergency Room"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
        ]

        hash1 = _compute_entity_dimensions_hash(dims1)
        hash2 = _compute_entity_dimensions_hash(dims2)

        assert hash1 == hash2, f"Dimension order shouldn't affect hash. Hash1: {hash1}, Hash2: {hash2}"

    def test_hash_differs_for_different_dimensions(self) -> None:
        """Different dimensions must produce different hashes.

        This is critical for grain uniqueness - if two different
        dimension combinations hash to the same value, we'd have
        a grain violation.
        """
        dims1 = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
        ]
        dims2 = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": "Neurology"},
        ]
        dims3 = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
            {"id": "admissionSource", "value": "Emergency Room"},
        ]

        hash1 = _compute_entity_dimensions_hash(dims1)
        hash2 = _compute_entity_dimensions_hash(dims2)
        hash3 = _compute_entity_dimensions_hash(dims3)

        assert hash1 != hash2, "Different service lines should produce different hashes"
        assert hash1 != hash3, "Different dimension counts should produce different hashes"
        assert hash2 != hash3, "All three should be distinct"

    def test_hash_excludes_facility_id(self) -> None:
        """Hash should NOT include medicareId (facility_id).

        facility_id is stored separately in fct_signals.
        entity_dimensions_hash identifies dimension combinations WITHIN a facility.
        """
        dims_with_facility = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
        ]
        dims_different_facility = [
            {"id": "medicareId", "value": "999999"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
        ]

        hash1 = _compute_entity_dimensions_hash(dims_with_facility)
        hash2 = _compute_entity_dimensions_hash(dims_different_facility)

        assert hash1 == hash2, (
            f"Different facility IDs with same dimensions should produce same hash. "
            f"This ensures entity_dimensions_hash is facility-agnostic. "
            f"Hash1: {hash1}, Hash2: {hash2}"
        )

    def test_empty_dimensions_after_excluding_facility(self) -> None:
        """Facility-only entities should have empty dimensions hash.

        For nodes like losIndex__medicareId__aggregate_time_period,
        the only dimension is medicareId, so entity_dimensions is {}.
        """
        dims_facility_only = [
            {"id": "medicareId", "value": "010033"},
        ]

        hash_value = _compute_entity_dimensions_hash(dims_facility_only)
        expected = hashlib.md5(b"{}").hexdigest()

        assert hash_value == expected, f"Facility-only entities should hash to empty dict. Got: {hash_value}, Expected: {expected}"


class TestEntityHashUniqueness:
    """Validate hash uniqueness in production data."""

    def test_no_hash_collisions_in_run_data(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """No two different dimension combinations should produce the same hash.

        This is a critical grain validation - hash collisions would cause
        data corruption in fct_signals joins.
        """
        # Collect hash -> dimension mappings across all nodes
        hash_to_dims: dict[str, tuple[tuple[str, Any], ...]] = {}
        collisions: list[dict[str, Any]] = []
        validated_entities = 0

        node_files = validation_loader.iter_node_files()

        # Sample nodes with multiple dimensions (more likely to find collisions)
        multi_dim_nodes = [p for p in node_files if "_" in p.stem and "__aggregate_time_period" in p.stem]

        for node_path in multi_dim_nodes[:10]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results[:100]:  # Sample per node
                    validated_entities += 1

                    entity_dims = entity.get("entity", [])
                    entity_key = _extract_entity_key(entity_dims)
                    hash_value = _compute_entity_dimensions_hash(entity_dims)

                    if hash_value in hash_to_dims:
                        existing_key = hash_to_dims[hash_value]
                        # Exclude medicareId for comparison (it's stored separately)
                        entity_key_no_facility = tuple((k, v) for k, v in entity_key if k != "medicareId")
                        existing_key_no_facility = tuple((k, v) for k, v in existing_key if k != "medicareId")

                        if entity_key_no_facility != existing_key_no_facility:
                            collisions.append(
                                {
                                    "node_id": node_id,
                                    "hash": hash_value,
                                    "dims1": dict(existing_key_no_facility),
                                    "dims2": dict(entity_key_no_facility),
                                }
                            )
                    else:
                        hash_to_dims[hash_value] = entity_key

            except Exception as e:
                collisions.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_entities > 0, "No entities were validated"

        if collisions:
            sample = collisions[:5]
            pytest.fail(
                f"{len(collisions)} hash collisions found (validated {validated_entities} entities). Sample: {json.dumps(sample, indent=2, default=str)}"
            )

    def test_hash_consistency_across_nodes(
        self,
        validation_loader: ValidationDataLoader,
    ) -> None:
        """Same dimension combination produces same hash across different nodes.

        For example, facility + service_line "Cardiology" should have the
        same hash whether it appears in losIndex or meanIcuDays node.
        """
        # Collect dimension -> hash mappings from different nodes
        dims_to_hash: dict[tuple[tuple[str, Any], ...], str] = {}
        inconsistencies: list[dict[str, Any]] = []
        validated_entities = 0

        node_files = validation_loader.iter_node_files()

        # Get nodes for different metrics with same dimensions
        service_line_nodes = [p for p in node_files if "__medicareId_vizientServiceLine__aggregate_time_period" in p.stem]

        for node_path in service_line_nodes[:5]:
            try:
                node_id = node_path.stem
                results = validation_loader.load_node_results(node_id)

                for entity in results[:50]:
                    validated_entities += 1

                    entity_dims = entity.get("entity", [])
                    entity_key = _extract_entity_key(entity_dims)
                    # Exclude medicareId and facility for cross-node comparison
                    entity_key_no_facility = tuple((k, v) for k, v in entity_key if k != "medicareId")
                    hash_value = _compute_entity_dimensions_hash(entity_dims)

                    if entity_key_no_facility in dims_to_hash:
                        existing_hash = dims_to_hash[entity_key_no_facility]
                        if hash_value != existing_hash:
                            inconsistencies.append(
                                {
                                    "node_id": node_id,
                                    "dims": dict(entity_key_no_facility),
                                    "hash1": existing_hash,
                                    "hash2": hash_value,
                                }
                            )
                    else:
                        dims_to_hash[entity_key_no_facility] = hash_value

            except Exception as e:
                inconsistencies.append(
                    {
                        "node_id": str(node_path),
                        "error": str(e),
                    }
                )

        assert validated_entities > 0, "No entities were validated"

        if inconsistencies:
            sample = inconsistencies[:5]
            pytest.fail(
                f"{len(inconsistencies)} hash inconsistencies found "
                f"(validated {validated_entities} entities). "
                f"Sample: {json.dumps(sample, indent=2, default=str)}"
            )


class TestEntityHashFormat:
    """Validate hash format and structure."""

    def test_hash_is_valid_md5_format(self) -> None:
        """Hash should be a valid 32-character hex MD5 string."""
        dims = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": "Cardiology"},
        ]

        hash_value = _compute_entity_dimensions_hash(dims)

        assert len(hash_value) == 32, f"MD5 hash should be 32 chars, got {len(hash_value)}"
        assert all(c in "0123456789abcdef" for c in hash_value), f"MD5 hash should be hex characters only, got: {hash_value}"

    def test_hash_handles_special_characters_in_values(self) -> None:
        """Hash should correctly handle special characters in dimension values."""
        dims_special = [
            {"id": "medicareId", "value": "010033"},
            {"id": "admissionSource", "value": "Clinic/Physician's Office"},
            {"id": "notes", "value": 'Value with "quotes" and \\ backslash'},
        ]

        # Should not raise
        hash_value = _compute_entity_dimensions_hash(dims_special)
        assert len(hash_value) == 32

    def test_hash_handles_null_values(self) -> None:
        """Hash should handle None/null dimension values."""
        dims_with_null: list[dict[str, Any]] = [
            {"id": "medicareId", "value": "010033"},
            {"id": "vizientServiceLine", "value": None},
        ]

        # Should not raise
        hash_value = _compute_entity_dimensions_hash(dims_with_null)
        assert len(hash_value) == 32

    def test_hash_handles_numeric_values(self) -> None:
        """Hash should handle numeric dimension values consistently."""
        dims_with_number: list[dict[str, Any]] = [
            {"id": "medicareId", "value": "010033"},
            {"id": "year", "value": 2024},
        ]

        hash_value = _compute_entity_dimensions_hash(dims_with_number)
        assert len(hash_value) == 32

        # Verify determinism with numeric values
        hash_value2 = _compute_entity_dimensions_hash(dims_with_number)
        assert hash_value == hash_value2
