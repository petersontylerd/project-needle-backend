#!/usr/bin/env python3
"""Validate that dbt metric_catalog.csv seed matches taxonomy/metrics.yaml.

This script enforces the DRY (Don't Repeat Yourself) rule for metric semantics
by verifying that the generated seed file is in sync with the taxonomy.

Usage:
    python scripts/semantic/validate_semantic_dry.py \
        --taxonomy taxonomy/metrics.yaml \
        --seed backend/dbt/seeds/metric_catalog.csv

Exit codes:
    0: Validation passed (seed matches taxonomy)
    1: Validation failed (drift detected or file errors)
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

import yaml


def load_taxonomy(taxonomy_path: Path) -> dict[str, dict[str, Any]]:
    """Load the taxonomy YAML and return a dict keyed by metric_id.

    Args:
        taxonomy_path: Path to the taxonomy/metrics.yaml file.

    Returns:
        Dictionary mapping metric_id to metric data.

    Raises:
        FileNotFoundError: If the taxonomy file doesn't exist.
        ValueError: If the taxonomy is malformed.
    """
    if not taxonomy_path.exists():
        raise FileNotFoundError(f"Taxonomy file not found: {taxonomy_path}")

    with taxonomy_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if not isinstance(data, dict) or "entries" not in data:
        raise ValueError("Taxonomy must be a mapping with an 'entries' key")

    entries = data["entries"]
    if not isinstance(entries, list):
        raise ValueError("Taxonomy 'entries' must be a list")

    result: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        metric_id = entry.get("metric_id")
        if metric_id:
            result[metric_id] = entry

    return result


def load_seed(seed_path: Path) -> dict[str, dict[str, str]]:
    """Load the seed CSV and return a dict keyed by metric_id.

    Args:
        seed_path: Path to the seed CSV file.

    Returns:
        Dictionary mapping metric_id to seed row data.

    Raises:
        FileNotFoundError: If the seed file doesn't exist.
    """
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_path}")

    result: dict[str, dict[str, str]] = {}

    with seed_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            metric_id = row.get("metric_id")
            if metric_id:
                result[metric_id] = row

    return result


def validate_alignment(
    taxonomy: dict[str, dict[str, Any]],
    seed: dict[str, dict[str, str]],
) -> list[str]:
    """Validate that seed aligns with taxonomy.

    Args:
        taxonomy: Dictionary of taxonomy entries keyed by metric_id.
        seed: Dictionary of seed rows keyed by metric_id.

    Returns:
        List of validation error messages (empty if valid).
    """
    errors: list[str] = []

    # Check for metrics in taxonomy but missing from seed
    for metric_id in taxonomy:
        if metric_id not in seed:
            errors.append(f"Metric '{metric_id}' in taxonomy but missing from seed")

    # Check for metrics in seed but not in taxonomy
    for metric_id in seed:
        if metric_id not in taxonomy:
            errors.append(f"Metric '{metric_id}' in seed but missing from taxonomy")

    # Check for drift in shared metrics
    for metric_id in taxonomy:
        if metric_id not in seed:
            continue

        tax_entry = taxonomy[metric_id]
        seed_row = seed[metric_id]

        # Compare display_name / metric_name
        tax_name = tax_entry.get("display_name", "")
        seed_name = seed_row.get("metric_name", "")
        if tax_name != seed_name:
            errors.append(f"Metric '{metric_id}' name mismatch: taxonomy='{tax_name}' vs seed='{seed_name}'")

        # Compare description (normalize whitespace)
        tax_desc = " ".join(str(tax_entry.get("description", "")).split())
        seed_desc = " ".join(str(seed_row.get("description", "")).split())
        if tax_desc != seed_desc:
            errors.append(f"Metric '{metric_id}' description mismatch: taxonomy='{tax_desc[:50]}...' vs seed='{seed_desc[:50]}...'")

        # Compare domain
        tax_domain = tax_entry.get("domain", "")
        seed_domain = seed_row.get("domain", "")
        if tax_domain != seed_domain:
            errors.append(f"Metric '{metric_id}' domain mismatch: taxonomy='{tax_domain}' vs seed='{seed_domain}'")

        # Compare polarity / direction_preference
        tax_polarity = tax_entry.get("polarity", "")
        seed_polarity = seed_row.get("direction_preference", "")
        if tax_polarity != seed_polarity:
            errors.append(f"Metric '{metric_id}' polarity mismatch: taxonomy='{tax_polarity}' vs seed='{seed_polarity}'")

        # Compare unit_type
        tax_unit = tax_entry.get("unit_type", "")
        seed_unit = seed_row.get("unit_type", "")
        if tax_unit != seed_unit:
            errors.append(f"Metric '{metric_id}' unit_type mismatch: taxonomy='{tax_unit}' vs seed='{seed_unit}'")

    return errors


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the validator.

    Returns:
        Configured argument parser.
    """
    parser = argparse.ArgumentParser(
        description="Validate that dbt metric_catalog.csv seed matches taxonomy/metrics.yaml.",
    )
    parser.add_argument(
        "--taxonomy",
        required=True,
        help="Path to the taxonomy/metrics.yaml file.",
    )
    parser.add_argument(
        "--seed",
        required=True,
        help="Path to the dbt seed CSV file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the validator.

    Args:
        argv: Command line arguments (defaults to sys.argv).

    Returns:
        Exit code (0 for validation passed, 1 for failure).
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    taxonomy_path = Path(args.taxonomy)
    seed_path = Path(args.seed)

    try:
        taxonomy = load_taxonomy(taxonomy_path)
        seed = load_seed(seed_path)
    except (ValueError, FileNotFoundError) as error:
        print(f"Error loading files: {error}", file=sys.stderr)
        return 1

    errors = validate_alignment(taxonomy, seed)

    if errors:
        print("Semantic DRY validation FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            f"\nTo fix: run 'python scripts/semantic/generate_metric_catalog_seed.py --taxonomy {taxonomy_path} --out {seed_path}'",
            file=sys.stderr,
        )
        return 1

    print(
        f"Semantic DRY validation PASSED: {len(taxonomy)} metrics aligned",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
