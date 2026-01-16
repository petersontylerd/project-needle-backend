#!/usr/bin/env python3
"""Generate dbt metric_catalog.csv seed from taxonomy/metrics.yaml.

This script reads the authoritative metric definitions from the taxonomy
and generates a dbt seed file for consumption by the semantic layer.

Usage:
    python scripts/semantic/generate_metric_catalog_seed.py \
        --taxonomy taxonomy/metrics.yaml \
        --out backend/dbt/seeds/metric_catalog.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

import yaml


def load_taxonomy(taxonomy_path: Path) -> list[dict[str, Any]]:
    """Load and validate the taxonomy YAML file.

    Args:
        taxonomy_path: Path to the taxonomy/metrics.yaml file.

    Returns:
        List of metric entry dictionaries.

    Raises:
        ValueError: If the taxonomy is malformed or contains duplicates.
        FileNotFoundError: If the taxonomy file doesn't exist.
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

    # Validate required fields and check for duplicates
    seen_ids: set[str] = set()
    validated_entries: list[dict[str, Any]] = []

    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"Entry {idx} must be a mapping")

        metric_id = entry.get("metric_id")
        if not metric_id:
            raise ValueError(f"Entry {idx} missing required 'metric_id'")
        if not entry.get("display_name"):
            raise ValueError(f"Entry {idx} (metric_id={metric_id}) missing required 'display_name'")
        if not entry.get("description"):
            raise ValueError(f"Entry {idx} (metric_id={metric_id}) missing required 'description'")

        if metric_id in seen_ids:
            raise ValueError(f"Duplicate metric_id: {metric_id}")
        seen_ids.add(metric_id)
        validated_entries.append(entry)

    return validated_entries


def generate_seed(entries: list[dict[str, Any]], output_path: Path) -> None:
    """Generate the dbt seed CSV file from metric entries.

    Args:
        entries: List of validated metric entry dictionaries.
        output_path: Path to write the CSV seed file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Sort entries by metric_id for deterministic output
    sorted_entries = sorted(entries, key=lambda e: e["metric_id"])

    fieldnames = [
        "metric_id",
        "metric_name",
        "description",
        "domain",
        "direction_preference",
        "unit_type",
        "display_format",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for entry in sorted_entries:
            row = {
                "metric_id": entry["metric_id"],
                "metric_name": entry["display_name"],
                "description": entry["description"].strip(),
                "domain": entry.get("domain", ""),
                "direction_preference": entry.get("polarity", ""),
                "unit_type": entry.get("unit_type", ""),
                "display_format": entry.get("display_format", ""),
            }
            writer.writerow(row)


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the seed generator.

    Returns:
        Configured argument parser.
    """
    parser = argparse.ArgumentParser(
        description="Generate dbt metric_catalog.csv seed from taxonomy/metrics.yaml.",
    )
    parser.add_argument(
        "--taxonomy",
        required=True,
        help="Path to the taxonomy/metrics.yaml file.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Path to write the output CSV seed file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the seed generator.

    Args:
        argv: Command line arguments (defaults to sys.argv).

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    taxonomy_path = Path(args.taxonomy)
    output_path = Path(args.out)

    try:
        entries = load_taxonomy(taxonomy_path)
        generate_seed(entries, output_path)
        print(f"Generated {output_path} with {len(entries)} metrics", file=sys.stdout)
        return 0
    except (ValueError, FileNotFoundError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
