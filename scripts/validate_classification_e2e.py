#!/usr/bin/env python3
"""End-to-end validation for signal classification pipeline.

Validates the entire classification pipeline from insight-graph-classify
output through dbt transformation to backend API, ensuring all simplified
signal type fields are properly processed and available.

Usage:
    # Validate with specific run directory
    UV_CACHE_DIR=.uv-cache uv run python scripts/validate_classification_e2e.py \
        --run-dir runs/lean/20251210154909

    # Validate with database URL
    UV_CACHE_DIR=.uv-cache uv run python scripts/validate_classification_e2e.py \
        --database-url postgresql://user:pass@host:port/db

    # Validate API endpoint
    UV_CACHE_DIR=.uv-cache uv run python scripts/validate_classification_e2e.py \
        --api-url http://localhost:8000

    # Skip certain validations
    UV_CACHE_DIR=.uv-cache uv run python scripts/validate_classification_e2e.py \
        --skip-api --skip-classification-output
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger(__name__)

# ANSI color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


@dataclass
class ValidationResult:
    """Result of a single validation step."""

    step: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


class ClassificationE2EValidator:
    """Validates entire signal classification pipeline end-to-end."""

    # Required fields in classification output (9 signal type system)
    # Note: Classification JSONL uses signal_type/severity, dbt renames to simplified_* in fct_signals
    REQUIRED_CLASSIFICATION_OUTPUT_FIELDS = {
        "signal_type",
        "severity",
    }

    # Valid simplified signal types (9 types)
    VALID_SIMPLIFIED_SIGNAL_TYPES = {
        "suspect_data",
        "sustained_excellence",
        "improving_leader",
        "baseline",
        "emerging_risk",
        "volatility_alert",
        "recovering",
        "chronic_underperformer",
        "critical_trajectory",
    }

    def __init__(
        self,
        run_dir: Path | None = None,
        database_url: str | None = None,
        api_url: str | None = None,
    ) -> None:
        """Initialize the validator.

        Args:
            run_dir: Path to insight graph run directory containing classification output.
            database_url: PostgreSQL connection URL.
            api_url: Backend API base URL.
        """
        self.run_dir = run_dir
        self.database_url = database_url or "postgresql+asyncpg://postgres:postgres@localhost:5433/quality_compass"
        self.api_url = api_url or "http://localhost:8000"
        self.results: list[ValidationResult] = []

    async def validate_classification_output(self) -> ValidationResult:
        """Check classifications.jsonl contains all required fields."""
        if not self.run_dir:
            return ValidationResult(
                step="Classification Output",
                passed=False,
                message="No run directory specified",
            )

        classification_file = self.run_dir / "analysis" / "classification" / "classifications.jsonl"

        if not classification_file.exists():
            return ValidationResult(
                step="Classification Output",
                passed=False,
                message=f"File not found: {classification_file}",
            )

        try:
            records_checked = 0
            missing_fields: set[str] = set()
            invalid_values: list[str] = []

            with classification_file.open() as f:
                for line_num, line in enumerate(f, 1):
                    if line_num > 100:  # Check first 100 records
                        break

                    record = json.loads(line)
                    records_checked += 1

                    # Check required fields (classification output uses signal_type/severity)
                    for field in self.REQUIRED_CLASSIFICATION_OUTPUT_FIELDS:
                        if field not in record:
                            missing_fields.add(field)

                    # Validate signal type enum values
                    signal_type = record.get("signal_type", "").lower() if record.get("signal_type") else None
                    if signal_type and signal_type not in self.VALID_SIMPLIFIED_SIGNAL_TYPES:
                        invalid_values.append(f"Invalid signal_type: {signal_type}")

                    # Validate severity range (0-100)
                    severity = record.get("severity")
                    if severity is not None and (severity < 0 or severity > 100):
                        invalid_values.append(f"Severity out of range: {severity}")

            if missing_fields:
                return ValidationResult(
                    step="Classification Output",
                    passed=False,
                    message=f"Missing fields: {missing_fields}",
                    details={"records_checked": records_checked, "missing": list(missing_fields)},
                )

            if invalid_values:
                return ValidationResult(
                    step="Classification Output",
                    passed=False,
                    message=f"Invalid values found: {invalid_values[:5]}",
                    details={"records_checked": records_checked, "invalid": invalid_values[:10]},
                )

            return ValidationResult(
                step="Classification Output",
                passed=True,
                message=f"{records_checked} records validated, all fields present",
                details={"records_checked": records_checked},
            )

        except json.JSONDecodeError as e:
            return ValidationResult(
                step="Classification Output",
                passed=False,
                message=f"JSON parse error: {e}",
            )
        except Exception as e:
            return ValidationResult(
                step="Classification Output",
                passed=False,
                message=f"Error: {e}",
            )

    async def validate_stg_classifications(self, session: AsyncSession) -> ValidationResult:
        """Check staging model parsed JSON correctly."""
        try:
            # Note: stg_classifications uses signal_type/severity (not simplified_* prefix)
            result = await session.execute(
                text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(signal_type) as has_signal_type,
                    COUNT(severity) as has_severity
                FROM public_staging.stg_classifications
            """)
            )
            row = result.fetchone()

            if not row or row.total == 0:
                return ValidationResult(
                    step="Staging Classifications",
                    passed=False,
                    message="No records in stg_classifications",
                )

            total = row.total
            completeness = {
                "signal_type": row.has_signal_type / total * 100,
                "severity": row.has_severity / total * 100,
            }

            # Check if all fields have >90% completeness
            low_completeness = [k for k, v in completeness.items() if v < 90]

            if low_completeness:
                return ValidationResult(
                    step="Staging Classifications",
                    passed=False,
                    message=f"Low completeness: {low_completeness}",
                    details={"total": total, "completeness": completeness},
                )

            return ValidationResult(
                step="Staging Classifications",
                passed=True,
                message=f"{total} rows, all fields >90% complete",
                details={"total": total, "completeness": completeness},
            )

        except Exception as e:
            return ValidationResult(
                step="Staging Classifications",
                passed=False,
                message=f"Query error: {e}",
            )

    async def validate_fct_signals(self, session: AsyncSession) -> ValidationResult:
        """Check mart model has joined classification data."""
        try:
            result = await session.execute(
                text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(simplified_signal_type) as has_signal_type,
                    COUNT(simplified_severity) as has_severity
                FROM public_marts.fct_signals
            """)
            )
            row = result.fetchone()

            if not row or row.total == 0:
                return ValidationResult(
                    step="Fact Signals",
                    passed=False,
                    message="No records in fct_signals",
                )

            total = row.total
            with_signal_type = row.has_signal_type
            percentage = with_signal_type / total * 100 if total > 0 else 0

            details = {
                "total_signals": total,
                "with_signal_type": with_signal_type,
                "percentage": round(percentage, 1),
            }

            if percentage < 10:
                return ValidationResult(
                    step="Fact Signals",
                    passed=False,
                    message=f"Only {percentage:.1f}% have signal type",
                    details=details,
                )

            return ValidationResult(
                step="Fact Signals",
                passed=True,
                message=f"{total} signals, {percentage:.1f}% have signal type",
                details=details,
            )

        except Exception as e:
            return ValidationResult(
                step="Fact Signals",
                passed=False,
                message=f"Query error: {e}",
            )

    async def validate_signals_table(self, session: AsyncSession) -> ValidationResult:
        """Check backend signals table has hydrated data."""
        try:
            result = await session.execute(
                text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(simplified_signal_type) as has_signal_type,
                    MIN(simplified_severity) as min_severity,
                    MAX(simplified_severity) as max_severity,
                    AVG(simplified_severity) as avg_severity
                FROM signals
                WHERE simplified_signal_type IS NOT NULL
            """)
            )
            row = result.fetchone()

            if not row or row.total == 0:
                return ValidationResult(
                    step="Signals Table",
                    passed=False,
                    message="No signals with signal type in signals table",
                )

            details = {
                "total_with_type": row.total,
                "severity_range": f"{row.min_severity}-{row.max_severity}",
                "avg_severity": round(float(row.avg_severity), 1) if row.avg_severity else None,
            }

            # Validate severity range
            if row.min_severity is not None and (row.min_severity < 0 or row.max_severity > 100):
                return ValidationResult(
                    step="Signals Table",
                    passed=False,
                    message=f"Severity out of range: {row.min_severity}-{row.max_severity}",
                    details=details,
                )

            return ValidationResult(
                step="Signals Table",
                passed=True,
                message=f"{row.total} signals with type, severity {row.min_severity}-{row.max_severity}",
                details=details,
            )

        except Exception as e:
            return ValidationResult(
                step="Signals Table",
                passed=False,
                message=f"Query error: {e}",
            )

    async def validate_api_response(self) -> ValidationResult:
        """Check API returns new fields correctly."""
        try:
            # Fetch signals from API
            req = urllib.request.Request(f"{self.api_url}/api/signals?limit=10")
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.status != 200:
                    return ValidationResult(
                        step="API Response",
                        passed=False,
                        message=f"API returned status {response.status}",
                    )

                data = json.loads(response.read().decode("utf-8"))

            if not data:
                return ValidationResult(
                    step="API Response",
                    passed=False,
                    message="API returned empty response",
                )

            # Check first signal has all classification fields
            # API returns {"signals": [...], "total_count": N, ...}
            signal = data[0] if isinstance(data, list) else data.get("signals", data.get("items", [{}]))[0]

            # Required simplified classification fields
            required_fields = [
                "simplified_signal_type",
                "simplified_severity",
            ]

            missing = [f for f in required_fields if f not in signal]

            if missing:
                return ValidationResult(
                    step="API Response",
                    passed=False,
                    message=f"Missing fields in response: {missing}",
                )

            # Test filtering works with simplified_signal_type
            filter_req = urllib.request.Request(f"{self.api_url}/api/signals?simplified_signal_type=emerging_risk&limit=1")
            with urllib.request.urlopen(filter_req, timeout=30) as filter_response:
                if filter_response.status != 200:
                    return ValidationResult(
                        step="API Response",
                        passed=False,
                        message="Filter by simplified_signal_type failed",
                    )

            return ValidationResult(
                step="API Response",
                passed=True,
                message="All classification fields present, filtering works",
                details={"sample_signal_type": signal.get("simplified_signal_type")},
            )

        except urllib.error.URLError as e:
            return ValidationResult(
                step="API Response",
                passed=False,
                message=f"Cannot connect to API at {self.api_url}: {e}",
            )
        except Exception as e:
            return ValidationResult(
                step="API Response",
                passed=False,
                message=f"Error: {e}",
            )

    async def validate_signal_type_distribution(self, session: AsyncSession) -> ValidationResult:
        """Check signal type distribution is reasonable."""
        try:
            result = await session.execute(
                text("""
                SELECT
                    simplified_signal_type,
                    COUNT(*) as count
                FROM signals
                WHERE simplified_signal_type IS NOT NULL
                GROUP BY simplified_signal_type
                ORDER BY count DESC
            """)
            )
            rows = result.fetchall()

            if not rows:
                return ValidationResult(
                    step="Signal Type Distribution",
                    passed=False,
                    message="No signal type data found",
                )

            distribution = {row.simplified_signal_type: row.count for row in rows}
            total = sum(distribution.values())

            # Check we have multiple categories (not all one type)
            if len(distribution) < 2:
                return ValidationResult(
                    step="Signal Type Distribution",
                    passed=False,
                    message=f"Only {len(distribution)} signal type(s)",
                    details=distribution,
                )

            # Check no single category dominates >95%
            max_pct = max(distribution.values()) / total * 100
            if max_pct > 95:
                return ValidationResult(
                    step="Signal Type Distribution",
                    passed=False,
                    message=f"Single category dominates: {max_pct:.1f}%",
                    details=distribution,
                )

            return ValidationResult(
                step="Signal Type Distribution",
                passed=True,
                message=f"{len(distribution)} types, max {max_pct:.1f}%",
                details={"distribution": distribution, "total": total},
            )

        except Exception as e:
            return ValidationResult(
                step="Signal Type Distribution",
                passed=False,
                message=f"Query error: {e}",
            )

    async def run_all(
        self,
        skip_classification_output: bool = False,
        skip_api: bool = False,
    ) -> list[ValidationResult]:
        """Run all validations and return results."""
        self.results = []

        # Classification output validation
        if not skip_classification_output and self.run_dir:
            result = await self.validate_classification_output()
            self.results.append(result)

        # Database validations
        engine = create_async_engine(self.database_url)
        session_maker = async_sessionmaker(engine)

        async with session_maker() as session:
            # Staging table
            result = await self.validate_stg_classifications(session)
            self.results.append(result)

            # Fact table
            result = await self.validate_fct_signals(session)
            self.results.append(result)

            # Signals table
            result = await self.validate_signals_table(session)
            self.results.append(result)

            # Distribution
            result = await self.validate_signal_type_distribution(session)
            self.results.append(result)

        await engine.dispose()

        # API validation
        if not skip_api:
            result = await self.validate_api_response()
            self.results.append(result)

        return self.results

    def print_report(self) -> int:
        """Print formatted validation report and return exit code."""
        print(f"\n{BOLD}{'=' * 50}")
        print("Signal Classification E2E Validation")
        print(f"{'=' * 50}{RESET}\n")

        passed_count = 0
        failed_count = 0

        for i, result in enumerate(self.results, 1):
            status = f"{GREEN}PASS{RESET}" if result.passed else f"{RED}FAIL{RESET}"
            print(f"[{i}/{len(self.results)}] {result.step}... {status}")
            print(f"    {result.message}")

            if result.details and not result.passed:
                for key, value in result.details.items():
                    print(f"    - {key}: {value}")

            if result.passed:
                passed_count += 1
            else:
                failed_count += 1

        # Distribution summary
        for result in self.results:
            if result.step == "Signal Type Distribution" and result.passed:
                dist = result.details.get("distribution", {})
                total = result.details.get("total", 0)
                if dist and total > 0:
                    print(f"\n{CYAN}=== Signal Type Distribution ==={RESET}")
                    for signal_type, count in sorted(dist.items(), key=lambda x: -x[1]):
                        pct = count / total * 100
                        print(f"  {signal_type}: {pct:.1f}% ({count} signals)")

        # Summary
        print(f"\n{BOLD}=== Summary ==={RESET}")
        if failed_count == 0:
            print(f"{GREEN}All {passed_count} validation steps passed.{RESET}")
            return 0
        else:
            print(f"{RED}{failed_count} of {passed_count + failed_count} steps failed.{RESET}")
            return 1


async def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Validate signal classification pipeline end-to-end")
    parser.add_argument(
        "--run-dir",
        type=Path,
        help="Path to insight graph run directory",
    )
    parser.add_argument(
        "--database-url",
        help="PostgreSQL connection URL (default: localhost:5433)",
    )
    parser.add_argument(
        "--api-url",
        help="Backend API URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--skip-classification-output",
        action="store_true",
        help="Skip classification output file validation",
    )
    parser.add_argument(
        "--skip-api",
        action="store_true",
        help="Skip API endpoint validation",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    validator = ClassificationE2EValidator(
        run_dir=args.run_dir,
        database_url=args.database_url,
        api_url=args.api_url,
    )

    await validator.run_all(
        skip_classification_output=args.skip_classification_output,
        skip_api=args.skip_api,
    )

    return validator.print_report()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
