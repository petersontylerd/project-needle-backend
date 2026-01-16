"""Signal hydrator service for populating database from dbt mart tables.

This service queries the dbt fct_signals mart table and upserts signals
into the application's signals table, enabling the Quality Compass frontend
to display data transformed by the dbt pipeline.

The hydration flow:
1. Raw files → load_insight_graph_to_dbt.py → raw_* tables
2. dbt run → raw_* → staging → marts (fct_signals)
3. SignalHydrator queries fct_signals → upserts to signals table

This separation allows:
- dbt to handle data transformation and quality checks
- The signals table to maintain workflow state (assignments, events)
- Clean separation between analytics data and application data
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.db.models import (
    Signal,
    SignalDomain,
)
from src.db.session import async_session_maker

logger = logging.getLogger(__name__)


# SQL query to fetch signals from dbt fct_signals table (public_marts schema)
# Grain: One row per entity/metric combination (no statistical method fan-out)
FCT_SIGNALS_QUERY = """
SELECT
    -- Core identifiers
    signal_id,
    run_id,
    canonical_node_id,
    temporal_node_id,
    system_name,
    facility_id,
    service_line,
    sub_service_line,
    metric_id,
    metric_value,
    benchmark_value as peer_mean,
    -- Statistical measures (primary values for display)
    percentile_rank,
    encounters,
    -- Domain (classification-driven)
    domain,
    description,
    -- Entity grouping (4 fields)
    entity_dimensions,
    entity_dimensions_hash,
    groupby_label,
    group_value,
    -- Temporal statistics (kept for trend display)
    metric_trend_timeline,
    trend_direction,
    -- 9 Signal Type classification
    simplified_signal_type,
    simplified_severity,
    simplified_severity_range,
    simplified_inputs,
    simplified_indicators,
    simplified_reasoning,
    simplified_severity_calculation,
    -- Metadata
    metadata,
    metadata_per_period,
    -- Peer percentile trends (for reference band visualization)
    peer_percentile_trends,
    -- Timestamps
    detected_at,
    dbt_updated_at
FROM public_marts.fct_signals
ORDER BY canonical_node_id, metric_id
"""

# SQL query to fetch technical details for a single signal (for drill-down API)
# Updated for entity-level grain: uses entity_dimensions_hash for precise lookup
# Note: We have two queries because asyncpg doesn't support `:param IS NULL` checks.
_TECHNICAL_DETAILS_COLUMNS = """
    canonical_node_id,
    entity_dimensions_hash,
    statistical_methods,
    simple_zscore,
    robust_zscore,
    latest_simple_zscore,
    mean_simple_zscore,
    latest_robust_zscore,
    mean_robust_zscore,
    percentile_rank,
    peer_std,
    peer_count,
    encounters,
    global_metric_mean,
    global_metric_std,
    slope,
    slope_percentile,
    acceleration,
    trend_direction,
    momentum,
    monthly_z_scores,
    simple_zscore_anomaly,
    robust_zscore_anomaly,
    latest_simple_zscore_anomaly,
    mean_simple_zscore_anomaly,
    latest_robust_zscore_anomaly,
    mean_robust_zscore_anomaly,
    slope_anomaly,
    magnitude_tier,
    trajectory_tier,
    consistency_tier,
    coefficient_of_variation,
    -- 9 Signal Type classification
    simplified_signal_type,
    simplified_severity,
    simplified_severity_range,
    simplified_inputs,
    simplified_indicators,
    simplified_reasoning,
    simplified_severity_calculation
"""

TECHNICAL_DETAILS_QUERY = f"""
SELECT {_TECHNICAL_DETAILS_COLUMNS}
FROM public_marts.fct_signals
WHERE canonical_node_id = :canonical_node_id
  AND entity_dimensions_hash = :entity_dimensions_hash
"""

TECHNICAL_DETAILS_QUERY_NO_HASH = f"""
SELECT {_TECHNICAL_DETAILS_COLUMNS}
FROM public_marts.fct_signals
WHERE canonical_node_id = :canonical_node_id
LIMIT 1
"""


class SignalHydrator:
    """Service for hydrating signals from dbt mart tables into the application database.

    Queries the fct_signals dbt mart table and upserts records into the signals
    table. The dbt pipeline must be run before hydration to ensure fct_signals is populated.

    Attributes:
        run_id: Optional run ID filter for the dbt tables.

    Example:
        >>> hydrator = SignalHydrator()
        >>> stats = await hydrator.hydrate_signals()
        >>> print(f"Processed {stats['signals_processed']} signals")

        >>> # With run ID filter
        >>> hydrator = SignalHydrator(run_id="20251210170210")
        >>> stats = await hydrator.hydrate_signals()
    """

    def __init__(
        self,
        run_id: str | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        limit: int | None = None,
        facility_ids: list[str] | None = None,
    ) -> None:
        """Initialize the hydrator.

        Args:
            run_id: Optional run ID to filter signals by.
                If None, processes all signals in fct_signals.
            session_factory: Optional async session factory for database connections.
                If None, uses the default async_session_maker from src.db.session.
                Useful for testing with alternative database connections.
            limit: Optional limit on number of signals to process.
                Useful for testing with large datasets.
            facility_ids: Optional list of facility IDs to filter signals by.
                If None, processes signals from all facilities.
        """
        self.run_id = run_id
        self._session_factory = session_factory or async_session_maker
        self._limit = limit
        self._facility_ids = facility_ids

    async def _query_fct_signals(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Query signals from the dbt fct_signals mart table.

        Args:
            session: Async database session.

        Returns:
            List of signal records as dictionaries.

        Raises:
            Exception: If the fct_signals table doesn't exist or query fails.
        """
        # Build WHERE conditions
        conditions: list[str] = []
        if self.run_id:
            conditions.append(f"run_id = '{self.run_id}'")
        if self._facility_ids:
            # Escape single quotes and build IN clause
            escaped_ids = [fid.replace("'", "''") for fid in self._facility_ids]
            ids_str = ", ".join(f"'{fid}'" for fid in escaped_ids)
            conditions.append(f"facility_id IN ({ids_str})")

        # Build query with optional WHERE clause
        query = FCT_SIGNALS_QUERY
        if conditions:
            where_clause = " AND ".join(conditions)
            query = query.replace(
                "ORDER BY canonical_node_id",
                f"WHERE {where_clause}\nORDER BY canonical_node_id",
            )

        # Add LIMIT clause if specified (useful for testing)
        if self._limit:
            query = query.rstrip() + f"\nLIMIT {self._limit}"

        result = await session.execute(text(query))
        rows = result.fetchall()

        # Convert to list of dicts
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    _DOMAIN_MAP: dict[str, SignalDomain] = {
        "Safety": SignalDomain.SAFETY,
        "Effectiveness": SignalDomain.EFFECTIVENESS,
        "Efficiency": SignalDomain.EFFICIENCY,
    }

    def _map_domain(self, domain_str: str | None) -> SignalDomain:
        """Map dbt domain string to SignalDomain enum.

        Args:
            domain_str: Domain string from fct_signals ('Efficiency', 'Safety', etc.)

        Returns:
            SignalDomain enum value. Defaults to EFFICIENCY if unknown.
        """
        return self._DOMAIN_MAP.get(domain_str or "", SignalDomain.EFFICIENCY)

    async def hydrate_signals(self) -> dict[str, int]:
        """Hydrate signals from dbt fct_signals into the application database.

        Queries the fct_signals dbt mart table and upserts all signals into
        the signals table using PostgreSQL's ON CONFLICT clause.

        Returns:
            dict[str, int]: Statistics about the hydration process:
                - signals_processed: Number of signals read from fct_signals
                - signals_created: Number of new signals inserted
                - signals_updated: Number of existing signals updated
                - signals_skipped: Number of signals skipped due to errors

        Example:
            >>> hydrator = SignalHydrator()
            >>> stats = await hydrator.hydrate_signals()
            >>> print(stats)
            {'signals_processed': 500, 'signals_created': 450, 'signals_updated': 50, 'signals_skipped': 0}
        """
        stats = {
            "signals_processed": 0,
            "signals_created": 0,
            "signals_updated": 0,
            "signals_skipped": 0,
        }

        # Query signals data first in a separate session
        async with self._session_factory() as session:
            try:
                fct_signals = await self._query_fct_signals(session)
            except Exception as e:
                logger.error("Failed to query fct_signals: %s", e)
                logger.info("Ensure dbt run has been executed and fct_signals table exists")
                return stats

            if not fct_signals:
                logger.warning("No signals found in fct_signals table")
                return stats

            logger.info("Found %d signals in fct_signals", len(fct_signals))

        # Process signals in batches using bulk INSERT...ON CONFLICT
        # This reduces round-trips from N to ~1 per batch
        # Note: PostgreSQL has a 32,767 parameter limit per query. With 28 columns
        # per signal record, max batch size = floor(32767/28) = 1170. Using 1000
        # for safety margin and round number.
        batch_size = 1000
        total_batches = (len(fct_signals) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(fct_signals))
            batch = fct_signals[start_idx:end_idx]

            # Prepare all records for batch insert
            records = []
            skipped = 0
            for signal_data in batch:
                try:
                    record = self._prepare_signal_record(signal_data)
                    records.append(record)
                except Exception as e:
                    logger.warning(
                        "Failed to prepare signal %s/%s: %s",
                        signal_data.get("canonical_node_id"),
                        signal_data.get("metric_id"),
                        e,
                    )
                    skipped += 1

            if not records:
                stats["signals_skipped"] += skipped
                continue

            # Execute bulk upsert for the entire batch in one statement
            async with self._session_factory() as session:
                try:
                    count = await self._bulk_upsert_signals(session, records)
                    await session.commit()
                    stats["signals_processed"] += count
                    stats["signals_created"] += count  # Simplified: treat all as creates
                    stats["signals_skipped"] += skipped
                    logger.info("Batch %d/%d: committed %d signals, skipped %d", batch_num + 1, total_batches, count, skipped)
                except Exception as e:
                    logger.error("Failed to commit batch %d: %s", batch_num + 1, e)
                    await session.rollback()
                    stats["signals_skipped"] += len(records) + skipped

        logger.info(
            "Signal hydration complete: %d processed, %d created, %d updated, %d skipped",
            stats["signals_processed"],
            stats["signals_created"],
            stats["signals_updated"],
            stats["signals_skipped"],
        )
        return stats

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        """Convert a value to Decimal, returning None for null/invalid values."""
        if value is None:
            return None
        return Decimal(str(value))

    def _prepare_signal_record(self, signal_data: dict[str, Any]) -> dict[str, Any]:
        """Prepare a signal record dictionary from dbt data.

        Transforms fct_signals columns to Signal model fields, handling
        enum parsing and value conversions.

        Args:
            signal_data: Signal data dictionary from fct_signals query.

        Returns:
            dict: Record ready for bulk insert.
        """
        canonical_node_id = signal_data["canonical_node_id"]
        metric_id = signal_data["metric_id"]

        # Map domain from dbt
        domain = self._map_domain(signal_data.get("domain"))

        # Use facility_id directly as facility name (no mapping for now)
        facility_id = signal_data.get("facility_id")
        facility_name = facility_id if facility_id else "Unknown Facility"

        # Use detected_at from dbt or current time
        detected_at = signal_data.get("detected_at")
        if detected_at is None:
            detected_at = datetime.now(UTC)

        to_decimal = self._to_decimal

        return {
            # Core identifiers
            "canonical_node_id": canonical_node_id,
            "metric_id": metric_id,
            "domain": domain,
            "system_name": signal_data.get("system_name"),
            "facility": facility_name,
            "facility_id": facility_id,
            "service_line": signal_data.get("service_line") or "Unknown",
            "sub_service_line": signal_data.get("sub_service_line"),
            "description": signal_data.get("description") or f"{metric_id} anomaly detected",
            "metric_value": to_decimal(signal_data.get("metric_value")) or Decimal("0"),
            "peer_mean": to_decimal(signal_data.get("peer_mean")),
            "percentile_rank": to_decimal(signal_data.get("percentile_rank")),
            "encounters": signal_data.get("encounters"),
            "detected_at": detected_at,
            "temporal_node_id": signal_data.get("temporal_node_id"),
            # Entity grouping
            "entity_dimensions": signal_data.get("entity_dimensions"),
            "entity_dimensions_hash": signal_data.get("entity_dimensions_hash"),
            "groupby_label": signal_data.get("groupby_label") or "Facility-wide",
            "group_value": signal_data.get("group_value") or "Facility-wide",
            # Temporal statistics (kept for trend display)
            "metric_trend_timeline": signal_data.get("metric_trend_timeline"),
            "trend_direction": signal_data.get("trend_direction"),
            # 9 Signal Type classification
            "simplified_signal_type": signal_data.get("simplified_signal_type"),
            "simplified_severity": signal_data.get("simplified_severity"),
            "simplified_severity_range": signal_data.get("simplified_severity_range"),
            "simplified_inputs": signal_data.get("simplified_inputs"),
            "simplified_indicators": signal_data.get("simplified_indicators"),
            "simplified_reasoning": signal_data.get("simplified_reasoning"),
            "simplified_severity_calculation": signal_data.get("simplified_severity_calculation"),
            # Metadata (use database column names)
            "metadata": signal_data.get("metadata"),
            "metadata_per_period": signal_data.get("metadata_per_period"),
            # Peer percentile trends (for reference band visualization)
            "peer_percentile_trends": signal_data.get("peer_percentile_trends"),
        }

    async def _bulk_upsert_signals(
        self,
        session: AsyncSession,
        records: list[dict[str, Any]],
    ) -> int:
        """Bulk upsert signals using a single multi-row INSERT...ON CONFLICT.

        This is significantly faster than individual upserts, reducing database
        round-trips from N to 1 per batch.

        Args:
            session: Database session.
            records: List of signal record dictionaries.

        Returns:
            int: Number of signals upserted.
        """
        if not records:
            return 0

        # Build multi-row insert statement using table (not ORM) to avoid
        # MetaData class naming conflict with the 'metadata' column
        insert_stmt = insert(Signal.__table__).values(records)

        # On conflict, update all mutable fields
        upsert_stmt = insert_stmt.on_conflict_do_update(
            constraint="uq_signals_entity_metric_detected",
            set_={
                "description": insert_stmt.excluded.description,
                "metric_value": insert_stmt.excluded.metric_value,
                "peer_mean": insert_stmt.excluded.peer_mean,
                "percentile_rank": insert_stmt.excluded.percentile_rank,
                "encounters": insert_stmt.excluded.encounters,
                "temporal_node_id": insert_stmt.excluded.temporal_node_id,
                "system_name": insert_stmt.excluded.system_name,
                # Entity grouping
                "entity_dimensions": insert_stmt.excluded.entity_dimensions,
                "entity_dimensions_hash": insert_stmt.excluded.entity_dimensions_hash,
                "groupby_label": insert_stmt.excluded.groupby_label,
                "group_value": insert_stmt.excluded.group_value,
                # Temporal statistics (kept for trend display)
                "metric_trend_timeline": insert_stmt.excluded.metric_trend_timeline,
                "trend_direction": insert_stmt.excluded.trend_direction,
                # 9 Signal Type classification
                "simplified_signal_type": insert_stmt.excluded.simplified_signal_type,
                "simplified_severity": insert_stmt.excluded.simplified_severity,
                "simplified_severity_range": insert_stmt.excluded.simplified_severity_range,
                "simplified_inputs": insert_stmt.excluded.simplified_inputs,
                "simplified_indicators": insert_stmt.excluded.simplified_indicators,
                "simplified_reasoning": insert_stmt.excluded.simplified_reasoning,
                "simplified_severity_calculation": insert_stmt.excluded.simplified_severity_calculation,
                # Metadata
                "metadata": insert_stmt.excluded.metadata,
                "metadata_per_period": insert_stmt.excluded.metadata_per_period,
                # Peer percentile trends (for reference band visualization)
                "peer_percentile_trends": insert_stmt.excluded.peer_percentile_trends,
            },
        )

        await session.execute(upsert_stmt)
        return len(records)

    async def get_signal_count(self) -> int:
        """Get the current count of signals in the application database.

        Returns:
            int: Number of signals in the signals table.
        """
        async with self._session_factory() as session:
            result = await session.execute(select(Signal.id))
            return len(result.all())

    async def get_fct_signal_count(self) -> int:
        """Get the count of signals in the dbt fct_signals table.

        Useful for comparing source (dbt) vs destination (app) counts.

        Returns:
            int: Number of signals in fct_signals, or 0 if table doesn't exist.

        Raises:
            None: Exceptions are caught and logged.
        """
        async with self._session_factory() as session:
            try:
                query = "SELECT COUNT(*) FROM public_marts.fct_signals"
                if self.run_id:
                    query += f" WHERE run_id = '{self.run_id}'"
                result = await session.execute(text(query))
                count = result.scalar_one_or_none()
                return count or 0
            except Exception as e:
                logger.warning("Could not count fct_signals: %s", e)
                return 0

    async def get_technical_details(
        self,
        canonical_node_id: str,
        entity_dimensions_hash: str | None = None,
    ) -> dict[str, Any] | None:
        """Fetch technical details for a signal from fct_signals.

        Args:
            canonical_node_id: The signal's canonical node ID.
            entity_dimensions_hash: Optional hash for precise entity lookup.
                If None, returns the first matching row for the node.

        Returns:
            Dict of technical details or None if not found.
        """
        to_decimal = self._to_decimal

        async with self._session_factory() as session:
            # Use different queries based on whether hash is provided
            # (asyncpg doesn't support :param IS NULL pattern)
            if entity_dimensions_hash is not None:
                result = await session.execute(
                    text(TECHNICAL_DETAILS_QUERY),
                    {
                        "canonical_node_id": canonical_node_id,
                        "entity_dimensions_hash": entity_dimensions_hash,
                    },
                )
            else:
                result = await session.execute(
                    text(TECHNICAL_DETAILS_QUERY_NO_HASH),
                    {"canonical_node_id": canonical_node_id},
                )
            row = result.mappings().fetchone()
            if row is None:
                return None

            return {
                # Entity identification
                "entity_dimensions_hash": row.get("entity_dimensions_hash"),
                # Statistical methods (JSONB array with all method details)
                "statistical_methods": row.get("statistical_methods"),
                # Primary z-score values
                "simple_zscore": to_decimal(row.get("simple_zscore")),
                "robust_zscore": to_decimal(row.get("robust_zscore")),
                "latest_simple_zscore": to_decimal(row.get("latest_simple_zscore")),
                "mean_simple_zscore": to_decimal(row.get("mean_simple_zscore")),
                "latest_robust_zscore": to_decimal(row.get("latest_robust_zscore")),
                "mean_robust_zscore": to_decimal(row.get("mean_robust_zscore")),
                # Peer context
                "percentile_rank": to_decimal(row.get("percentile_rank")),
                "peer_std": to_decimal(row.get("peer_std")),
                "peer_count": row.get("peer_count"),
                "encounters": row.get("encounters"),
                # Global benchmark
                "global_metric_mean": to_decimal(row.get("global_metric_mean")),
                "global_metric_std": to_decimal(row.get("global_metric_std")),
                # Temporal/slope statistics
                "slope": to_decimal(row.get("slope")),
                "slope_percentile": to_decimal(row.get("slope_percentile")),
                "acceleration": to_decimal(row.get("acceleration")),
                "trend_direction": row.get("trend_direction"),
                "momentum": row.get("momentum"),
                "monthly_z_scores": row.get("monthly_z_scores"),
                # Anomaly labels (scalar for backward compatibility)
                "simple_zscore_anomaly": row.get("simple_zscore_anomaly"),
                "robust_zscore_anomaly": row.get("robust_zscore_anomaly"),
                "latest_simple_zscore_anomaly": row.get("latest_simple_zscore_anomaly"),
                "mean_simple_zscore_anomaly": row.get("mean_simple_zscore_anomaly"),
                "latest_robust_zscore_anomaly": row.get("latest_robust_zscore_anomaly"),
                "mean_robust_zscore_anomaly": row.get("mean_robust_zscore_anomaly"),
                "slope_anomaly": row.get("slope_anomaly"),
                # 3D Matrix tiers (kept for technical display)
                "magnitude_tier": row.get("magnitude_tier"),
                "trajectory_tier": row.get("trajectory_tier"),
                "consistency_tier": row.get("consistency_tier"),
                "coefficient_of_variation": to_decimal(row.get("coefficient_of_variation")),
                # 9 Signal Type classification (glass box details)
                "simplified_signal_type": row.get("simplified_signal_type"),
                "simplified_severity": row.get("simplified_severity"),
                "simplified_severity_range": row.get("simplified_severity_range"),
                "simplified_inputs": row.get("simplified_inputs"),
                "simplified_indicators": row.get("simplified_indicators"),
                "simplified_reasoning": row.get("simplified_reasoning"),
                "simplified_severity_calculation": row.get("simplified_severity_calculation"),
                # Data quality indicators (currently null - placeholder for future implementation)
                "data_quality_fallback_rate": None,  # TODO: Implement when dbt model available
                "data_quality_missing_rate": None,  # TODO: Implement when dbt model available
                "data_quality_suppressed": False,  # All signals in fct_signals are non-suppressed
            }
