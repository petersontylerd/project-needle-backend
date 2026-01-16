"""Contribution service for querying dbt contribution tables.

This service queries the fct_contributions dbt mart table and transforms
the data into ContributionRecord and ContributionResponse objects for API output.

The contribution data flow:
1. JSONL files → load_insight_graph_to_dbt.py → raw_contributions
2. dbt run → raw_contributions → stg_contributions → fct_contributions
3. ContributionService queries fct_contributions → API responses
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.db.session import async_session_maker
from src.schemas.contribution import ContributionRecord, ContributionResponse

if TYPE_CHECKING:
    from src.db.models import Signal

logger = logging.getLogger(__name__)


# SQL query to fetch contributions for a parent node (DOWNWARD: children contributing to this parent)
# IMPORTANT: Filters by parent_node_id, parent_facility_id, AND parent_service_line to ensure
# contributions are only from the same facility AND service line context as the signal being viewed.
# This prevents sibling service lines from appearing in the contribution analysis.
FCT_CONTRIBUTIONS_BY_PARENT_QUERY = """
SELECT
    contribution_id,
    run_id,
    parent_node_id,
    child_node_id,
    parent_facility_id,
    parent_service_line,
    child_facility_id,
    child_service_line,
    child_sub_service_line,
    child_admission_status,
    child_discharge_status,
    child_payer_segment,
    child_admission_source,
    metric_id,
    contribution_method,
    child_value,
    parent_value,
    weight_field,
    weight_value,
    weight_share,
    excess_over_parent,
    contribution_weight,
    contribution_direction,
    contribution_rank,
    contribution_pct
FROM public_marts.fct_contributions
WHERE parent_node_id = :parent_node_id
  AND parent_facility_id = :parent_facility_id
  AND (parent_service_line = :parent_service_line
       OR (parent_service_line IS NULL AND :parent_service_line IS NULL))
ORDER BY contribution_rank
"""

# SQL query to fetch upward contribution (UPWARD: how this signal contributes to its parent)
# Used to show "This Signal's Impact" section in the UI.
FCT_UPWARD_CONTRIBUTION_QUERY = """
SELECT
    contribution_id,
    run_id,
    parent_node_id,
    child_node_id,
    parent_facility_id,
    parent_service_line,
    child_facility_id,
    child_service_line,
    child_sub_service_line,
    child_admission_status,
    child_discharge_status,
    child_payer_segment,
    child_admission_source,
    metric_id,
    contribution_method,
    child_value,
    parent_value,
    weight_field,
    weight_value,
    weight_share,
    excess_over_parent,
    contribution_weight,
    contribution_direction,
    contribution_rank,
    contribution_pct
FROM public_marts.fct_contributions
WHERE child_facility_id = :child_facility_id
  AND metric_id = :metric_id
  AND (child_service_line = :child_service_line
       OR (child_service_line IS NULL AND :child_service_line IS NULL))
  AND (child_sub_service_line = :child_sub_service_line
       OR (child_sub_service_line IS NULL AND :child_sub_service_line IS NULL))
ORDER BY contribution_rank
LIMIT 1
"""

# SQL query to fetch top contributors globally
FCT_TOP_CONTRIBUTORS_QUERY = """
SELECT
    contribution_id,
    run_id,
    parent_node_id,
    child_node_id,
    parent_facility_id,
    parent_service_line,
    child_facility_id,
    child_service_line,
    child_sub_service_line,
    metric_id,
    contribution_method,
    child_value,
    parent_value,
    weight_field,
    weight_value,
    weight_share,
    excess_over_parent,
    contribution_weight,
    contribution_direction,
    contribution_rank,
    contribution_pct
FROM public_marts.fct_contributions
WHERE contribution_rank = 1
ORDER BY contribution_weight DESC
LIMIT :top_n
"""


class ContributionServiceError(Exception):
    """Base exception for contribution service errors.

    Attributes:
        message: Error description.
        parent_node_id: Optional parent node ID for context.

    Example:
        >>> raise ContributionServiceError("Query failed", parent_node_id="losIndex__medicareId")
    """

    def __init__(self, message: str, parent_node_id: str | None = None) -> None:
        """Initialize the error with a message and optional context.

        Args:
            message: Error description.
            parent_node_id: Parent node ID for context.
        """
        self.message = message
        self.parent_node_id = parent_node_id
        super().__init__(f"{message}" + (f" (parent_node: {parent_node_id})" if parent_node_id else ""))


class ContributionService:
    """Service for querying contribution data from dbt fct_contributions table.

    This class queries the dbt fct_contributions mart table and transforms
    records into ContributionRecord objects for API output. It handles
    entity reconstruction from denormalized fields and provides methods
    for filtering and ranking contributors.

    Attributes:
        run_id: Optional run ID filter.

    Example:
        >>> service = ContributionService()
        >>> records = await service.get_contributions_for_parent("losIndex__medicareId")
        >>> for record in records:
        ...     print(f"{record.child_entity}: {record.excess_over_parent}")
    """

    def __init__(
        self,
        run_id: str | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            run_id: Optional run ID to filter contributions by.
            session_factory: Optional async session factory for database connections.
                If None, uses the default async_session_maker from src.db.session.
                Useful for testing with alternative database connections.
        """
        self.run_id = run_id
        self._session_factory = session_factory or async_session_maker

    async def get_contributions_for_parent(
        self,
        parent_node_id: str,
        parent_facility_id: str,
        parent_service_line: str | None = None,
        top_n: int | None = None,
    ) -> list[ContributionRecord]:
        """Get contribution records for a specific parent node and facility.

        Queries fct_contributions for all child contributions to the given
        parent node, filtered to the specific facility and service line context,
        and ordered by contribution rank.

        Args:
            parent_node_id: Canonical ID of the parent node.
            parent_facility_id: Medicare ID of the facility to filter by.
                This ensures contributions are only from the same facility
                as the signal being viewed.
            parent_service_line: Service line to filter by. If None, matches
                contributions where parent_service_line is also NULL (facility-wide).
                This prevents sibling service lines from appearing.
            top_n: Optional limit on number of results. If None, returns all.

        Returns:
            list[ContributionRecord]: Contribution records for the parent.
                Empty list if no contributions found.

        Raises:
            ContributionServiceError: If the query fails.

        Example:
            >>> service = ContributionService()
            >>> records = await service.get_contributions_for_parent(
            ...     parent_node_id="losIndex__medicareId__aggregate_time_period",
            ...     parent_facility_id="AFP658",
            ...     parent_service_line="Cardiology",
            ... )
            >>> len(records)
            5
        """
        async with self._session_factory() as session:
            try:
                rows = await self._query_contributions_by_parent(session, parent_node_id, parent_facility_id, parent_service_line)
            except Exception as e:
                logger.error(
                    "Failed to query contributions for %s (facility %s, service_line %s): %s",
                    parent_node_id,
                    parent_facility_id,
                    parent_service_line,
                    e,
                )
                raise ContributionServiceError(f"Failed to query contributions: {e}", parent_node_id=parent_node_id) from e

        records = [self._row_to_record(row) for row in rows]

        if top_n is not None:
            records = records[:top_n]

        logger.info(
            "Found %d contributions for parent %s (facility %s, service_line %s)",
            len(records),
            parent_node_id,
            parent_facility_id,
            parent_service_line,
        )
        return records

    async def get_top_contributors_global(self, top_n: int = 10) -> list[ContributionRecord]:
        """Get top contributors across all parent nodes.

        Queries fct_contributions for the highest impact contributors
        globally, useful for dashboard summaries.

        Args:
            top_n: Number of top contributors to return. Defaults to 10.

        Returns:
            list[ContributionRecord]: Top contributors sorted by contribution weight.

        Example:
            >>> service = ContributionService()
            >>> top = await service.get_top_contributors_global(top_n=5)
            >>> len(top)
            5
        """
        async with self._session_factory() as session:
            try:
                rows = await self._query_top_contributors(session, top_n)
            except Exception as e:
                logger.warning("Failed to query top contributors: %s", e)
                return []

        records = [self._row_to_record(row) for row in rows]
        logger.info("Found %d top contributors", len(records))
        return records

    async def _query_contributions_by_parent(
        self,
        session: AsyncSession,
        parent_node_id: str,
        parent_facility_id: str,
        parent_service_line: str | None = None,
    ) -> list[dict[str, Any]]:
        """Query contributions for a specific parent node, facility, and service line.

        Args:
            session: Async database session.
            parent_node_id: Parent node to query for.
            parent_facility_id: Facility ID to filter by.
            parent_service_line: Service line to filter by. If None, matches
                contributions where parent_service_line is also NULL.

        Returns:
            List of contribution rows as dictionaries.
        """
        query = FCT_CONTRIBUTIONS_BY_PARENT_QUERY
        if self.run_id:
            query = query.replace(
                "WHERE parent_node_id = :parent_node_id",
                "WHERE parent_node_id = :parent_node_id AND run_id = :run_id",
            )

        params: dict[str, Any] = {
            "parent_node_id": parent_node_id,
            "parent_facility_id": parent_facility_id,
            "parent_service_line": parent_service_line,
        }
        if self.run_id:
            params["run_id"] = self.run_id

        result = await session.execute(text(query), params)
        rows = result.fetchall()
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    async def _query_top_contributors(self, session: AsyncSession, top_n: int) -> list[dict[str, Any]]:
        """Query top contributors globally.

        Args:
            session: Async database session.
            top_n: Number of top contributors.

        Returns:
            List of contribution rows as dictionaries.
        """
        query = FCT_TOP_CONTRIBUTORS_QUERY
        if self.run_id:
            query = query.replace(
                "WHERE contribution_rank <= :top_n",
                "WHERE contribution_rank <= :top_n AND run_id = :run_id",
            )

        params: dict[str, Any] = {"top_n": top_n}
        if self.run_id:
            params["run_id"] = self.run_id

        result = await session.execute(text(query), params)
        rows = result.fetchall()
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def _row_to_record(self, row: dict[str, Any]) -> ContributionRecord:
        """Convert a database row to a ContributionRecord.

        Reconstructs parent_entity and child_entity dicts from ALL denormalized
        fields including service lines, admission status, discharge status, etc.

        Args:
            row: Database row as dictionary.

        Returns:
            ContributionRecord with entity dicts populated.
        """
        # Reconstruct parent entity from denormalized fields
        parent_entity: dict[str, str] = {}
        if row.get("parent_facility_id"):
            parent_entity["medicareId"] = row["parent_facility_id"]
        if row.get("parent_service_line"):
            parent_entity["vizientServiceLine"] = row["parent_service_line"]

        # Reconstruct child entity from ALL denormalized dimension fields
        # This mapping defines (db_column, entity_key) pairs for reconstruction
        dimension_mappings = [
            ("child_facility_id", "medicareId"),
            ("child_service_line", "vizientServiceLine"),
            ("child_sub_service_line", "vizientSubServiceLine"),
            ("child_admission_status", "admissionStatus"),
            ("child_discharge_status", "dischargeStatus"),
            ("child_payer_segment", "payerSegment"),
            ("child_admission_source", "admissionSource"),
        ]

        child_entity: dict[str, str] | None = None
        for db_col, entity_key in dimension_mappings:
            value = row.get(db_col)
            if value:
                if child_entity is None:
                    child_entity = {}
                child_entity[entity_key] = value

        return ContributionRecord(
            method=row.get("contribution_method") or "weighted_mean",
            child_value=float(row["child_value"]) if row.get("child_value") is not None else None,
            parent_value=float(row["parent_value"]) if row.get("parent_value") is not None else None,
            weight_field=row.get("weight_field") or "encounters",
            weight_value=float(row.get("weight_value") or 0),
            weight_share=float(row.get("weight_share") or 0),
            weighted_child_value=None,  # Not in fct_contributions
            contribution_value=float(row["contribution_pct"]) / 100 if row.get("contribution_pct") is not None else None,
            raw_component=None,  # Not in fct_contributions
            excess_over_parent=float(row["excess_over_parent"]) if row.get("excess_over_parent") is not None else None,
            parent_node_file="",  # Not in fct_contributions
            parent_node_id=row.get("parent_node_id") or "",
            parent_entity=parent_entity,
            child_entity=child_entity,
        )

    def to_response(self, record: ContributionRecord) -> ContributionResponse:
        """Convert a ContributionRecord to a ContributionResponse for API output.

        Transforms the raw record data into a frontend-friendly format with
        derived fields like percentages and human-readable labels.

        Args:
            record: The raw contribution record to transform.

        Returns:
            ContributionResponse: Transformed response suitable for API output.

        Example:
            >>> service = ContributionService()
            >>> records = await service.get_contributions_for_parent(parent_id)
            >>> responses = [service.to_response(r) for r in records]
        """
        # Calculate weight share percentage
        weight_share_percent = Decimal(str(round(record.weight_share * 100, 1)))

        # Calculate excess over parent as percentage
        excess_over_parent_percent: Decimal | None = None
        if record.excess_over_parent is not None and record.parent_value:
            excess_pct = (record.excess_over_parent / record.parent_value) * 100
            excess_over_parent_percent = Decimal(str(round(excess_pct, 1)))

        # Extract child entity label
        child_entity_label = self._extract_child_entity_label(record)

        # Generate description
        description = self._generate_description(record, child_entity_label, weight_share_percent, excess_over_parent_percent)

        return ContributionResponse(
            method=record.method,
            child_value=Decimal(str(record.child_value)) if record.child_value is not None else None,
            parent_value=Decimal(str(record.parent_value)) if record.parent_value is not None else None,
            weight_field=record.weight_field,
            weight_value=Decimal(str(record.weight_value)),
            weight_share_percent=weight_share_percent,
            contribution_value=(Decimal(str(record.contribution_value)) if record.contribution_value is not None else None),
            excess_over_parent_percent=excess_over_parent_percent,
            parent_node_id=record.parent_node_id,
            parent_entity=record.parent_entity,
            child_entity=record.child_entity,
            child_entity_label=child_entity_label,
            description=description,
        )

    def _extract_child_entity_label(self, record: ContributionRecord) -> str:
        """Extract a human-readable label for the child entity.

        Looks for vizientServiceLine or vizientSubServiceLine in the child entity,
        otherwise uses the full child entity as the label.

        Args:
            record: The contribution record.

        Returns:
            str: Human-readable label for the child entity.
        """
        if not record.child_entity:
            return "Root"

        # Prefer service line labels
        if "vizientSubServiceLine" in record.child_entity:
            return record.child_entity["vizientSubServiceLine"]
        if "vizientServiceLine" in record.child_entity:
            return record.child_entity["vizientServiceLine"]

        # Fall back to the first non-parent entity field
        parent_keys = set(record.parent_entity.keys())
        for key, value in record.child_entity.items():
            if key not in parent_keys:
                return value

        # Last resort: use first child entity value
        return next(iter(record.child_entity.values()), "Unknown")

    def _generate_description(
        self,
        record: ContributionRecord,
        child_label: str,
        weight_share_percent: Decimal,
        excess_over_parent_percent: Decimal | None,
    ) -> str:
        """Generate a human-readable description for the contribution.

        Creates text describing how the child entity contributes relative
        to the parent average.

        Args:
            record: The contribution record.
            child_label: Human-readable label for child entity.
            weight_share_percent: Weight share as percentage.
            excess_over_parent_percent: Excess over parent as percentage.

        Returns:
            str: Description text for frontend display.
        """
        if excess_over_parent_percent is None:
            return f"{child_label} accounts for {weight_share_percent}% of total volume."

        if excess_over_parent_percent > 0:
            return f"{child_label} contributes {excess_over_parent_percent}% above parent average, accounting for {weight_share_percent}% of total volume."
        elif excess_over_parent_percent < 0:
            return f"{child_label} performs {abs(excess_over_parent_percent)}% below parent average, representing {weight_share_percent}% of total volume."
        else:
            return f"{child_label} matches parent average, representing {weight_share_percent}% of total volume."

    async def get_upward_contribution(
        self,
        child_facility_id: str,
        child_service_line: str | None,
        child_sub_service_line: str | None,
        metric_id: str,
    ) -> ContributionRecord | None:
        """Get contribution record showing how this signal contributes to its parent.

        Queries fct_contributions where this signal is the child contributing to
        its parent aggregate. This is the "upward" view showing the signal's impact.

        Args:
            child_facility_id: Medicare ID of the facility.
            child_service_line: Service line of this signal (None for facility-wide).
            child_sub_service_line: Sub-service line (None if not applicable).
            metric_id: Metric identifier (e.g., "losIndex", "pctIcuEncountersIcuFile").

        Returns:
            ContributionRecord if found, None if no parent contribution exists
            (e.g., for facility-wide signals that have no parent).

        Example:
            >>> service = ContributionService()
            >>> record = await service.get_upward_contribution(
            ...     child_facility_id="AFP658",
            ...     child_service_line="Cardiology",
            ...     child_sub_service_line=None,
            ...     metric_id="losIndex",
            ... )
            >>> if record:
            ...     print(f"Cardiology contributes {record.excess_over_parent} to facility-wide")
        """
        async with self._session_factory() as session:
            try:
                rows = await self._query_upward_contribution(
                    session,
                    child_facility_id,
                    child_service_line,
                    child_sub_service_line,
                    metric_id,
                )
            except Exception as e:
                logger.warning(
                    "Failed to query upward contribution for facility %s, service_line %s, metric %s: %s",
                    child_facility_id,
                    child_service_line,
                    metric_id,
                    e,
                )
                return None

        if not rows:
            logger.debug(
                "No upward contribution found for facility %s, service_line %s, metric %s",
                child_facility_id,
                child_service_line,
                metric_id,
            )
            return None

        record = self._row_to_record(rows[0])
        logger.info(
            "Found upward contribution for facility %s, service_line %s, metric %s",
            child_facility_id,
            child_service_line,
            metric_id,
        )
        return record

    async def _query_upward_contribution(
        self,
        session: AsyncSession,
        child_facility_id: str,
        child_service_line: str | None,
        child_sub_service_line: str | None,
        metric_id: str,
    ) -> list[dict[str, Any]]:
        """Query upward contribution where this signal is the child.

        Args:
            session: Async database session.
            child_facility_id: Facility ID of the child entity.
            child_service_line: Service line of the child entity.
            child_sub_service_line: Sub-service line of the child entity.
            metric_id: Metric identifier.

        Returns:
            List of contribution rows (should be 0 or 1 due to LIMIT 1).
        """
        query = FCT_UPWARD_CONTRIBUTION_QUERY
        if self.run_id:
            query = query.replace(
                "WHERE child_facility_id = :child_facility_id",
                "WHERE child_facility_id = :child_facility_id AND run_id = :run_id",
            )

        params: dict[str, Any] = {
            "child_facility_id": child_facility_id,
            "child_service_line": child_service_line,
            "child_sub_service_line": child_sub_service_line,
            "metric_id": metric_id,
        }
        if self.run_id:
            params["run_id"] = self.run_id

        result = await session.execute(text(query), params)
        rows = result.fetchall()
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    async def get_hierarchical_contributions(
        self,
        signal: Signal,
        top_n: int = 10,
    ) -> tuple[ContributionRecord | None, list[ContributionRecord], str]:
        """Get both upward and downward contributions for a signal.

        This method retrieves:
        1. Upward contribution: How this signal contributes to its parent
        2. Downward contributions: How child entities contribute to this signal

        Args:
            signal: The Signal model instance with entity dimensions.
            top_n: Maximum number of downward contributions to return.

        Returns:
            Tuple of (upward_contribution, downward_contributions, hierarchy_level)
            - upward_contribution: ContributionRecord or None (for facility-wide)
            - downward_contributions: List of ContributionRecord
            - hierarchy_level: "facility", "service_line", or "sub_service_line"

        Example:
            >>> service = ContributionService()
            >>> upward, downward, level = await service.get_hierarchical_contributions(signal)
            >>> if upward:
            ...     print(f"This signal contributes {upward.excess_over_parent} to parent")
            >>> for d in downward:
            ...     print(f"Child {d.child_entity} contributes {d.excess_over_parent}")
        """
        hierarchy_level = self._determine_hierarchy_level(signal)

        # Get upward contribution (None for facility-wide signals)
        upward: ContributionRecord | None = None
        if hierarchy_level != "facility":
            # Normalize service_line: treat "Facility-wide" as None
            child_service_line: str | None = signal.service_line
            if child_service_line == "Facility-wide":
                child_service_line = None

            # Normalize sub_service_line: treat empty strings as None
            child_sub_service_line: str | None = signal.sub_service_line
            if not child_sub_service_line or child_sub_service_line == "None":
                child_sub_service_line = None

            upward = await self.get_upward_contribution(
                child_facility_id=signal.facility_id or "",
                child_service_line=child_service_line,
                child_sub_service_line=child_sub_service_line,
                metric_id=signal.metric_id,
            )

        # Get downward contributions with proper service line filter
        parent_service_line: str | None = None
        if hierarchy_level == "service_line":
            # For service line level, filter by this service line
            parent_service_line = signal.service_line
            if parent_service_line == "Facility-wide":
                parent_service_line = None
        elif hierarchy_level == "sub_service_line":
            # For sub-service line level, filter by parent service line
            parent_service_line = signal.service_line
            if parent_service_line == "Facility-wide":
                parent_service_line = None

        downward = await self.get_contributions_for_parent(
            parent_node_id=signal.canonical_node_id,
            parent_facility_id=signal.facility_id or "",
            parent_service_line=parent_service_line,
            top_n=top_n,
        )

        logger.info(
            "Hierarchical contributions for signal %s: level=%s, upward=%s, downward=%d",
            signal.id,
            hierarchy_level,
            "found" if upward else "none",
            len(downward),
        )

        return upward, downward, hierarchy_level

    def _determine_hierarchy_level(self, signal: Signal) -> str:
        """Determine hierarchy level from signal dimensions.

        Args:
            signal: The Signal model instance.

        Returns:
            One of: "facility", "service_line", or "sub_service_line"
        """
        # Check for sub-service line level
        if signal.sub_service_line and signal.sub_service_line not in ("None", ""):
            return "sub_service_line"

        # Check for service line level
        if signal.service_line and signal.service_line != "Facility-wide":
            return "service_line"

        # Default to facility level
        return "facility"

    def get_top_contributors(
        self,
        records: list[ContributionRecord],
        top_n: int = 5,
        sort_by: str = "excess_over_parent",
    ) -> list[ContributionRecord]:
        """Get the top N contributors sorted by specified field.

        Filters and sorts contribution records to identify the most significant
        contributors, useful for root cause analysis displays.

        Args:
            records: List of contribution records to filter.
            top_n: Number of top contributors to return. Defaults to 5.
            sort_by: Field to sort by. Options: "excess_over_parent", "weight_share",
                "contribution_value". Defaults to "excess_over_parent".

        Returns:
            list[ContributionRecord]: Top N contributors sorted by the specified field.

        Example:
            >>> service = ContributionService()
            >>> records = await service.get_contributions_for_parent(parent_id)
            >>> top_5 = service.get_top_contributors(records, top_n=5)
        """

        def get_sort_value(record: ContributionRecord) -> float:
            if sort_by == "weight_share":
                return record.weight_share
            elif sort_by == "contribution_value":
                return record.contribution_value or 0.0
            else:  # excess_over_parent
                return abs(record.excess_over_parent or 0.0)

        # Filter out records without the sort field value
        valid_records = [r for r in records if get_sort_value(r) is not None]

        # Sort by absolute value (highest impact first)
        sorted_records = sorted(valid_records, key=get_sort_value, reverse=True)

        return sorted_records[:top_n]
