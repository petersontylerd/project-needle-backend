"""Metric Query Generator Service.

Generates SQL queries for metrics based on semantic model definitions
and executes them against the database.

This is a lightweight SQL generator that works with the semantic manifest
without requiring the full dbt-metricflow SDK.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.semantic_manifest_service import (
    SemanticManifestService,
    SemanticModelDefinition,
    get_semantic_manifest_service,
)

logger = logging.getLogger(__name__)


# Aggregation function mapping from MetricFlow to SQL
AGG_FUNCTIONS = {
    "count": "COUNT",
    "sum": "SUM",
    "average": "AVG",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "count_distinct": "COUNT(DISTINCT",
}


@dataclass
class MetricQueryResult:
    """Result from a metric query."""

    metric_name: str
    dimensions: list[str]
    rows: list[dict[str, Any]]
    sql: str
    row_count: int


class MetricQueryService:
    """Generates and executes SQL queries for metrics.

    Uses semantic model definitions to build proper aggregation queries.
    Supports:
    - Simple metrics (direct measure aggregation)
    - Filtered metrics (with WHERE clause from metric filter)
    - Grouped metrics (with GROUP BY dimensions)
    - Time-ranged queries
    """

    def __init__(self, manifest_service: SemanticManifestService | None = None) -> None:
        """Initialize the service.

        Args:
            manifest_service: Optional SemanticManifestService instance.
                If not provided, uses the singleton.
        """
        self._manifest = manifest_service or get_semantic_manifest_service()

    def _build_aggregation(self, agg: str, expr: str) -> str:
        """Build SQL aggregation expression.

        Args:
            agg: Aggregation type (count, sum, average, etc.)
            expr: Expression to aggregate.

        Returns:
            SQL aggregation expression.
        """
        if agg == "count_distinct":
            return f"COUNT(DISTINCT {expr})"
        func = AGG_FUNCTIONS.get(agg, "SUM")
        return f"{func}({expr})"

    def _resolve_dimension_filter(self, filter_expr: str, semantic_model: SemanticModelDefinition) -> str:
        """Resolve MetricFlow dimension references to SQL column expressions.

        Converts {{ Dimension('signal__severity') }} to the actual column expression.

        Args:
            filter_expr: Filter expression with MetricFlow dimension references.
            semantic_model: Semantic model containing dimension definitions.

        Returns:
            SQL WHERE clause expression.
        """
        # Pattern: {{ Dimension('entity__dimension') }}
        pattern = r"\{\{\s*Dimension\(['\"](\w+)__(\w+)['\"]\)\s*\}\}"

        def replace_dimension(match: re.Match[str]) -> str:
            _entity_name = match.group(1)  # Captured for potential entity-specific lookup
            dim_name = match.group(2)
            # Find the dimension in the semantic model
            for dim in semantic_model.dimensions:
                if dim.name == dim_name:
                    return dim.expr
            # Fallback to dimension name
            return dim_name

        return re.sub(pattern, replace_dimension, filter_expr)

    def _get_qualified_table_name(self, semantic_model: SemanticModelDefinition) -> str:
        """Get fully qualified table name.

        Args:
            semantic_model: Semantic model definition.

        Returns:
            Qualified table name (schema.table).
        """
        if semantic_model.schema_name:
            return f"{semantic_model.schema_name}.{semantic_model.table_name}"
        return semantic_model.table_name

    def generate_metric_sql(
        self,
        metric_name: str,
        group_by: list[str] | None = None,
        where: str | None = None,
        time_range: tuple[date, date] | None = None,
        limit: int | None = None,
    ) -> str:
        """Generate SQL for a simple metric query.

        Args:
            metric_name: Name of the metric to query.
            group_by: List of dimension names to group by.
            where: Additional WHERE clause filter.
            time_range: Optional (start_date, end_date) tuple.
            limit: Maximum number of rows to return.

        Returns:
            SQL query string.

        Raises:
            ValueError: If metric not found or metric type not supported.
        """
        metric = self._manifest.get_metric(metric_name)
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found")

        if metric.type != "simple":
            raise ValueError(f"Only simple metrics supported for direct query, got {metric.type}. Derived metrics require additional implementation.")

        if not metric.measure_reference:
            raise ValueError(f"Metric '{metric_name}' has no measure reference")

        # Find measure context (semantic model + measure)
        context = self._manifest.find_measure_context(metric.measure_reference)
        if not context:
            raise ValueError(f"Measure '{metric.measure_reference}' not found")

        sm, measure = context
        table_name = self._get_qualified_table_name(sm)

        # Build SELECT clause
        agg_expr = self._build_aggregation(measure.agg, measure.expr)
        select_parts = [f"{agg_expr} AS {metric_name}"]

        # Add dimensions to SELECT
        group_by = group_by or []
        dim_expressions: dict[str, str] = {}
        for dim_name in group_by:
            dim = next((d for d in sm.dimensions if d.name == dim_name), None)
            if dim:
                dim_expressions[dim_name] = dim.expr
                select_parts.insert(0, f"{dim.expr} AS {dim_name}")
            else:
                logger.warning(
                    "Dimension '%s' not found in semantic model '%s'",
                    dim_name,
                    sm.name,
                )

        # Build WHERE clause parts
        where_parts: list[str] = []

        # Add metric-level filter (e.g., severity = 'Critical')
        if metric.filter_expression:
            resolved_filter = self._resolve_dimension_filter(metric.filter_expression, sm)
            where_parts.append(f"({resolved_filter})")

        # Add user-provided filter
        if where:
            where_parts.append(f"({where})")

        # Add time range filter
        if time_range and sm.default_time_dimension:
            start, end = time_range
            time_dim = next((d for d in sm.dimensions if d.name == sm.default_time_dimension), None)
            if time_dim:
                where_parts.append(f"{time_dim.expr} BETWEEN '{start}' AND '{end}'")

        # Build final query
        sql_parts = [f"SELECT {', '.join(select_parts)}", f"FROM {table_name}"]

        if where_parts:
            sql_parts.append(f"WHERE {' AND '.join(where_parts)}")

        if group_by:
            group_exprs = [dim_expressions.get(d, d) for d in group_by]
            sql_parts.append(f"GROUP BY {', '.join(group_exprs)}")
            # Order by first dimension
            sql_parts.append(f"ORDER BY {group_exprs[0]}")

        if limit:
            sql_parts.append(f"LIMIT {limit}")

        return "\n".join(sql_parts)

    async def query_metric(
        self,
        db: AsyncSession,
        metric_name: str,
        group_by: list[str] | None = None,
        where: str | None = None,
        time_range: tuple[date, date] | None = None,
        limit: int | None = None,
    ) -> MetricQueryResult:
        """Execute a metric query and return results.

        Args:
            db: AsyncSession for database access.
            metric_name: Name of the metric to query.
            group_by: List of dimension names to group by.
            where: Additional WHERE clause filter.
            time_range: Optional (start_date, end_date) tuple.
            limit: Maximum number of rows to return.

        Returns:
            MetricQueryResult with rows and metadata.

        Raises:
            ValueError: If metric not found or query generation fails.
        """
        sql = self.generate_metric_sql(
            metric_name=metric_name,
            group_by=group_by,
            where=where,
            time_range=time_range,
            limit=limit,
        )

        logger.debug("Executing metric query for '%s': %s", metric_name, sql)

        result = await db.execute(text(sql))
        rows = [dict(row._mapping) for row in result.fetchall()]

        return MetricQueryResult(
            metric_name=metric_name,
            dimensions=group_by or [],
            rows=rows,
            sql=sql,
            row_count=len(rows),
        )

    async def query_metric_aggregate(
        self,
        db: AsyncSession,
        metric_name: str,
    ) -> dict[str, Any] | None:
        """Query a metric without grouping for a single aggregate value.

        Args:
            db: AsyncSession for database access.
            metric_name: Name of the metric to query.

        Returns:
            Dict with metric value, or None if no data.
        """
        result = await self.query_metric(db, metric_name, limit=1)
        if result.rows:
            return result.rows[0]
        return None


# Singleton instance
_query_service: MetricQueryService | None = None


def get_metric_query_service() -> MetricQueryService:
    """Get or create the metric query service singleton.

    Returns:
        MetricQueryService instance.
    """
    global _query_service
    if _query_service is None:
        _query_service = MetricQueryService()
    return _query_service
