"""Graph synchronization service for healthcare ontology.

This service syncs data from relational tables to the Apache AGE graph database.
It creates vertices for entities (Facility, Metric, Signal, etc.) and edges
for their relationships.
"""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from .cypher import _escape_value, age_query_wrapper, build_create_edge, build_create_vertex
from .schema import GRAPH_NAME

logger = logging.getLogger(__name__)


class GraphSyncService:
    """Synchronizes relational data to the Apache AGE graph.

    This service reads from PostgreSQL relational tables and creates
    corresponding vertices and edges in the healthcare_ontology graph.

    Usage:
        async with async_session_maker() as session:
            service = GraphSyncService(session)
            stats = await service.sync_all()
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the sync service.

        Args:
            session: SQLAlchemy async session for database operations.
        """
        self.session = session
        self._age_loaded = False

    async def _ensure_age_loaded(self) -> None:
        """Ensure AGE extension is loaded for this session.

        Apache AGE requires LOAD 'age' to be run once per session
        before executing any Cypher queries.
        """
        if self._age_loaded:
            return

        try:
            # Load AGE extension and set search path for this session
            await self.session.execute(text("LOAD 'age'"))
            await self.session.execute(text('SET search_path = public, ag_catalog, "$user"'))
            self._age_loaded = True
            logger.debug("AGE extension loaded for session")
        except SQLAlchemyError as exc:
            logger.error("Failed to load AGE extension: %s", exc)
            raise RuntimeError(f"Failed to initialize AGE: {exc}") from exc

    async def _execute_cypher(self, cypher: str) -> list[Any]:
        """Execute a Cypher query via AGE.

        Args:
            cypher: The Cypher query to execute.

        Returns:
            List of result rows.

        Raises:
            RuntimeError: If query execution fails.
        """
        await self._ensure_age_loaded()
        sql = age_query_wrapper(cypher, GRAPH_NAME)
        try:
            result = await self.session.execute(text(sql))
            return list(result.fetchall())
        except SQLAlchemyError as exc:
            logger.error("Cypher query failed: %s", cypher[:200])
            raise RuntimeError(f"Graph query failed: {exc}") from exc

    async def _vertex_exists(self, label: str, vertex_id: str) -> bool:
        """Check if a vertex already exists.

        Args:
            label: Vertex label.
            vertex_id: Vertex id property.

        Returns:
            True if vertex exists, False otherwise.
        """
        cypher = f"MATCH (n:{label} {{id: {_escape_value(vertex_id)}}}) RETURN n LIMIT 1"
        results = await self._execute_cypher(cypher)
        return len(results) > 0

    async def _create_vertex_if_not_exists(
        self,
        label: str,
        properties: dict[str, Any],
    ) -> bool:
        """Create a vertex if it doesn't already exist.

        Args:
            label: Vertex label.
            properties: Vertex properties (must include 'id').

        Returns:
            True if vertex was created, False if it already existed.
        """
        vertex_id = properties.get("id")
        if not vertex_id:
            raise ValueError("Vertex properties must include 'id'")

        if await self._vertex_exists(label, vertex_id):
            return False

        cypher = build_create_vertex(label, properties)
        await self._execute_cypher(cypher)
        return True

    async def _edge_exists(
        self,
        from_label: str,
        from_id: str,
        edge_label: str,
        to_label: str,
        to_id: str,
    ) -> bool:
        """Check if an edge already exists.

        Args:
            from_label: Source vertex label.
            from_id: Source vertex id.
            edge_label: Edge label.
            to_label: Target vertex label.
            to_id: Target vertex id.

        Returns:
            True if edge exists, False otherwise.
        """
        cypher = f"MATCH (a:{from_label} {{id: {_escape_value(from_id)}}})-[r:{edge_label}]->(b:{to_label} {{id: {_escape_value(to_id)}}}) RETURN r LIMIT 1"
        results = await self._execute_cypher(cypher)
        return len(results) > 0

    async def _create_edge_if_not_exists(
        self,
        from_label: str,
        from_id: str,
        edge_label: str,
        to_label: str,
        to_id: str,
    ) -> bool:
        """Create an edge if it doesn't already exist.

        Args:
            from_label: Source vertex label.
            from_id: Source vertex id.
            edge_label: Edge label.
            to_label: Target vertex label.
            to_id: Target vertex id.

        Returns:
            True if edge was created, False if it already existed.
        """
        if await self._edge_exists(from_label, from_id, edge_label, to_label, to_id):
            return False

        cypher = build_create_edge(from_label, from_id, edge_label, to_label, to_id)
        await self._execute_cypher(cypher)
        return True

    async def sync_domains(self) -> dict[str, int]:
        """Sync quality domains to graph vertices.

        Returns:
            Stats dict with 'created' and 'skipped' counts.
        """
        # Get distinct domains from signals table
        result = await self.session.execute(text("SELECT DISTINCT domain FROM signals WHERE domain IS NOT NULL"))
        domains = [row[0] for row in result.fetchall()]

        created = 0
        skipped = 0
        for domain in domains:
            if await self._create_vertex_if_not_exists("Domain", {"id": domain, "name": domain}):
                created += 1
            else:
                skipped += 1

        logger.info("Synced %d domains (%d created, %d skipped)", len(domains), created, skipped)
        return {"created": created, "skipped": skipped}

    async def sync_facilities(self) -> dict[str, int]:
        """Sync facilities to graph vertices.

        Returns:
            Stats dict with 'created' and 'skipped' counts.
        """
        result = await self.session.execute(text("SELECT DISTINCT facility_id FROM signals WHERE facility_id IS NOT NULL"))
        facilities = [row[0] for row in result.fetchall()]

        created = 0
        skipped = 0
        for facility_id in facilities:
            if await self._create_vertex_if_not_exists("Facility", {"id": facility_id, "name": facility_id}):
                created += 1
            else:
                skipped += 1

        logger.info(
            "Synced %d facilities (%d created, %d skipped)",
            len(facilities),
            created,
            skipped,
        )
        return {"created": created, "skipped": skipped}

    async def sync_metrics(self) -> dict[str, int]:
        """Sync metrics to graph vertices.

        Returns:
            Stats dict with 'created' and 'skipped' counts.
        """
        result = await self.session.execute(text("SELECT DISTINCT metric_id, domain FROM signals WHERE metric_id IS NOT NULL"))
        metrics = result.fetchall()

        created = 0
        skipped = 0
        for row in metrics:
            metric_id, domain = row
            if await self._create_vertex_if_not_exists("Metric", {"id": metric_id, "name": metric_id, "domain": domain}):
                created += 1
            else:
                skipped += 1

        logger.info("Synced %d metrics (%d created, %d skipped)", len(metrics), created, skipped)
        return {"created": created, "skipped": skipped}

    async def sync_signals(self) -> dict[str, int]:
        """Sync signals to graph vertices.

        Returns:
            Stats dict with 'created' and 'skipped' counts.
        """
        result = await self.session.execute(
            text("""
                SELECT id, simplified_signal_type, simplified_severity, created_at
                FROM signals
                LIMIT 1000
            """)
        )
        signals = result.fetchall()

        created = 0
        skipped = 0
        for row in signals:
            signal_id, signal_type, severity, created_at = row
            properties: dict[str, Any] = {
                "id": str(signal_id),
            }
            if signal_type:
                properties["signal_type"] = signal_type
            if severity is not None:
                properties["severity"] = severity
            if created_at:
                properties["created_at"] = created_at.isoformat()

            if await self._create_vertex_if_not_exists("Signal", properties):
                created += 1
            else:
                skipped += 1

        logger.info("Synced %d signals (%d created, %d skipped)", len(signals), created, skipped)
        return {"created": created, "skipped": skipped}

    async def sync_edges(self) -> dict[str, int]:
        """Sync relationships as graph edges.

        Creates edges:
        - Facility -> Signal (has_signal)
        - Metric -> Signal (measures)
        - Metric -> Domain (belongs_to)

        Returns:
            Stats dict with 'created' and 'skipped' counts.
        """
        created = 0
        skipped = 0

        # Get signal relationships
        result = await self.session.execute(
            text("""
                SELECT id, facility_id, metric_id, domain
                FROM signals
                WHERE facility_id IS NOT NULL AND metric_id IS NOT NULL
                LIMIT 1000
            """)
        )
        signals = result.fetchall()

        for row in signals:
            signal_id, facility_id, metric_id, domain = row

            # Facility -> Signal
            if await self._create_edge_if_not_exists("Facility", facility_id, "has_signal", "Signal", str(signal_id)):
                created += 1
            else:
                skipped += 1

            # Metric -> Signal
            if await self._create_edge_if_not_exists("Metric", metric_id, "measures", "Signal", str(signal_id)):
                created += 1
            else:
                skipped += 1

            # Metric -> Domain
            if domain:
                if await self._create_edge_if_not_exists("Metric", metric_id, "belongs_to", "Domain", domain):
                    created += 1
                else:
                    skipped += 1

        logger.info("Synced edges (%d created, %d skipped)", created, skipped)
        return {"created": created, "skipped": skipped}

    async def sync_all(self) -> dict[str, dict[str, int]]:
        """Sync all entities and relationships to the graph.

        Returns:
            Stats dict with counts for each entity type.
        """
        logger.info("Starting graph sync...")

        stats = {
            "domains": await self.sync_domains(),
            "facilities": await self.sync_facilities(),
            "metrics": await self.sync_metrics(),
            "signals": await self.sync_signals(),
            "edges": await self.sync_edges(),
        }

        await self.session.commit()
        logger.info("Graph sync complete: %s", stats)
        return stats
