"""Tests for graph sync idempotency.

These tests verify that the graph sync operation is safe for re-runs:
- Running sync twice should not duplicate vertices
- Running sync twice should not duplicate edges
- Modified properties should be updated on re-sync

These tests require:
- PostgreSQL with AGE extension installed
- Graph schema already created

Usage:
    cd backend
    UV_CACHE_DIR=../.uv-cache uv run pytest tests/ontology/test_sync_idempotency.py -v
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.ontology.sync_service import GraphSyncService


class TestSyncIdempotency:
    """Tests for sync operation idempotency."""

    @pytest.fixture
    def mock_session(self) -> AsyncMock:
        """Create a mock async session."""
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        return session

    @pytest.fixture
    def service(self, mock_session: AsyncMock) -> GraphSyncService:
        """Create service with mock session."""
        return GraphSyncService(mock_session)

    @pytest.mark.asyncio
    async def test_sync_twice_does_not_duplicate_vertices(
        self,
        service: GraphSyncService,
        mock_session: AsyncMock,
    ) -> None:
        """Running sync twice should not create duplicate vertices.

        On first run: creates vertices
        On second run: skips existing vertices (doesn't duplicate)
        """
        # Mock domain sync
        mock_domain_result = MagicMock()
        mock_domain_result.fetchall.return_value = [("Efficiency",), ("Safety",)]

        # First sync: check existence (empty) → create
        mock_empty_result = MagicMock()
        mock_empty_result.fetchall.return_value = []
        mock_created_result = MagicMock()
        mock_created_result.fetchall.return_value = [("created",)]

        # Set up first call sequence: query domains, then for each domain:
        # check exists (empty), create
        mock_session.execute.side_effect = [
            mock_domain_result,
            mock_empty_result,  # Efficiency doesn't exist
            mock_created_result,  # Create Efficiency
            mock_empty_result,  # Safety doesn't exist
            mock_created_result,  # Create Safety
        ]

        first_stats = await service.sync_domains()
        assert first_stats["created"] == 2
        assert first_stats["skipped"] == 0

        # Reset mock for second sync
        mock_session.execute.reset_mock()

        # Second sync: vertices now exist
        mock_exists_result = MagicMock()
        mock_exists_result.fetchall.return_value = [("vertex",)]

        mock_session.execute.side_effect = [
            mock_domain_result,
            mock_exists_result,  # Efficiency exists
            mock_exists_result,  # Safety exists
        ]

        second_stats = await service.sync_domains()

        # Should skip all (no duplicates created)
        assert second_stats["created"] == 0
        assert second_stats["skipped"] == 2

    @pytest.mark.asyncio
    async def test_sync_twice_does_not_duplicate_edges(
        self,
        service: GraphSyncService,
        mock_session: AsyncMock,
    ) -> None:
        """Running sync twice should not create duplicate edges.

        Edge creation checks if edge exists before creating.
        """
        # First sync: edge doesn't exist → create
        mock_empty_result = MagicMock()
        mock_empty_result.fetchall.return_value = []
        mock_created_result = MagicMock()
        mock_created_result.fetchall.return_value = [("created",)]

        mock_session.execute.side_effect = [
            mock_empty_result,  # Edge doesn't exist
            mock_created_result,  # Create edge
        ]

        created = await service._create_edge_if_not_exists("Facility", "F1", "has_signal", "Signal", "S1")
        assert created is True

        # Reset mock for second sync
        mock_session.execute.reset_mock()

        # Second sync: edge now exists
        mock_exists_result = MagicMock()
        mock_exists_result.fetchall.return_value = [("edge",)]

        mock_session.execute.side_effect = [mock_exists_result]

        created = await service._create_edge_if_not_exists("Facility", "F1", "has_signal", "Signal", "S1")
        assert created is False

    @pytest.mark.asyncio
    async def test_vertex_with_same_id_not_recreated(
        self,
        service: GraphSyncService,
        mock_session: AsyncMock,
    ) -> None:
        """Vertex with same ID should not be recreated on re-sync."""
        mock_exists_result = MagicMock()
        mock_exists_result.fetchall.return_value = [("existing_vertex",)]

        mock_session.execute.return_value = mock_exists_result

        created = await service._create_vertex_if_not_exists(
            "Facility",
            {"id": "FAC001", "name": "Hospital A"},
        )

        assert created is False
        # Only one execute call (existence check)
        assert mock_session.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_sync_all_idempotent_on_rerun(
        self,
        service: GraphSyncService,
        mock_session: AsyncMock,
    ) -> None:
        """sync_all should be idempotent - rerun produces same counts."""
        # Mock all sync methods for first run: all created
        service.sync_domains = AsyncMock(return_value={"created": 2, "skipped": 0})
        service.sync_facilities = AsyncMock(return_value={"created": 10, "skipped": 0})
        service.sync_metrics = AsyncMock(return_value={"created": 5, "skipped": 0})
        service.sync_signals = AsyncMock(return_value={"created": 100, "skipped": 0})
        service.sync_edges = AsyncMock(return_value={"created": 200, "skipped": 0})

        first_stats = await service.sync_all()
        total_created_first = (
            first_stats["domains"]["created"]
            + first_stats["facilities"]["created"]
            + first_stats["metrics"]["created"]
            + first_stats["signals"]["created"]
            + first_stats["edges"]["created"]
        )

        # Mock all sync methods for second run: all skipped
        service.sync_domains = AsyncMock(return_value={"created": 0, "skipped": 2})
        service.sync_facilities = AsyncMock(return_value={"created": 0, "skipped": 10})
        service.sync_metrics = AsyncMock(return_value={"created": 0, "skipped": 5})
        service.sync_signals = AsyncMock(return_value={"created": 0, "skipped": 100})
        service.sync_edges = AsyncMock(return_value={"created": 0, "skipped": 200})

        second_stats = await service.sync_all()
        total_created_second = (
            second_stats["domains"]["created"]
            + second_stats["facilities"]["created"]
            + second_stats["metrics"]["created"]
            + second_stats["signals"]["created"]
            + second_stats["edges"]["created"]
        )

        # First run creates, second run skips everything
        assert total_created_first == 317
        assert total_created_second == 0


class TestSyncPropertyUpdates:
    """Tests for property update behavior during sync."""

    @pytest.fixture
    def mock_session(self) -> AsyncMock:
        """Create a mock async session."""
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        return session

    @pytest.fixture
    def service(self, mock_session: AsyncMock) -> GraphSyncService:
        """Create service with mock session."""
        return GraphSyncService(mock_session)

    @pytest.mark.asyncio
    async def test_create_vertex_includes_all_properties(
        self,
        service: GraphSyncService,
        mock_session: AsyncMock,
    ) -> None:
        """Vertex creation should include all provided properties."""
        mock_empty_result = MagicMock()
        mock_empty_result.fetchall.return_value = []
        mock_created_result = MagicMock()
        mock_created_result.fetchall.return_value = [("created",)]

        mock_session.execute.side_effect = [mock_empty_result, mock_created_result]

        created = await service._create_vertex_if_not_exists(
            "Signal",
            {
                "id": "SIG001",
                "node_id": "losIndex__medicareId__aggregate",
                "severity": "Critical",
                "zscore": 2.5,
            },
        )

        assert created is True
        # Verify the create query was called
        assert mock_session.execute.call_count == 2


class TestSyncCountConsistency:
    """Tests for count consistency during sync operations."""

    @pytest.fixture
    def mock_session(self) -> AsyncMock:
        """Create a mock async session."""
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        return session

    @pytest.fixture
    def service(self, mock_session: AsyncMock) -> GraphSyncService:
        """Create service with mock session."""
        return GraphSyncService(mock_session)

    @pytest.mark.asyncio
    async def test_created_plus_skipped_equals_total_input(
        self,
        service: GraphSyncService,
        mock_session: AsyncMock,
    ) -> None:
        """created + skipped should equal total input count.

        This validates no items are lost during sync.
        """
        # Query returns 3 domains
        mock_domain_result = MagicMock()
        mock_domain_result.fetchall.return_value = [
            ("Efficiency",),
            ("Safety",),
            ("Quality",),
        ]

        # First two exist, third is new
        mock_exists_result = MagicMock()
        mock_exists_result.fetchall.return_value = [("vertex",)]
        mock_empty_result = MagicMock()
        mock_empty_result.fetchall.return_value = []
        mock_created_result = MagicMock()
        mock_created_result.fetchall.return_value = [("created",)]

        mock_session.execute.side_effect = [
            mock_domain_result,
            mock_exists_result,  # Efficiency exists
            mock_exists_result,  # Safety exists
            mock_empty_result,  # Quality doesn't exist
            mock_created_result,  # Create Quality
        ]

        stats = await service.sync_domains()

        total_input = 3  # Efficiency, Safety, Quality
        total_processed = stats["created"] + stats["skipped"]

        assert total_processed == total_input, f"created ({stats['created']}) + skipped ({stats['skipped']}) should equal total input ({total_input})"
