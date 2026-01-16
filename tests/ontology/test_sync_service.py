"""Tests for graph sync service."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.ontology.sync_service import GraphSyncService


class TestGraphSyncService:
    """Tests for GraphSyncService class."""

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

    def test_init_stores_session(self, mock_session: AsyncMock) -> None:
        """Service should store the session."""
        service = GraphSyncService(mock_session)
        assert service.session is mock_session

    @pytest.mark.asyncio
    async def test_vertex_exists_returns_true_when_found(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """Should return True when vertex exists."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("vertex",)]
        mock_session.execute.return_value = mock_result

        exists = await service._vertex_exists("Facility", "F1")
        assert exists is True

    @pytest.mark.asyncio
    async def test_vertex_exists_returns_false_when_not_found(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """Should return False when vertex doesn't exist."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        exists = await service._vertex_exists("Facility", "F1")
        assert exists is False

    @pytest.mark.asyncio
    async def test_create_vertex_if_not_exists_requires_id(self, service: GraphSyncService) -> None:
        """Should raise ValueError when properties missing id."""
        with pytest.raises(ValueError, match="must include 'id'"):
            await service._create_vertex_if_not_exists("Facility", {"name": "Test"})

    @pytest.mark.asyncio
    async def test_create_vertex_if_not_exists_creates_when_new(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """Should create vertex when it doesn't exist."""
        # First call checks existence (returns empty = doesn't exist)
        # Second call creates the vertex
        mock_result_empty = MagicMock()
        mock_result_empty.fetchall.return_value = []
        mock_result_created = MagicMock()
        mock_result_created.fetchall.return_value = [("created",)]
        mock_session.execute.side_effect = [mock_result_empty, mock_result_created]

        created = await service._create_vertex_if_not_exists("Facility", {"id": "F1", "name": "Test"})

        assert created is True
        assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_create_vertex_if_not_exists_skips_when_exists(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """Should skip creation when vertex already exists."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("vertex",)]
        mock_session.execute.return_value = mock_result

        created = await service._create_vertex_if_not_exists("Facility", {"id": "F1", "name": "Test"})

        assert created is False
        # Only one call to check existence
        assert mock_session.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_edge_exists_returns_true_when_found(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """Should return True when edge exists."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("edge",)]
        mock_session.execute.return_value = mock_result

        exists = await service._edge_exists("Facility", "F1", "has_signal", "Signal", "S1")
        assert exists is True

    @pytest.mark.asyncio
    async def test_edge_exists_returns_false_when_not_found(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """Should return False when edge doesn't exist."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        exists = await service._edge_exists("Facility", "F1", "has_signal", "Signal", "S1")
        assert exists is False

    @pytest.mark.asyncio
    async def test_sync_all_calls_all_sync_methods(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """sync_all should call all individual sync methods."""
        # Mock all sync methods
        service.sync_domains = AsyncMock(return_value={"created": 1, "skipped": 0})
        service.sync_facilities = AsyncMock(return_value={"created": 2, "skipped": 0})
        service.sync_metrics = AsyncMock(return_value={"created": 3, "skipped": 0})
        service.sync_signals = AsyncMock(return_value={"created": 4, "skipped": 0})
        service.sync_edges = AsyncMock(return_value={"created": 5, "skipped": 0})

        stats = await service.sync_all()

        service.sync_domains.assert_called_once()
        service.sync_facilities.assert_called_once()
        service.sync_metrics.assert_called_once()
        service.sync_signals.assert_called_once()
        service.sync_edges.assert_called_once()
        mock_session.commit.assert_called_once()

        assert stats["domains"]["created"] == 1
        assert stats["facilities"]["created"] == 2
        assert stats["metrics"]["created"] == 3
        assert stats["signals"]["created"] == 4
        assert stats["edges"]["created"] == 5

    @pytest.mark.asyncio
    async def test_sync_domains_queries_distinct_domains(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """sync_domains should query distinct domains from signals table."""
        # First call: query domains
        mock_domain_result = MagicMock()
        mock_domain_result.fetchall.return_value = [("Efficiency",), ("Safety",)]

        # Subsequent calls: check existence and create
        mock_empty_result = MagicMock()
        mock_empty_result.fetchall.return_value = []
        mock_created_result = MagicMock()
        mock_created_result.fetchall.return_value = [("created",)]

        mock_session.execute.side_effect = [
            mock_domain_result,
            mock_empty_result,
            mock_created_result,
            mock_empty_result,
            mock_created_result,
        ]

        stats = await service.sync_domains()

        assert stats["created"] == 2
        assert stats["skipped"] == 0

    @pytest.mark.asyncio
    async def test_sync_facilities_queries_distinct_facilities(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """sync_facilities should query distinct facility_ids from signals table."""
        # First call: query facilities
        mock_facility_result = MagicMock()
        mock_facility_result.fetchall.return_value = [("FAC001",)]

        # Subsequent calls: check existence and create
        mock_empty_result = MagicMock()
        mock_empty_result.fetchall.return_value = []
        mock_created_result = MagicMock()
        mock_created_result.fetchall.return_value = [("created",)]

        mock_session.execute.side_effect = [
            mock_facility_result,
            mock_empty_result,
            mock_created_result,
        ]

        stats = await service.sync_facilities()

        assert stats["created"] == 1
        assert stats["skipped"] == 0

    @pytest.mark.asyncio
    async def test_sync_returns_stats_with_created_and_skipped(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """All sync methods should return stats with created and skipped counts."""
        # Mock all sync methods to return proper stats
        service.sync_domains = AsyncMock(return_value={"created": 0, "skipped": 1})
        service.sync_facilities = AsyncMock(return_value={"created": 0, "skipped": 2})
        service.sync_metrics = AsyncMock(return_value={"created": 0, "skipped": 3})
        service.sync_signals = AsyncMock(return_value={"created": 0, "skipped": 4})
        service.sync_edges = AsyncMock(return_value={"created": 0, "skipped": 5})

        stats = await service.sync_all()

        assert stats["domains"]["skipped"] == 1
        assert stats["facilities"]["skipped"] == 2
        assert stats["metrics"]["skipped"] == 3
        assert stats["signals"]["skipped"] == 4
        assert stats["edges"]["skipped"] == 5

    @pytest.mark.asyncio
    async def test_execute_cypher_handles_database_error(self, service: GraphSyncService, mock_session: AsyncMock) -> None:
        """Should raise RuntimeError when query fails."""
        mock_session.execute.side_effect = SQLAlchemyError("connection lost")

        with pytest.raises(RuntimeError, match="Graph query failed"):
            await service._execute_cypher("MATCH (n) RETURN n")
