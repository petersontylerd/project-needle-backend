"""Test that ontology router is properly registered."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_ontology_vertices_endpoint_registered(client: AsyncClient) -> None:
    """Verify /api/ontology/vertices/{label} endpoint is accessible."""
    # Note: We expect 400/503 since no valid label/DB, but route should exist
    response = await client.get("/api/ontology/vertices/Metric")
    # 404 means route not registered, other codes mean route exists
    assert response.status_code != 404, "Ontology router not registered"


@pytest.mark.asyncio
async def test_ontology_vertex_detail_endpoint_registered(client: AsyncClient) -> None:
    """Verify /api/ontology/vertices/{label}/{vertex_id} endpoint is accessible."""
    response = await client.get("/api/ontology/vertices/Metric/test-id")
    assert response.status_code != 404, "Ontology router not registered"


@pytest.mark.asyncio
async def test_ontology_neighbors_endpoint_registered(client: AsyncClient) -> None:
    """Verify /api/ontology/vertices/{label}/{vertex_id}/neighbors endpoint is accessible."""
    response = await client.get("/api/ontology/vertices/Metric/test-id/neighbors")
    assert response.status_code != 404, "Ontology router not registered"


@pytest.mark.asyncio
async def test_ontology_labels_endpoint_registered(client: AsyncClient) -> None:
    """Verify /api/ontology/labels endpoint is accessible."""
    response = await client.get("/api/ontology/labels")
    # This should return 200 with list of labels (no DB required)
    assert response.status_code == 200, "Ontology labels endpoint not registered"
