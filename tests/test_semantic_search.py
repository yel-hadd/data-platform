"""Tests for the semantic-search API endpoint.

All OpenAI embedding calls are mocked — no real API key required.
pgvector cosine distance (<=>) is not supported in SQLite, so the
endpoint is tested with a stubbed semantic_search() function.
"""

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker

from datapipeline.api.deps import get_session
from datapipeline.api.main import app
from datapipeline.storage.orm_models import DocumentChunk


@pytest_asyncio.fixture()
async def search_client(async_engine):
    """AsyncClient backed by an in-memory DB that has one DocumentChunk row."""
    factory = async_sessionmaker(async_engine, expire_on_commit=False)

    # Seed a DocumentChunk row.
    async with factory() as session:
        session.add(
            DocumentChunk(
                source_file="unstructured_orders.txt",
                content="Alice ordered 2 Widgets on 2024-01-15.",
                embedding=[0.1] * 1536,
            )
        )
        await session.commit()

    async def _override_session():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session

    with patch("datapipeline.api.main.init_db", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client

    app.dependency_overrides.clear()


async def test_semantic_search_returns_results(search_client):
    """GET /metrics/semantic-search with a mocked embedding returns SemanticResult list."""
    fake_results = [
        {
            "source_file": "unstructured_orders.txt",
            "content":     "Alice ordered 2 Widgets on 2024-01-15.",
            "similarity":  0.92,
        }
    ]

    with (
        patch(
            "datapipeline.api.routes.metrics.get_query_embedding",
            new_callable=AsyncMock,
            return_value=[0.1] * 1536,
        ),
        patch(
            "datapipeline.api.routes.metrics.semantic_search",
            new_callable=AsyncMock,
            return_value=fake_results,
        ),
    ):
        resp = await search_client.get("/metrics/semantic-search?q=widgets")

    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["source_file"] == "unstructured_orders.txt"
    assert "content" in data[0]
    assert "similarity" in data[0]


async def test_semantic_search_empty_db_returns_empty(search_client):
    """semantic-search returns an empty list when no matching chunks exist."""
    with (
        patch(
            "datapipeline.api.routes.metrics.get_query_embedding",
            new_callable=AsyncMock,
            return_value=[0.0] * 1536,
        ),
        patch(
            "datapipeline.api.routes.metrics.semantic_search",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        resp = await search_client.get("/metrics/semantic-search?q=nothing")

    assert resp.status_code == 200
    assert resp.json() == []


async def test_semantic_search_missing_api_key_returns_503(search_client):
    """semantic-search returns 503 when OPENAI_API_KEY is not set."""
    from datapipeline.exceptions import IngestionError

    with patch(
        "datapipeline.api.routes.metrics.get_query_embedding",
        new_callable=AsyncMock,
        side_effect=IngestionError("OPENAI_API_KEY is not set"),
    ):
        resp = await search_client.get("/metrics/semantic-search?q=test")

    assert resp.status_code == 503
    assert "OPENAI_API_KEY" in resp.json()["detail"]


async def test_semantic_search_missing_query_returns_422(search_client):
    """semantic-search returns 422 when the required 'q' parameter is missing."""
    resp = await search_client.get("/metrics/semantic-search")
    assert resp.status_code == 422


async def test_semantic_search_limit_param(search_client):
    """?limit param is forwarded to semantic_search."""
    with (
        patch(
            "datapipeline.api.routes.metrics.get_query_embedding",
            new_callable=AsyncMock,
            return_value=[0.1] * 1536,
        ),
        patch(
            "datapipeline.api.routes.metrics.semantic_search",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_search,
    ):
        await search_client.get("/metrics/semantic-search?q=test&limit=3")
        _, kwargs = mock_search.call_args
        assert kwargs.get("limit") == 3 or mock_search.call_args[0][2] == 3
