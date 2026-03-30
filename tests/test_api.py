"""Tests for the FastAPI metrics API using an in-memory database."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker

from datapipeline.api.deps import get_session
from datapipeline.api.main import app
from datapipeline.storage import insert_orders
from datapipeline.storage.database import AsyncSessionLocal
from datapipeline.storage.orm_models import Base


@pytest_asyncio.fixture()
async def seeded_client(async_engine, sample_orders):
    """AsyncClient with session overridden to use in-memory DB, pre-seeded with data."""
    factory = async_sessionmaker(async_engine, expire_on_commit=False)

    # Seed the in-memory DB.
    async with factory() as session:
        await insert_orders(session, sample_orders)

    async def _override_session():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session

    # Skip the real init_db (which would try to reach postgres).
    with patch("datapipeline.api.main.init_db", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client

    app.dependency_overrides.clear()


@pytest_asyncio.fixture()
async def empty_client(async_engine):
    """AsyncClient backed by an empty in-memory DB."""
    factory = async_sessionmaker(async_engine, expire_on_commit=False)

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


async def test_health_returns_ok(seeded_client):
    """GET /health returns 200 with status=ok."""
    resp = await seeded_client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_revenue_by_country_schema(seeded_client):
    """GET /metrics/revenue-by-country returns a list with country and revenue keys."""
    resp = await seeded_client.get("/metrics/revenue-by-country")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list) and len(data) > 0
    assert "country" in data[0] and "revenue" in data[0]


async def test_revenue_by_product_schema(seeded_client):
    """GET /metrics/revenue-by-product returns a list with product and revenue keys."""
    resp = await seeded_client.get("/metrics/revenue-by-product")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list) and len(data) > 0
    assert "product" in data[0] and "revenue" in data[0]


async def test_top_customers_schema(seeded_client):
    """GET /metrics/top-customers returns a list with customer_name and revenue."""
    resp = await seeded_client.get("/metrics/top-customers")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list) and len(data) > 0
    assert "customer_name" in data[0] and "revenue" in data[0]


async def test_top_customers_limit_param(seeded_client):
    """?limit=2 restricts the result to at most 2 entries."""
    resp = await seeded_client.get("/metrics/top-customers?limit=2")
    assert resp.status_code == 200
    assert len(resp.json()) <= 2


async def test_revenue_trend_schema(seeded_client):
    """GET /metrics/revenue-trend returns a list with order_date and revenue."""
    resp = await seeded_client.get("/metrics/revenue-trend")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list) and len(data) > 0
    assert "order_date" in data[0] and "revenue" in data[0]


async def test_empty_db_returns_empty_lists(empty_client):
    """All metric endpoints return empty lists when the database has no rows."""
    for path in [
        "/metrics/revenue-by-country",
        "/metrics/revenue-by-product",
        "/metrics/top-customers",
        "/metrics/revenue-trend",
    ]:
        resp = await empty_client.get(path)
        assert resp.status_code == 200
        assert resp.json() == []
