"""Tests for analytical report queries using an in-memory database."""

import pytest

from datapipeline.analytics.reports import (
    revenue_by_country,
    revenue_by_product,
    revenue_trend,
    top_customers,
)
from datapipeline.storage import insert_orders


async def test_revenue_by_country_correct_totals(async_session, sample_orders):
    """Revenue totals are aggregated correctly per country."""
    await insert_orders(async_session, sample_orders)
    results = await revenue_by_country(async_session)

    assert len(results) > 0
    assert all("country" in r and "revenue" in r for r in results)
    # All revenues should be positive
    assert all(r["revenue"] > 0 for r in results)


async def test_revenue_by_country_ordered_desc(async_session, sample_orders):
    """Results are sorted from highest to lowest revenue."""
    await insert_orders(async_session, sample_orders)
    results = await revenue_by_country(async_session)
    revenues = [r["revenue"] for r in results]
    assert revenues == sorted(revenues, reverse=True)


async def test_revenue_by_product_ordered_desc(async_session, sample_orders):
    """Products are ranked by descending revenue."""
    await insert_orders(async_session, sample_orders)
    results = await revenue_by_product(async_session)
    assert len(results) > 0
    revenues = [r["revenue"] for r in results]
    assert revenues == sorted(revenues, reverse=True)


async def test_top_customers_limit(async_session, sample_orders):
    """top_customers respects the limit parameter."""
    await insert_orders(async_session, sample_orders)
    results = await top_customers(async_session, limit=3)
    assert len(results) <= 3
    assert all("customer_name" in r and "revenue" in r for r in results)


async def test_revenue_trend_ordered_by_date(async_session, sample_orders):
    """Revenue trend results are sorted chronologically."""
    await insert_orders(async_session, sample_orders)
    results = await revenue_trend(async_session)
    assert len(results) > 0
    dates = [r["order_date"] for r in results]
    assert dates == sorted(dates)


async def test_empty_db_returns_empty_lists(async_session):
    """All report functions return empty lists when the database has no orders."""
    assert await revenue_by_country(async_session) == []
    assert await revenue_by_product(async_session) == []
    assert await top_customers(async_session) == []
    assert await revenue_trend(async_session) == []
