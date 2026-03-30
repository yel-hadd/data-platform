"""Tests for async storage operations (insert_orders, insert_error)."""

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from datapipeline.storage import insert_error, insert_orders
from datapipeline.storage.orm_models import IngestionError, Order


async def test_insert_orders_returns_count(async_session, sample_orders):
    """insert_orders returns the number of rows successfully inserted."""
    count = await insert_orders(async_session, sample_orders)
    assert count == 10


async def test_insert_orders_persists_to_db(async_session, sample_orders):
    """Inserted records are retrievable from the database."""
    await insert_orders(async_session, sample_orders)
    result = await async_session.execute(select(Order))
    rows = result.scalars().all()
    assert len(rows) == 10
    assert rows[0].order_id == "ORD-TEST-000"


async def test_insert_orders_empty_list(async_session):
    """Inserting an empty list returns 0 and does not error."""
    count = await insert_orders(async_session, [])
    assert count == 0


async def test_duplicate_order_id_raises(async_session, sample_orders):
    """Inserting two records with the same order_id raises an integrity error."""
    await insert_orders(async_session, [sample_orders[0]])
    with pytest.raises(Exception):  # IntegrityError or StorageError
        await insert_orders(async_session, [sample_orders[0]])


async def test_insert_error_persists(async_session):
    """insert_error stores a row-level validation failure in ingestion_errors."""
    await insert_error(
        async_session,
        source_file="orders.csv",
        row={"order_id": "", "quantity": "-5"},
        reason="quantity must be positive",
    )
    result = await async_session.execute(select(IngestionError))
    errors = result.scalars().all()
    assert len(errors) == 1
    assert errors[0].source_file == "orders.csv"
    assert "quantity must be positive" in errors[0].reason
