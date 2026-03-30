"""Shared pytest fixtures: in-memory async DB engine, session, and sample data."""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from datapipeline.storage.orm_models import Base
from datapipeline.validation.models import OrderRecord

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest_asyncio.fixture()
async def async_engine():
    """In-memory SQLite async engine; creates all tables before each test."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture()
async def async_session(async_engine):
    """Async DB session bound to the in-memory engine."""
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture()
def sample_orders() -> list[OrderRecord]:
    """Ten pre-built valid OrderRecord instances for use in DB and API tests."""
    countries = ["FR", "DE", "US", "GB", "ES"]
    products = ["Widget", "Gadget"]
    records = []
    for i in range(10):
        records.append(
            OrderRecord(
                order_id=f"ORD-TEST-{i:03d}",
                customer_name=f"Customer {i}",
                country=countries[i % len(countries)],
                product=products[i % len(products)],
                quantity=i + 1,
                price=Decimal("9.99") if i % 2 == 0 else Decimal("24.50"),
                order_date=date(2024, 1, i + 1),
                source_type="csv",
            )
        )
    return records
