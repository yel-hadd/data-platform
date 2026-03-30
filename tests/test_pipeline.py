"""Near-integration tests for the async pipeline orchestrator."""

import shutil
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from datapipeline.pipeline.orchestrator import run_pipeline
from datapipeline.storage.orm_models import IngestionError, Order

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def session_factory(async_engine):
    """async_sessionmaker bound to the in-memory test engine."""
    return async_sessionmaker(async_engine, expire_on_commit=False)


async def test_pipeline_processes_csv_file(tmp_path, session_factory, async_session):
    """Pipeline correctly ingests a CSV fixture and persists rows to the DB."""
    shutil.copy(FIXTURES / "sample.csv", tmp_path / "sample.csv")
    result = await run_pipeline(tmp_path, session_factory)

    assert result.files_processed == 1
    assert result.total_valid == 3
    assert result.total_invalid == 0

    rows = (await async_session.execute(select(Order))).scalars().all()
    assert len(rows) == 3


async def test_pipeline_processes_multiple_formats(tmp_path, session_factory):
    """Pipeline handles CSV, JSON, and XML fixtures in a single concurrent run."""
    shutil.copy(FIXTURES / "sample.csv",  tmp_path / "orders.csv")
    shutil.copy(FIXTURES / "sample.json", tmp_path / "events.json")
    shutil.copy(FIXTURES / "sample.xml",  tmp_path / "legacy.xml")

    result = await run_pipeline(tmp_path, session_factory)

    assert result.files_processed == 3
    assert result.total_valid > 0


async def test_pipeline_isolates_invalid_rows(tmp_path, session_factory, async_session):
    """Rows that fail validation land in ingestion_errors, not orders."""
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "order_id,customer_name,country,product,quantity,price,order_date\n"
        ",Empty ID,FR,Widget,-1,9.99,2024-01-01\n"    # invalid: empty id + negative qty
        "ORD-OK,Alice,FR,Widget,1,9.99,2024-01-01\n"  # valid
    )

    result = await run_pipeline(tmp_path, session_factory)

    assert result.total_valid == 1
    assert result.total_invalid == 1

    errors = (await async_session.execute(select(IngestionError))).scalars().all()
    assert len(errors) == 1


async def test_pipeline_empty_directory(tmp_path, session_factory):
    """An empty directory produces a result with zero files processed."""
    result = await run_pipeline(tmp_path, session_factory)
    assert result.files_processed == 0
    assert result.total_valid == 0
