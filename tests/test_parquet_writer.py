"""Tests for the Parquet export writer."""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from datapipeline.storage.parquet_writer import write_parquet
from datapipeline.validation.models import OrderRecord


def _make_order(order_id: str, country: str) -> OrderRecord:
    return OrderRecord(
        order_id=order_id,
        customer_name="Test User",
        country=country,
        product="Widget",
        quantity=1,
        price=Decimal("9.99"),
        order_date=date(2024, 1, 1),
        source_type="csv",
    )


def test_parquet_roundtrip(tmp_path):
    """Five records written to Parquet can be read back with all fields present."""
    records = [_make_order(f"ORD-{i}", "FR") for i in range(5)]
    write_parquet(records, tmp_path / "out")
    dataset = pq.read_table(tmp_path / "out")
    assert dataset.num_rows == 5
    ids = dataset.column("order_id").to_pylist()
    assert sorted(ids) == [f"ORD-{i}" for i in range(5)]


def test_parquet_partitioned_by_country(tmp_path):
    """Records with two distinct countries produce two partition subdirectories."""
    records = [
        _make_order("ORD-FR-1", "FR"),
        _make_order("ORD-FR-2", "FR"),
        _make_order("ORD-DE-1", "DE"),
    ]
    out = tmp_path / "partitioned"
    write_parquet(records, out, partition_col="country")

    subdirs = {p.name for p in out.iterdir() if p.is_dir()}
    assert "country=FR" in subdirs
    assert "country=DE" in subdirs


def test_parquet_empty_records_skips_write(tmp_path):
    """Calling write_parquet with an empty list does not create any files."""
    out = tmp_path / "empty"
    write_parquet([], out)
    assert not out.exists()


def test_parquet_creates_output_dir(tmp_path):
    """write_parquet creates the output directory if it does not exist."""
    out = tmp_path / "new" / "nested" / "dir"
    write_parquet([_make_order("ORD-1", "US")], out)
    assert out.exists()
