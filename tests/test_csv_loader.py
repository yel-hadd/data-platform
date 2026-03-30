"""Tests for the CSV loader."""

from pathlib import Path

import pytest

from datapipeline.ingestion.csv_loader import CSVLoader

FIXTURES = Path(__file__).parent / "fixtures"


def test_csv_returns_records():
    """sample.csv with 3 data rows yields exactly 3 records."""
    loader = CSVLoader()
    records = list(loader.load(FIXTURES / "sample.csv"))
    assert len(records) == 3


def test_csv_record_fields():
    """Each record contains all expected column keys."""
    loader = CSVLoader()
    record = list(loader.load(FIXTURES / "sample.csv"))[0]
    assert record["order_id"] == "ORD-001"
    assert record["customer_name"] == "Alice Martin"
    assert record["country"] == "FR"


def test_csv_handles_missing_values(tmp_path):
    """Empty cells in a CSV row are converted to None."""
    f = tmp_path / "sparse.csv"
    f.write_text("a,b,c\n1,,3\n")
    loader = CSVLoader()
    records = list(loader.load(f))
    assert records[0]["b"] is None


def test_csv_chunking(tmp_path):
    """A file with more rows than chunk_size still yields all rows."""
    rows = ["id,val"] + [f"{i},{i*2}" for i in range(25_000)]
    f = tmp_path / "large.csv"
    f.write_text("\n".join(rows))
    loader = CSVLoader()
    result = list(loader.load(f))
    assert len(result) == 25_000


def test_csv_empty_file(tmp_path):
    """A CSV with only a header row yields no records."""
    f = tmp_path / "empty.csv"
    f.write_text("order_id,name\n")
    loader = CSVLoader()
    assert list(loader.load(f)) == []
