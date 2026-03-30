"""Tests for the Excel loader."""

from pathlib import Path

import pandas as pd
import pytest

from datapipeline.ingestion.excel_loader import ExcelLoader


@pytest.fixture()
def sample_xlsx(tmp_path) -> Path:
    """Create a minimal .xlsx fixture with 3 order rows."""
    df = pd.DataFrame(
        {
            "Order ID": ["ORD-E001", "ORD-E002", "ORD-E003"],
            "Customer": ["Alice", "Bob", "Carol"],
            "country": ["FR", "DE", "US"],
            "product": ["Widget", "Gadget", "Widget"],
            "quantity": ["2", "1", "5"],
            "price": ["9.99", "24.50", "9.99"],
            "order_date": ["2024-01-15", "2024-01-16", "2024-01-17"],
        }
    )
    path = tmp_path / "sample.xlsx"
    df.to_excel(path, index=False)
    return path


def test_excel_returns_records(sample_xlsx):
    """Excel file with 3 data rows yields exactly 3 records."""
    loader = ExcelLoader()
    records = list(loader.load(sample_xlsx))
    assert len(records) == 3


def test_excel_record_fields(sample_xlsx):
    """Records contain the column names from the header row."""
    loader = ExcelLoader()
    record = list(loader.load(sample_xlsx))[0]
    assert record["Order ID"] == "ORD-E001"
    assert record["Customer"] == "Alice"


def test_excel_empty_cell_is_none(tmp_path):
    """Empty Excel cells are returned as None."""
    df = pd.DataFrame({"a": ["1"], "b": [None], "c": ["3"]})
    path = tmp_path / "sparse.xlsx"
    df.to_excel(path, index=False)
    loader = ExcelLoader()
    records = list(loader.load(path))
    assert records[0]["b"] is None
