"""Tests for the XML loader."""

from pathlib import Path

import pytest

from datapipeline.exceptions import IngestionError
from datapipeline.ingestion.xml_loader import XMLLoader

FIXTURES = Path(__file__).parent / "fixtures"


def test_xml_returns_records():
    """sample.xml with 2 <order> elements yields exactly 2 records."""
    loader = XMLLoader()
    records = list(loader.load(FIXTURES / "sample.xml"))
    assert len(records) == 2


def test_xml_record_fields():
    """Each XML record contains all expected field keys."""
    loader = XMLLoader()
    record = list(loader.load(FIXTURES / "sample.xml"))[0]
    assert record["order_id"] == "ORD-X001"
    assert record["customer_name"] == "Alice Martin"
    assert record["country"] == "FR"


def test_xml_missing_field_yields_none(tmp_path):
    """An <order> missing a child element yields None for that key."""
    f = tmp_path / "partial.xml"
    f.write_text("<orders><order><order_id>X</order_id><product/></order></orders>")
    loader = XMLLoader()
    records = list(loader.load(f))
    assert records[0]["order_id"] == "X"
    # <product/> has no text content → None
    assert records[0]["product"] is None


def test_xml_invalid_raises(tmp_path):
    """Malformed XML raises IngestionError."""
    f = tmp_path / "bad.xml"
    f.write_text("<orders><order><unclosed></orders>")
    with pytest.raises(IngestionError, match="Malformed XML"):
        list(XMLLoader().load(f))
