"""Tests for the source-to-canonical normalizer."""

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from datapipeline.exceptions import RowValidationError
from datapipeline.transform.normalize import normalize
from datapipeline.validation.models import OrderRecord

CSV_RAW = {
    "order_id": "ORD-001",
    "customer_name": "Alice",
    "country": "FR",
    "product": "Widget",
    "quantity": "2",
    "price": "9.99",
    "order_date": "2024-01-15",
}

JSON_RAW = {
    "orderId": "ORD-J001",
    "customer": "Bob",
    "country": "DE",
    "product": "Gadget",
    "quantity": "1",
    "price": "24.50",
    "order_date": "2024-01-16",
}

EXCEL_RAW = {
    "Order ID": "ORD-E001",
    "Customer": "Carol",
    "country": "US",
    "product": "Widget",
    "quantity": "5",
    "price": "9.99",
    "order_date": "2024-01-17",
}

XML_RAW = {
    "order_id": "ORD-X001",
    "customer_name": "Dave",
    "country": "GB",
    "product": "Gadget",
    "quantity": "3",
    "price": "24.50",
    "order_date": "2024-01-18",
}


def test_normalize_csv_raw():
    """CSV raw dict normalizes to a valid OrderRecord."""
    canonical = normalize(CSV_RAW, source_type="csv")
    record = OrderRecord.model_validate(canonical)
    assert record.order_id == "ORD-001"
    assert record.source_type == "csv"


def test_normalize_json_raw():
    """JSON raw dict (using orderId / customer keys) normalizes correctly."""
    canonical = normalize(JSON_RAW, source_type="json")
    record = OrderRecord.model_validate(canonical)
    assert record.order_id == "ORD-J001"
    assert record.customer_name == "Bob"


def test_normalize_excel_raw():
    """Excel raw dict (using 'Order ID' / 'Customer' headers) normalizes correctly."""
    canonical = normalize(EXCEL_RAW, source_type="excel")
    record = OrderRecord.model_validate(canonical)
    assert record.order_id == "ORD-E001"
    assert record.customer_name == "Carol"


def test_normalize_xml_raw():
    """XML raw dict normalizes correctly."""
    canonical = normalize(XML_RAW, source_type="xml")
    record = OrderRecord.model_validate(canonical)
    assert record.order_id == "ORD-X001"


def test_normalize_sets_source_type():
    """Normalized dict always includes source_type."""
    canonical = normalize(CSV_RAW, source_type="csv")
    assert canonical["source_type"] == "csv"


def test_normalize_unknown_source_type_raises():
    """An unregistered source_type raises RowValidationError."""
    with pytest.raises(RowValidationError, match="Unknown source_type"):
        normalize(CSV_RAW, source_type="parquet")


def test_normalize_missing_required_field_fails_validation():
    """If a required canonical field is absent, model_validate raises ValidationError."""
    raw = {"order_id": "X"}  # missing most required fields
    canonical = normalize(raw, source_type="csv")
    with pytest.raises(ValidationError):
        OrderRecord.model_validate(canonical)
