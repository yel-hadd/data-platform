"""Tests for the canonical OrderRecord Pydantic model."""

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from datapipeline.validation.models import OrderRecord

VALID = dict(
    order_id="ORD-001",
    customer_name="Alice",
    country="FR",
    product="Widget",
    quantity=2,
    price=Decimal("9.99"),
    order_date=date(2024, 1, 15),
    source_type="csv",
)


def test_valid_order():
    """A fully specified valid order should parse without errors."""
    record = OrderRecord(**VALID)
    assert record.order_id == "ORD-001"
    assert record.quantity == 2


def test_missing_order_id():
    """Empty order_id must be rejected."""
    with pytest.raises(ValidationError):
        OrderRecord(**{**VALID, "order_id": ""})


def test_negative_quantity():
    """quantity must be strictly positive."""
    with pytest.raises(ValidationError):
        OrderRecord(**{**VALID, "quantity": -1})


def test_zero_quantity():
    """quantity=0 must also be rejected (gt=0)."""
    with pytest.raises(ValidationError):
        OrderRecord(**{**VALID, "quantity": 0})


def test_negative_price():
    """Negative price must be rejected."""
    with pytest.raises(ValidationError):
        OrderRecord(**{**VALID, "price": Decimal("-0.01")})


def test_zero_price_allowed():
    """price=0 is valid (free items)."""
    record = OrderRecord(**{**VALID, "price": Decimal("0")})
    assert record.price == Decimal("0")


def test_invalid_date():
    """Unparsable order_date string must raise ValidationError."""
    with pytest.raises(ValidationError):
        OrderRecord(**{**VALID, "order_date": "not-a-date"})


def test_whitespace_stripped():
    """Leading/trailing whitespace in string fields is stripped."""
    record = OrderRecord(**{**VALID, "customer_name": "  Alice  ", "product": " Widget "})
    assert record.customer_name == "Alice"
    assert record.product == "Widget"


def test_ingestion_timestamp_defaults():
    """ingestion_timestamp is auto-populated when not supplied."""
    record = OrderRecord(**VALID)
    assert record.ingestion_timestamp is not None
