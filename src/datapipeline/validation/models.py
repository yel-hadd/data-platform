"""Canonical Pydantic v2 model for a normalised order record."""

from datetime import UTC, date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class OrderRecord(BaseModel):
    """Single normalised order that every ingestion source maps to."""

    model_config = ConfigDict(str_strip_whitespace=True)

    order_id: str = Field(min_length=1)
    customer_name: str = Field(min_length=1)
    country: str = Field(min_length=2, max_length=100)
    product: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: Decimal = Field(ge=Decimal("0"))
    order_date: date
    source_type: str = Field(min_length=1)
    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
