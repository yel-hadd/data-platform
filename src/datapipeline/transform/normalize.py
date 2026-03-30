"""Normalizer: maps source-specific raw dicts to the canonical OrderRecord schema."""

from ..exceptions import RowValidationError
from ..ingestion.base import RawRecord
from ..validation.models import OrderRecord

# Per-source field alias maps: {source_field_name: canonical_field_name}.
# Each source may use different column names for the same business concept.
_FIELD_MAP: dict[str, dict[str, str]] = {
    "csv": {
        "order_id":      "order_id",
        "customer_name": "customer_name",
        "country":       "country",
        "product":       "product",
        "quantity":      "quantity",
        "price":         "price",
        "order_date":    "order_date",
    },
    "json": {
        "orderId":    "order_id",
        "customer":   "customer_name",
        "country":    "country",
        "product":    "product",
        "quantity":   "quantity",
        "price":      "price",
        "order_date": "order_date",
    },
    "ndjson": {
        "orderId":    "order_id",
        "customer":   "customer_name",
        "country":    "country",
        "product":    "product",
        "quantity":   "quantity",
        "price":      "price",
        "order_date": "order_date",
    },
    "excel": {
        "Order ID":  "order_id",
        "Customer":  "customer_name",
        "country":   "country",
        "product":   "product",
        "quantity":  "quantity",
        "price":     "price",
        "order_date":"order_date",
    },
    "xml": {
        "order_id":      "order_id",
        "customer_name": "customer_name",
        "country":       "country",
        "product":       "product",
        "quantity":      "quantity",
        "price":         "price",
        "order_date":    "order_date",
    },
    # ZIP extracts its inner files which then map through their own source types.
    "zip": {
        "order_id":      "order_id",
        "customer_name": "customer_name",
        "country":       "country",
        "product":       "product",
        "quantity":      "quantity",
        "price":         "price",
        "order_date":    "order_date",
    },
}


def normalize(raw: RawRecord, source_type: str) -> dict:
    """Remap *raw* fields to canonical names and return an OrderRecord-ready dict.

    Args:
        raw:         Raw record from a loader.
        source_type: Loader format key used to look up the field alias map.

    Returns:
        Dict with canonical field names, ready for OrderRecord.model_validate().

    Raises:
        RowValidationError: If *source_type* has no registered field map.
    """
    field_map = _FIELD_MAP.get(source_type)
    if field_map is None:
        raise RowValidationError(raw, f"Unknown source_type: {source_type!r}")

    canonical: dict = {"source_type": source_type}
    for src_key, canon_key in field_map.items():
        value = raw.get(src_key)
        if value is not None:
            canonical[canon_key] = value

    return canonical
