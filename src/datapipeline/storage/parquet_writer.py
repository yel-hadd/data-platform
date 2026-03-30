"""Parquet export: writes normalised order records with optional partitioning."""

import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..validation.models import OrderRecord

logger = logging.getLogger(__name__)


def write_parquet(
    records: list[OrderRecord],
    output_dir: Path,
    partition_col: str = "country",
) -> None:
    """Serialise *records* to Parquet, partitioned by *partition_col*.

    Partitioning (e.g. by country) creates subdirectories such as
    ``country=FR/part-0.parquet`` which is the Hive partitioning convention
    understood by Spark, DuckDB, pandas, and other analytical tools.

    Args:
        records:       Validated OrderRecord objects to export.
        output_dir:    Root directory for the dataset (created if absent).
        partition_col: Column name to partition on; defaults to 'country'.
    """
    if not records:
        logger.warning("write_parquet called with empty records list — skipping")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    rows = [
        {
            "order_id":      r.order_id,
            "customer_name": r.customer_name,
            "country":       r.country,
            "product":       r.product,
            "quantity":      r.quantity,
            "price":         float(r.price),
            "order_date":    r.order_date.isoformat(),
            "source_type":   r.source_type,
        }
        for r in records
    ]

    table = pa.Table.from_pylist(rows)
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=[partition_col],
    )
    logger.info(
        "Exported %d records to %s (partitioned by %s)",
        len(records), output_dir, partition_col,
    )
