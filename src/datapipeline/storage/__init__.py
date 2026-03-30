"""Storage package: async helpers for persisting records and errors to PostgreSQL."""

import json

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import StorageError
from ..validation.models import OrderRecord
from .orm_models import IngestionError, Order


async def insert_orders(session: AsyncSession, records: list[OrderRecord]) -> int:
    """Bulk-insert validated order records and return the count inserted.

    Args:
        session: Active async DB session.
        records: List of validated OrderRecord objects.

    Returns:
        Number of rows inserted.

    Raises:
        StorageError: On any database error.
    """
    if not records:
        return 0
    try:
        orders = [
            Order(
                order_id=r.order_id,
                customer_name=r.customer_name,
                country=r.country,
                product=r.product,
                quantity=r.quantity,
                price=r.price,
                order_date=r.order_date,
                source_type=r.source_type,
            )
            for r in records
        ]
        session.add_all(orders)
        await session.commit()
        return len(orders)
    except SQLAlchemyError as exc:
        await session.rollback()
        raise StorageError(str(exc)) from exc


async def fetch_all_orders(session: AsyncSession) -> list[OrderRecord]:
    """Fetch all persisted orders and return them as validated OrderRecord objects.

    Used by the CLI to populate the Parquet export after a pipeline run.

    Args:
        session: Active async DB session.

    Returns:
        List of OrderRecord objects for every row in the orders table.
    """
    from sqlalchemy import select

    from ..validation.models import OrderRecord

    rows = (await session.execute(select(Order))).scalars().all()
    return [
        OrderRecord(
            order_id=o.order_id,
            customer_name=o.customer_name,
            country=o.country,
            product=o.product,
            quantity=o.quantity,
            price=o.price,
            order_date=o.order_date,
            source_type=o.source_type,
        )
        for o in rows
    ]


async def insert_error(
    session: AsyncSession, source_file: str, row: dict, reason: str
) -> None:
    """Persist a row-level ingestion error for audit purposes.

    Args:
        session:     Active async DB session.
        source_file: Path/name of the file that contained the bad row.
        row:         The raw dict that failed validation.
        reason:      Human-readable explanation of why the row was rejected.
    """
    try:
        session.add(
            IngestionError(
                source_file=source_file,
                row_data=json.dumps(row, default=str),
                reason=reason,
            )
        )
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        raise StorageError(str(exc)) from exc
