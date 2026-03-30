"""Analytical report queries executed against the orders table."""

from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..storage.orm_models import Order


async def revenue_by_country(session: AsyncSession) -> list[dict]:
    """Return total revenue per country, sorted descending by revenue.

    Args:
        session: Active async DB session.

    Returns:
        List of dicts with keys 'country' and 'revenue'.
    """
    stmt = (
        select(
            Order.country,
            func.sum(Order.quantity * Order.price).label("revenue"),
        )
        .group_by(Order.country)
        .order_by(func.sum(Order.quantity * Order.price).desc())
    )
    rows = (await session.execute(stmt)).all()
    return [{"country": r.country, "revenue": float(r.revenue)} for r in rows]


async def revenue_by_product(session: AsyncSession) -> list[dict]:
    """Return total revenue per product, sorted descending by revenue.

    Args:
        session: Active async DB session.

    Returns:
        List of dicts with keys 'product' and 'revenue'.
    """
    stmt = (
        select(
            Order.product,
            func.sum(Order.quantity * Order.price).label("revenue"),
        )
        .group_by(Order.product)
        .order_by(func.sum(Order.quantity * Order.price).desc())
    )
    rows = (await session.execute(stmt)).all()
    return [{"product": r.product, "revenue": float(r.revenue)} for r in rows]


async def top_customers(session: AsyncSession, limit: int = 10) -> list[dict]:
    """Return the top *limit* customers by total revenue, descending.

    Args:
        session: Active async DB session.
        limit:   Maximum number of customers to return (default 10).

    Returns:
        List of dicts with keys 'customer_name' and 'revenue'.
    """
    stmt = (
        select(
            Order.customer_name,
            func.sum(Order.quantity * Order.price).label("revenue"),
        )
        .group_by(Order.customer_name)
        .order_by(func.sum(Order.quantity * Order.price).desc())
        .limit(limit)
    )
    rows = (await session.execute(stmt)).all()
    return [{"customer_name": r.customer_name, "revenue": float(r.revenue)} for r in rows]


async def revenue_trend(session: AsyncSession) -> list[dict]:
    """Return daily total revenue ordered chronologically for time-series analysis.

    Args:
        session: Active async DB session.

    Returns:
        List of dicts with keys 'order_date' (ISO string) and 'revenue'.
    """
    stmt = (
        select(
            Order.order_date,
            func.sum(Order.quantity * Order.price).label("revenue"),
        )
        .group_by(Order.order_date)
        .order_by(Order.order_date)
    )
    rows = (await session.execute(stmt)).all()
    return [
        {"order_date": r.order_date.isoformat(), "revenue": float(r.revenue)}
        for r in rows
    ]
