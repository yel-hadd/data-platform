"""Analytical metrics endpoints backed by the orders table."""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ...analytics.reports import (
    revenue_by_country,
    revenue_by_product,
    revenue_trend,
    top_customers,
)
from ...analytics.semantic_search import get_query_embedding, semantic_search
from ...exceptions import IngestionError, StorageError
from ..deps import get_session

router = APIRouter(tags=["metrics"])


# ── Response schemas ──────────────────────────────────────────────────────────

class CountryRevenue(BaseModel):
    country: str
    revenue: float


class ProductRevenue(BaseModel):
    product: str
    revenue: float


class CustomerRevenue(BaseModel):
    customer_name: str
    revenue: float


class DailyRevenue(BaseModel):
    order_date: str
    revenue: float


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/revenue-by-country", response_model=list[CountryRevenue])
async def get_revenue_by_country(
    session: AsyncSession = Depends(get_session),
) -> list[CountryRevenue]:
    """Return total revenue aggregated by country, sorted descending."""
    try:
        rows = await revenue_by_country(session)
    except StorageError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return [CountryRevenue(**r) for r in rows]


@router.get("/revenue-by-product", response_model=list[ProductRevenue])
async def get_revenue_by_product(
    session: AsyncSession = Depends(get_session),
) -> list[ProductRevenue]:
    """Return total revenue aggregated by product, sorted descending."""
    try:
        rows = await revenue_by_product(session)
    except StorageError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return [ProductRevenue(**r) for r in rows]


@router.get("/top-customers", response_model=list[CustomerRevenue])
async def get_top_customers(
    limit: int = Query(default=10, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> list[CustomerRevenue]:
    """Return the top customers by total spend, limited to *limit* results."""
    try:
        rows = await top_customers(session, limit=limit)
    except StorageError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return [CustomerRevenue(**r) for r in rows]


@router.get("/revenue-trend", response_model=list[DailyRevenue])
async def get_revenue_trend(
    session: AsyncSession = Depends(get_session),
) -> list[DailyRevenue]:
    """Return daily revenue totals ordered chronologically."""
    try:
        rows = await revenue_trend(session)
    except StorageError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return [DailyRevenue(**r) for r in rows]


# ── Semantic search ───────────────────────────────────────────────────────────

class SemanticResult(BaseModel):
    """Single document chunk returned by semantic similarity search."""

    source_file: str
    content: str
    similarity: float


@router.get("/semantic-search", response_model=list[SemanticResult])
async def get_semantic_search(
    q: str = Query(min_length=1, description="Natural-language query to embed and search."),
    limit: int = Query(default=5, ge=1, le=50),
    session: AsyncSession = Depends(get_session),
) -> list[SemanticResult]:
    """Return the most semantically similar document chunks to the query.

    Embeds *q* via OpenAI text-embedding-3-small and queries the pgvector
    cosine index on document_chunks.  Requires OPENAI_API_KEY to be set.
    """
    try:
        embedding = await get_query_embedding(q)
        rows = await semantic_search(session, embedding, limit=limit)
    except IngestionError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return [SemanticResult(**r) for r in rows]
