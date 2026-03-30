"""Semantic similarity search over document_chunks using pgvector."""

import logging

from openai import AsyncOpenAI
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..exceptions import IngestionError
from ..storage.orm_models import DocumentChunk

logger = logging.getLogger(__name__)


async def get_query_embedding(query: str) -> list[float]:
    """Embed *query* using OpenAI text-embedding-3-small.

    Args:
        query: Text to embed.

    Returns:
        1536-dim float vector.

    Raises:
        IngestionError: If OPENAI_API_KEY is not set.
    """
    if not settings.openai_api_key:
        raise IngestionError("OPENAI_API_KEY is not set — cannot generate query embedding.")
    client = AsyncOpenAI(api_key=settings.openai_api_key)
    response = await client.embeddings.create(
        input=query,
        model=settings.embedding_model,
    )
    return response.data[0].embedding


async def semantic_search(
    session: AsyncSession,
    query_embedding: list[float],
    limit: int = 5,
) -> list[dict]:
    """Return the *limit* most similar document chunks by cosine distance.

    Uses pgvector's ``<=>`` (cosine distance) operator.  Results are ordered
    nearest-first (lowest distance = highest similarity).

    Args:
        session:         Active async DB session.
        query_embedding: Pre-computed query vector (1536 floats).
        limit:           Maximum number of results to return.

    Returns:
        List of dicts with keys ``source_file``, ``content``, ``similarity`` (0–1).
    """
    # Build the pgvector cosine distance expression as text to keep the query
    # portable across SQLAlchemy dialects in tests.
    stmt = (
        select(
            DocumentChunk.source_file,
            DocumentChunk.content,
            (
                text("1 - (embedding <=> CAST(:vec AS vector))")
            ).bindparams(vec=str(query_embedding)).label("similarity"),
        )
        .order_by(text("embedding <=> CAST(:vec AS vector)").bindparams(vec=str(query_embedding)))
        .limit(limit)
    )
    rows = (await session.execute(stmt)).all()
    return [
        {
            "source_file": r.source_file,
            "content":     r.content,
            "similarity":  float(r.similarity),
        }
        for r in rows
    ]
