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
    # Use pgvector's cosine distance operator (<=>).
    # We pass the vector as a plain SQL literal to avoid driver-level type-quoting
    # issues with the vector[] type.
    # Raise ivfflat.probes so small tables (few clusters populated) are searched fully.
    vec_literal = str(query_embedding)  # e.g. "[0.1, 0.2, ...]"
    await session.execute(text("SET ivfflat.probes = 100"))
    stmt = text(
        "SELECT source_file, content, "
        f"  1 - (embedding <=> '{vec_literal}'::vector) AS similarity "
        "FROM document_chunks "
        f"ORDER BY embedding <=> '{vec_literal}'::vector "
        f"LIMIT {int(limit)}"
    )
    rows = (await session.execute(stmt)).mappings().all()
    return [
        {
            "source_file": r["source_file"],
            "content":     r["content"],
            "similarity":  float(r["similarity"]),
        }
        for r in rows
    ]
