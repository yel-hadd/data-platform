"""Unstructured text parser: PydanticAI + OpenAI structured extraction + pgvector embedding."""

import logging
from datetime import date
from decimal import Decimal

from openai import AsyncOpenAI
from pydantic import BaseModel
from pydantic_ai import Agent
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..exceptions import IngestionError, RowValidationError
from ..storage.orm_models import DocumentChunk
from ..validation.models import OrderRecord

logger = logging.getLogger(__name__)

# ── Lenient extraction schema ─────────────────────────────────────────────────

class ExtractedOrder(BaseModel):
    """Flexible schema for AI extraction — all fields optional to handle partial data."""

    order_id: str | None = None
    customer_name: str | None = None
    country: str | None = None
    product: str | None = None
    quantity: str | None = None
    price: str | None = None
    order_date: str | None = None


# ── PydanticAI agent (module-level, reused across calls) ─────────────────────

_agent: Agent | None = None


def _get_agent() -> Agent:
    """Return (or lazily create) the extraction agent.

    Raises:
        IngestionError: If OPENAI_API_KEY is not configured.
    """
    global _agent
    if _agent is None:
        if not settings.openai_api_key:
            raise IngestionError(
                "OPENAI_API_KEY is not set — cannot parse unstructured text files."
            )
        _agent = Agent(
            settings.extraction_model,
            output_type=list[ExtractedOrder],
            system_prompt=(
                "You are a data extraction assistant. "
                "Given a block of free-form text, extract every order mentioned. "
                "For each order return: order_id, customer_name, country (ISO-2 code), "
                "product, quantity (integer), price (decimal), order_date (YYYY-MM-DD). "
                "Return an empty list if no orders are found. "
                "Never invent data that is not present in the text."
            ),
        )
    return _agent


# ── Public async functions ────────────────────────────────────────────────────

async def parse_text(
    text: str,
    source_file: str,
    session: AsyncSession,
) -> list[OrderRecord]:
    """Use PydanticAI (GPT-4o-mini) to extract structured orders from free-form text.

    Valid rows are returned as ``OrderRecord`` objects ready for ``insert_orders``.
    Invalid rows (failed coercion) are persisted to ``ingestion_errors`` via the
    provided session.

    Args:
        text:        Raw text content to parse.
        source_file: File path used for error attribution.
        session:     Active async DB session for error persistence.

    Returns:
        List of successfully validated OrderRecord objects.
    """
    from ..storage import insert_error  # local import to avoid circular dependency

    agent = _get_agent()
    logger.info("Sending %d chars to PydanticAI for extraction from %s", len(text), source_file)

    result = await agent.run(text)
    extracted: list[ExtractedOrder] = result.output

    valid_records: list[OrderRecord] = []
    for raw in extracted:
        try:
            record = OrderRecord.model_validate(
                {
                    "order_id":      raw.order_id or "",
                    "customer_name": raw.customer_name or "",
                    "country":       raw.country or "",
                    "product":       raw.product or "",
                    "quantity":      int(raw.quantity or 0),
                    "price":         Decimal(raw.price or "0"),
                    "order_date":    raw.order_date or "",
                    "source_type":   "text",
                }
            )
            valid_records.append(record)
        except Exception as exc:
            await insert_error(session, source_file, raw.model_dump(), str(exc))
            logger.warning("Extracted row failed validation from %s: %s", source_file, exc)

    logger.info(
        "Extracted %d valid, %d invalid orders from %s",
        len(valid_records), len(extracted) - len(valid_records), source_file,
    )
    return valid_records


async def embed_and_store(
    text: str,
    source_file: str,
    session: AsyncSession,
) -> None:
    """Generate an OpenAI embedding for *text* and persist it to document_chunks.

    Args:
        text:        Text to embed and store.
        source_file: Source file path for attribution.
        session:     Active async DB session.

    Raises:
        IngestionError: If OPENAI_API_KEY is not configured.
    """
    if not settings.openai_api_key:
        raise IngestionError(
            "OPENAI_API_KEY is not set — cannot generate embeddings."
        )

    client = AsyncOpenAI(api_key=settings.openai_api_key)
    response = await client.embeddings.create(
        input=text,
        model=settings.embedding_model,
    )
    vector = response.data[0].embedding  # list[float], length 1536

    session.add(
        DocumentChunk(
            source_file=source_file,
            content=text,
            embedding=vector,
        )
    )
    await session.commit()
    logger.info("Stored embedding for %s (%d dims)", source_file, len(vector))
