"""Tests for the PydanticAI unstructured parser and embedding storage.

All OpenAI API calls are mocked so no OPENAI_API_KEY is required.
"""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from datapipeline.ingestion.unstructured_parser import (
    ExtractedOrder,
    embed_and_store,
    parse_text,
)
from datapipeline.storage.orm_models import DocumentChunk, IngestionError, Order
from datapipeline.storage import insert_orders

# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_TEXT = (
    "Customer Alice Martin from France placed order ORD-U001 for "
    "2 units of Widget at $9.99 each on 2024-01-15."
)

_VALID_EXTRACTED = [
    ExtractedOrder(
        order_id="ORD-U001",
        customer_name="Alice Martin",
        country="FR",
        product="Widget",
        quantity="2",
        price="9.99",
        order_date="2024-01-15",
    )
]

_INVALID_EXTRACTED = [
    ExtractedOrder(
        order_id="",          # invalid: empty order_id
        customer_name=None,
        country="FR",
        product="Widget",
        quantity="-1",        # invalid: negative quantity
        price="9.99",
        order_date="2024-01-15",
    )
]


def _mock_agent_run(extracted: list[ExtractedOrder]):
    """Return a mock that simulates agent.run() returning *extracted*."""
    result = MagicMock()
    result.response = extracted
    mock_run = AsyncMock(return_value=result)
    return mock_run


def _mock_embeddings_response(dims: int = 1536):
    """Return a mock mimicking openai embeddings.create() response."""
    embedding_data = MagicMock()
    embedding_data.embedding = [0.1] * dims
    response = MagicMock()
    response.data = [embedding_data]
    return response


# ── parse_text tests ──────────────────────────────────────────────────────────

async def test_parse_text_returns_valid_records(async_session, monkeypatch):
    """parse_text returns valid OrderRecord objects for well-formed AI output."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    with (
        patch(
            "datapipeline.ingestion.unstructured_parser.settings.openai_api_key",
            "sk-test",
        ),
        patch(
            "datapipeline.ingestion.unstructured_parser._get_agent"
        ) as mock_get_agent,
    ):
        agent = MagicMock()
        agent.run = _mock_agent_run(_VALID_EXTRACTED)
        mock_get_agent.return_value = agent

        records = await parse_text(SAMPLE_TEXT, "test.txt", async_session)

    assert len(records) == 1
    assert records[0].order_id == "ORD-U001"
    assert records[0].source_type == "text"
    assert records[0].quantity == 2


async def test_parse_text_persists_invalid_rows(async_session):
    """Rows that fail OrderRecord validation are stored in ingestion_errors."""
    with (
        patch(
            "datapipeline.ingestion.unstructured_parser.settings.openai_api_key",
            "sk-test",
        ),
        patch(
            "datapipeline.ingestion.unstructured_parser._get_agent"
        ) as mock_get_agent,
    ):
        agent = MagicMock()
        agent.run = _mock_agent_run(_INVALID_EXTRACTED)
        mock_get_agent.return_value = agent

        records = await parse_text(SAMPLE_TEXT, "bad.txt", async_session)

    # No valid records — all failed validation.
    assert len(records) == 0

    errors = (await async_session.execute(select(IngestionError))).scalars().all()
    assert len(errors) == 1
    assert errors[0].source_file == "bad.txt"


async def test_parse_text_no_api_key_raises(async_session):
    """parse_text raises IngestionError when OPENAI_API_KEY is not configured."""
    from datapipeline.exceptions import IngestionError as AppIngestionError

    with patch(
        "datapipeline.ingestion.unstructured_parser.settings.openai_api_key",
        None,
    ):
        with pytest.raises(AppIngestionError, match="OPENAI_API_KEY"):
            await parse_text(SAMPLE_TEXT, "test.txt", async_session)


# ── embed_and_store tests ─────────────────────────────────────────────────────

async def test_embed_and_store_creates_document_chunk(async_session):
    """embed_and_store inserts a DocumentChunk with the returned embedding."""
    mock_response = _mock_embeddings_response()

    with (
        patch(
            "datapipeline.ingestion.unstructured_parser.settings.openai_api_key",
            "sk-test",
        ),
        patch(
            "datapipeline.ingestion.unstructured_parser.AsyncOpenAI"
        ) as mock_openai_cls,
    ):
        mock_client = AsyncMock()
        mock_client.embeddings.create = AsyncMock(return_value=mock_response)
        mock_openai_cls.return_value = mock_client

        await embed_and_store(SAMPLE_TEXT, "test.txt", async_session)

    chunks = (await async_session.execute(select(DocumentChunk))).scalars().all()
    assert len(chunks) == 1
    assert chunks[0].source_file == "test.txt"
    assert chunks[0].content == SAMPLE_TEXT


async def test_embed_and_store_no_api_key_raises(async_session):
    """embed_and_store raises IngestionError when OPENAI_API_KEY is not set."""
    from datapipeline.exceptions import IngestionError as AppIngestionError

    with patch(
        "datapipeline.ingestion.unstructured_parser.settings.openai_api_key",
        None,
    ):
        with pytest.raises(AppIngestionError, match="OPENAI_API_KEY"):
            await embed_and_store(SAMPLE_TEXT, "test.txt", async_session)
