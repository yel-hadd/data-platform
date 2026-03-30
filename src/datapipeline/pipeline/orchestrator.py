"""Async pipeline orchestrator: processes multiple files concurrently."""

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..ingestion import get_loader
from ..ingestion.detector import detect_format, discover_files
from ..storage import insert_error, insert_orders
from ..transform.normalize import normalize
from ..validation.models import OrderRecord

logger = logging.getLogger(__name__)


@dataclass
class FileResult:
    """Outcome of processing a single file."""

    path: str
    valid: int = 0
    invalid: int = 0
    error: str | None = None


@dataclass
class PipelineResult:
    """Aggregated outcome of the full pipeline run."""

    files_processed: int = 0
    total_valid: int = 0
    total_invalid: int = 0
    file_results: list[FileResult] = field(default_factory=list)


async def process_file(
    path: Path, session_factory: async_sessionmaker[AsyncSession]
) -> FileResult:
    """Load, normalise, validate and persist one data file.

    Each file gets its own session so concurrent gather calls don't conflict.

    Args:
        path:            Path to the data file.
        session_factory: Factory used to open a fresh session for this file.

    Returns:
        FileResult with counts of valid and invalid rows.
    """
    result = FileResult(path=str(path))
    try:
        fmt = detect_format(path)

        # Text files go through the AI extraction path instead of the normaliser.
        if fmt == "text":
            await _process_text_file(path, session_factory, result)
            return result

        loader = get_loader(fmt)
        valid_records: list[OrderRecord] = []

        async with session_factory() as session:
            for raw in loader.load(path):
                try:
                    canonical = normalize(raw, source_type=fmt)
                    valid_records.append(OrderRecord.model_validate(canonical))
                except Exception as exc:
                    result.invalid += 1
                    await insert_error(session, str(path), raw, str(exc))
                    logger.warning("Rejected row from %s: %s", path.name, exc)

            result.valid = await insert_orders(session, valid_records)

        logger.info(
            "Processed %s: %d valid, %d invalid",
            path.name, result.valid, result.invalid,
        )
    except Exception as exc:
        result.error = str(exc)
        logger.error("Failed to process %s: %s", path.name, exc)

    return result


async def _process_text_file(
    path: Path,
    session_factory: async_sessionmaker[AsyncSession],
    result: FileResult,
) -> None:
    """Delegate a .txt file to the PydanticAI unstructured parser.

    Extracts orders with GPT-4o-mini, stores the text embedding with pgvector,
    and persists valid orders — all within a single dedicated session.

    Args:
        path:            Path to the .txt file.
        session_factory: Session factory for DB writes.
        result:          FileResult mutated in-place with valid/invalid counts.
    """
    from ..ingestion.unstructured_parser import embed_and_store, parse_text

    text = path.read_text(encoding="utf-8")
    async with session_factory() as session:
        valid_records = await parse_text(text, str(path), session)
        result.valid = await insert_orders(session, valid_records)
        result.invalid = 0  # validation failures already written by parse_text

        # Store text + embedding for semantic search — best-effort.
        try:
            await embed_and_store(text, str(path), session)
        except Exception as exc:
            logger.warning("Embedding skipped for %s: %s", path.name, exc)

    logger.info(
        "Processed (text) %s: %d valid orders extracted",
        path.name, result.valid,
    )


async def run_pipeline(
    data_dir: Path, session_factory: async_sessionmaker[AsyncSession]
) -> PipelineResult:
    """Discover all data files in *data_dir* and process them concurrently.

    Uses asyncio.gather so that I/O-bound loading of multiple files overlaps.
    Each file is processed in its own DB session to avoid shared-state conflicts.
    Individual file failures are captured in FileResult.error, ensuring one
    bad file does not abort the entire run.

    Args:
        data_dir:        Directory containing raw data files.
        session_factory: SQLAlchemy async_sessionmaker for creating per-file sessions.

    Returns:
        PipelineResult with per-file and aggregate counts.
    """
    files = discover_files(data_dir)
    logger.info("Discovered %d file(s) in %s", len(files), data_dir)

    file_results: list[FileResult] = await asyncio.gather(
        *[process_file(f, session_factory) for f in files]
    )

    pipeline_result = PipelineResult(
        files_processed=len(files),
        total_valid=sum(r.valid for r in file_results),
        total_invalid=sum(r.invalid for r in file_results),
        file_results=list(file_results),
    )
    logger.info(
        "Pipeline complete: %d files, %d valid, %d invalid",
        pipeline_result.files_processed,
        pipeline_result.total_valid,
        pipeline_result.total_invalid,
    )
    return pipeline_result
