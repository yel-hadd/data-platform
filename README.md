# Advanced Python Data Platform

A production-ready, async multi-format data ingestion, validation, storage, and analytics platform built with Python 3.13, FastAPI, SQLAlchemy 2, Pydantic v2, and Docker Compose.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Business Objective](#2-business-objective)
3. [Main Features](#3-main-features)
4. [Architecture](#4-architecture)
5. [Technology Stack](#5-technology-stack)
6. [Installation & Setup](#6-installation--setup)
7. [Configuration](#7-configuration)
8. [Running with Docker Compose](#8-running-with-docker-compose)
9. [Running the Pipeline (CLI)](#9-running-the-pipeline-cli)
10. [Running the API](#10-running-the-api)
11. [Running the Tests](#11-running-the-tests)
12. [Repository Structure](#12-repository-structure)
13. [Datasets Used](#13-datasets-used)
14. [Known Limitations & Areas for Improvement](#14-known-limitations--areas-for-improvement)

---

## 1. Project Overview

This platform ingests tabular order data from heterogeneous file formats, validates and normalises every row into a canonical `OrderRecord` schema, persists valid records to PostgreSQL, and surfaces aggregate analytics via a FastAPI REST API.

It also supports AI-driven extraction from unstructured text files using PydanticAI (GPT-4o-mini) and stores text embeddings in PostgreSQL via pgvector for semantic search.

Invalid rows are **never silently dropped**: they are written to an `ingestion_errors` table with the original row data and failure reason for full audit and replay capability.

---

## 2. Business Objective

A company receives sales and order data from multiple heterogeneous sources — legacy XML systems, Excel spreadsheets, CSV exports, JSON event streams, and plain-text summaries. This platform provides a single, automated pipeline that:

- eliminates manual data wrangling by auto-detecting and parsing each format,
- enforces data quality through strict Pydantic validation before any record reaches the database,
- delivers consistent analytical metrics (revenue, top customers, trends) via a stable API consumed by dashboards and reporting tools,
- enables AI-powered querying of unstructured order summaries through semantic search.

---

## 3. Main Features

- **Multi-format ingestion** — CSV (chunked), Excel, JSON, NDJSON, XML, ZIP archives, and unstructured `.txt` files
- **Canonical schema** — all sources normalised to a single `OrderRecord` Pydantic model before storage
- **Zero silent failures** — invalid rows isolated to `ingestion_errors` table with full context
- **Async concurrent pipeline** — all files processed in parallel via `asyncio.gather` with per-file sessions
- **FastAPI analytics API** — revenue, product, customer, and trend metrics over HTTP
- **AI unstructured parser** — PydanticAI + OpenAI GPT extracts structured orders from free-form text
- **Semantic search** — pgvector stores text embeddings; cosine-similarity search exposed via API
- **Parquet export** — Hive-partitioned by country for Spark/DuckDB/Pandas compatibility
- **Alembic migrations** — version-controlled schema evolution for production deployments
- **Docker Compose** — one command brings up PostgreSQL (with pgvector), the API, and the pipeline worker
- **110 automated tests** — full coverage of all loaders, validation, pipeline, API, and AI features

---

## 4. Architecture

```mermaid
flowchart TD
    A[data/raw/] --> B[FileDetector]
    B --> C{Format?}
    C -->|csv / xlsx / json / xml / zip| D[Format Loader]
    C -->|txt| E[TextLoader]
    D --> F[Normaliser]
    F --> G[Pydantic v2 OrderRecord]
    G -->|valid| H[(orders)]
    G -->|invalid| I[(ingestion_errors)]
    E --> P[PydanticAI Agent\ngpt-4o-mini]
    P -->|extracted orders| H
    E --> V[OpenAI Embeddings\ntext-embedding-3-small]
    V --> J[(document_chunks\npgvector)]
    H --> K[write_parquet]
    K --> L[data/exports/\ncountry=XX/]
    H --> M[FastAPI]
    J --> M
    M --> N[GET /metrics/*]
    M --> O[GET /metrics/semantic-search]
```

**Processing flow for each run:**

1. `discover_files()` scans `data/raw/` and identifies supported formats
2. Each file is dispatched concurrently via `asyncio.gather` (one session per file)
3. Raw rows are normalised to canonical field names, then validated by Pydantic
4. Valid rows → `orders` table; invalid rows → `ingestion_errors` table
5. `.txt` files go through a PydanticAI extraction + OpenAI embedding path
6. A Parquet snapshot is written to `data/exports/` partitioned by country
7. The FastAPI service reads from `orders` and `document_chunks` for analytics

---

## 5. Technology Stack

| Component | Library / Version |
|---|---|
| Language | Python 3.13 |
| Web framework | FastAPI ≥ 0.115 |
| Database driver | asyncpg ≥ 0.30 (PostgreSQL) |
| ORM | SQLAlchemy 2 async |
| Migrations | Alembic ≥ 1.14 |
| Validation & settings | Pydantic v2 + pydantic-settings |
| AI extraction | PydanticAI ≥ 1.0 |
| AI embeddings | OpenAI `text-embedding-3-small` |
| Vector search | pgvector ≥ 0.3 |
| Data processing | pandas ≥ 2.2, pyarrow ≥ 18 |
| Excel | openpyxl ≥ 3.1 |
| CLI | Typer ≥ 0.12 |
| ASGI server | uvicorn |
| Package manager | uv |
| Testing | pytest, pytest-asyncio, aiosqlite, httpx, pytest-mock |
| Database (dev/prod) | PostgreSQL 16 + pgvector |
| Containerisation | Docker Compose |

---

## 6. Installation & Setup

**Requirements:** [uv](https://docs.astral.sh/uv/) and Python 3.13.

```bash
# Clone and enter the project
cd data-platform

# Create virtual environment and install all dependencies (including dev)
uv sync --extra dev

# Verify installation
uv run python -c "import datapipeline; print('OK')"
```

---

## 7. Configuration

Copy `.env.example` to `.env` and edit as needed:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://pipeline:pipeline@localhost:5432/dataplatform` | Async PostgreSQL connection string |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `DATA_DIR` | `data/raw` | Directory scanned for input files |
| `CHUNK_SIZE` | `10000` | Rows read per pandas chunk for large CSV files |
| `OPENAI_API_KEY` | *(unset)* | Required only for unstructured `.txt` file processing |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | OpenAI embedding model used for pgvector storage |
| `EXTRACTION_MODEL` | `openai:gpt-4o-mini` | PydanticAI model used for structured data extraction from text |

> **Note for Docker Compose:** the `.env` file in this repo already points `DATABASE_URL` at the `postgres` service name. Do not change it for Docker usage.

All variables can be set in `.env` or as standard environment variables. The `OPENAI_API_KEY` is only required if you process `.txt` files; all other functionality works without it.

---

## 8. Running with Docker Compose

> **Prerequisites:** Docker Engine installed and running.

### Quick start (recommended)

```bash
# 1. Start PostgreSQL and the API
docker compose up --build -d postgres api

# 2. Generate sample data in all 7 supported formats
docker compose run --rm pipeline uv run python -m datapipeline generate-data --output-dir data/raw

# 3. Run the ingestion pipeline
docker compose run --rm pipeline

# 4. Open the interactive API docs
open http://localhost:8000/docs   # or visit in your browser
```

### Services

| Service | Host port | Purpose |
|---|---|---|
| `postgres` | 5433 | PostgreSQL 16 + pgvector extension |
| `api` | 8000 | FastAPI analytics API |
| `pipeline` | — | One-shot ingestion worker (exits when done) |

> Port 5433 is used on the host to avoid conflicts with any locally-running PostgreSQL. Containers communicate internally on port 5432.

### Useful commands

```bash
# Stop everything and remove the database volume (clean slate)
docker compose down -v

# View live logs
docker compose logs -f api
docker compose logs -f pipeline

# Run Alembic schema migrations inside the container
docker compose run --rm pipeline uv run datapipeline migrate

# Run a pipeline against your own files (place them in data/raw/ first)
docker compose run --rm pipeline
```

---

## 9. Running the Pipeline (CLI)

These commands run on the **host** (requires a running PostgreSQL; use `docker compose up -d postgres` to start one):

```bash
# Generate synthetic sample data in all supported formats
uv run datapipeline generate-data --output-dir data/raw

# Run the ingestion pipeline
uv run datapipeline run --data-dir data/raw

# Run Alembic schema migrations
uv run datapipeline migrate

# Show all available commands
uv run datapipeline --help
```

### What the `run` command does

1. Discovers all supported files in `DATA_DIR`
2. Processes all files concurrently with `asyncio.gather`
3. Writes valid records to `orders`, invalid rows to `ingestion_errors`
4. Exports a Parquet snapshot to `data/exports/`
5. Exits with code `1` if any rows failed validation (useful for CI alerting); exits `0` if all rows were accepted

### Supported file formats

| Extension | Format | Loader | Notes |
|---|---|---|---|
| `.csv` | CSV | `CSVLoader` | Chunked with pandas (`CHUNK_SIZE` rows) |
| `.xlsx` / `.xls` | Excel | `ExcelLoader` | First sheet; via pandas + openpyxl |
| `.json` | JSON array | `JSONLoader` | Top-level array or single object |
| `.ndjson` / `.jsonl` | NDJSON | `NDJSONLoader` | One JSON object per line; streamed |
| `.xml` | XML | `XMLLoader` | `<order>` children via `iterparse` |
| `.zip` | ZIP archive | `ArchiveLoader` | Delegates to inner-file loaders |
| `.txt` | Unstructured text | `TextLoader` + PydanticAI | AI extraction via GPT; requires `OPENAI_API_KEY` |

Files with unsupported extensions are silently skipped by `discover_files()`.

---

## 10. Running the API

```bash
# Start the API server (binds to 0.0.0.0:8000 by default)
uv run datapipeline api

# Or via uvicorn directly (with auto-reload for development)
uv run uvicorn datapipeline.api.main:app --reload
```

Interactive Swagger docs: [http://localhost:8000/docs](http://localhost:8000/docs)

### Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service liveness check |
| GET | `/metrics/revenue-by-country` | Total revenue per country, sorted descending |
| GET | `/metrics/revenue-by-product` | Total revenue per product, sorted descending |
| GET | `/metrics/top-customers?limit=N` | Top N customers by total spend (default 10) |
| GET | `/metrics/revenue-trend` | Daily revenue totals in chronological order |
| GET | `/metrics/semantic-search?q=...&limit=N` | Semantic search over ingested text documents |

### Example requests

```bash
curl http://localhost:8000/health
# {"status": "ok"}

curl http://localhost:8000/metrics/revenue-by-country
# [{"country": "US", "revenue": 12450.50}, ...]

curl "http://localhost:8000/metrics/top-customers?limit=3"
# [{"customer_name": "Alice Martin", "revenue": 4230.00}, ...]

curl "http://localhost:8000/metrics/semantic-search?q=large+order+from+Germany&limit=3"
# [{"source_file": "...", "content": "...", "similarity": 0.91}, ...]
```

---

## 11. Running the Tests

All tests run against an **in-memory SQLite database** — no PostgreSQL, no internet, no API keys required.

```bash
# Run the full test suite
uv run pytest -v

# Run a specific module
uv run pytest tests/test_pipeline.py -v

# Short traceback on failure
uv run pytest --tb=short
```

**110 tests** covering:

| Area | Test file |
|---|---|
| Config loading and env override | `test_config.py` |
| CSV parser + edge cases | `test_csv_loader.py` |
| Excel parser | `test_excel_loader.py` |
| JSON / NDJSON parser | `test_json_loader.py` |
| XML parser | `test_xml_loader.py` |
| ZIP archive handling | `test_archive_loader.py` |
| Unstructured text loader | `test_text_loader.py` |
| File detection and discovery | `test_detector.py` |
| Field normalisation for every source | `test_normalize.py` |
| Pydantic validation (valid + invalid rows) | `test_validation.py` |
| Async DB insert and error persistence | `test_storage.py` |
| Parquet export with partitioning | `test_parquet_writer.py` |
| Async pipeline orchestration | `test_pipeline.py` |
| Analytics queries | `test_reports.py` |
| FastAPI endpoints | `test_api.py` |
| PydanticAI extraction (mocked) | `test_unstructured_parser.py` |
| Semantic search endpoint (mocked) | `test_semantic_search.py` |
| CLI commands | `test_cli.py` |

---

## 12. Repository Structure

```
data-platform/
├── src/datapipeline/
│   ├── __init__.py
│   ├── __main__.py              # python -m datapipeline entry point
│   ├── cli.py                   # Typer CLI: run / api / generate-data / migrate
│   ├── config.py                # pydantic-settings: all env vars
│   ├── exceptions.py            # IngestionError, RowValidationError, StorageError
│   ├── logging_config.py        # root logger setup
│   ├── ingestion/
│   │   ├── __init__.py          # loader registry + get_loader()
│   │   ├── base.py              # BaseLoader ABC + RawRecord type alias
│   │   ├── detector.py          # detect_format() + discover_files()
│   │   ├── csv_loader.py        # CSV with pandas chunking
│   │   ├── json_loader.py       # JSON array + NDJSON streaming
│   │   ├── xml_loader.py        # XML iterparse
│   │   ├── excel_loader.py      # xlsx/xls via pandas + openpyxl
│   │   ├── archive_loader.py    # ZIP extraction + delegation
│   │   ├── text_loader.py       # .txt files → raw text record
│   │   └── unstructured_parser.py  # PydanticAI extraction + embedding storage
│   ├── validation/
│   │   └── models.py            # OrderRecord (Pydantic v2 canonical schema)
│   ├── transform/
│   │   └── normalize.py         # source-field-to-canonical remapping
│   ├── storage/
│   │   ├── __init__.py          # insert_orders(), insert_error()
│   │   ├── database.py          # async engine, session factory, init_db()
│   │   ├── orm_models.py        # Order, IngestionError, DocumentChunk ORM classes
│   │   └── parquet_writer.py    # write_parquet() with Hive partitioning
│   ├── analytics/
│   │   ├── reports.py           # revenue_by_country/product, top_customers, trend
│   │   └── semantic_search.py   # pgvector cosine-similarity query
│   ├── pipeline/
│   │   └── orchestrator.py      # run_pipeline() with asyncio.gather
│   └── api/
│       ├── deps.py              # get_session() FastAPI dependency
│       ├── main.py              # FastAPI app + lifespan
│       └── routes/
│           ├── health.py        # GET /health
│           └── metrics.py       # GET /metrics/* endpoints
├── alembic/
│   ├── env.py                   # async Alembic environment
│   └── versions/
│       └── 001_initial_schema.py
├── tests/
│   ├── conftest.py              # async_engine, async_session, sample_orders fixtures
│   ├── fixtures/                # sample.csv, .json, .ndjson, .xml
│   └── test_*.py                # 18 test modules, 93 tests total
├── data/
│   ├── raw/                     # drop input files here (or use generate-data)
│   ├── processed/               # reserved for future use
│   └── exports/                 # Parquet output (country-partitioned)
├── sql/
│   └── init.sql                 # PostgreSQL schema + pgvector extension (Docker init)
├── alembic.ini
├── pyproject.toml
├── uv.lock
├── Dockerfile
├── .dockerignore
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## 13. Datasets Used

The `generate-data` CLI command creates synthetic sample data in every supported format. All sources represent the same business entity (orders) in format-specific representations:

| File | Format | Contents |
|---|---|---|
| `data/raw/orders.csv` | CSV | 500 order rows, chunked ingestion |
| `data/raw/sales.xlsx` | Excel | 200 rows, Excel column naming conventions |
| `data/raw/events.json` | JSON | 100 events in camelCase JSON array |
| `data/raw/stream.ndjson` | NDJSON | 100 records, one JSON object per line |
| `data/raw/legacy.xml` | XML | `<orders><order>...</order></orders>` legacy format |
| `data/raw/archive.zip` | ZIP | ZIP containing a nested CSV and JSON file |
| `data/raw/unstructured_orders.txt` | Text | Natural-language order descriptions for AI extraction |

About 5% of rows are intentionally invalid (negative quantities, missing order IDs) to demonstrate the validation and error-isolation pipeline. Generate them with:

```bash
# Via Docker Compose
docker compose run --rm pipeline uv run python -m datapipeline generate-data --output-dir data/raw

# Or locally
uv run datapipeline generate-data --output-dir data/raw
```

### Parquet export structure

After a pipeline run, all validated records are exported to `data/exports/` partitioned by country (Hive layout), natively readable by Spark, DuckDB, and Pandas:

```
data/exports/
├── country=DE/part-0.parquet
├── country=FR/part-0.parquet
└── country=US/part-0.parquet
```

```python
import pandas as pd
df = pd.read_parquet("data/exports/", filters=[("country", "=", "FR")])
```

### Data normalisation

Each source format uses different column names for the same business concept. The normaliser maps every source to the canonical `OrderRecord` schema:

```python
class OrderRecord(BaseModel):
    order_id:            str       # non-empty
    customer_name:       str       # non-empty
    country:             str       # 2–100 chars
    product:             str       # non-empty
    quantity:            int       # > 0
    price:               Decimal   # >= 0
    order_date:          date
    source_type:         str       # loader format key
    ingestion_timestamp: datetime  # auto-generated at load time
```

| Source | `order_id` field | `customer_name` field |
|---|---|---|
| CSV | `order_id` | `customer_name` |
| Excel | `Order ID` | `Customer` |
| JSON | `orderId` | `customer` |
| XML | `<order_id>` | `<customer_name>` |

---

## 14. Known Limitations & Areas for Improvement

| Area | Current state | Potential improvement |
|---|---|---|
| **AI extraction cost** | One OpenAI API call per `.txt` file | Batch or cache embeddings to reduce cost |
| **pgvector index** | IVFFlat with `lists=100` (approximate) | Switch to HNSW for better recall at scale |
| **Alembic autogenerate** | `Vector` column type requires manual DDL | Custom type comparator for full autogenerate support |
| **CI pipeline** | No automated CI | Add GitHub Actions: lint → test → build Docker image |
| **Authentication** | API is unauthenticated | Add API key or OAuth2 middleware |
| **Retry / backoff** | No retry on DB or OpenAI failures | Add `tenacity`-based retry decorator |
| **Streaming large files** | XML uses iterparse; CSV uses chunking | Explore async generators for all loaders |
| **Multi-table schema** | Single `orders` table | Add `customers` and `products` dimension tables |
| **Monitoring** | Structured logs only | Add Prometheus metrics + Grafana dashboard |
| **Pipeline exit code** | Exits `1` when any rows fail validation | Add `--strict` flag to control this behaviour |
