"""Typer CLI: entry point for the data pipeline, API server, and data generation."""

import asyncio
import random
import zipfile
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(
    name="datapipeline",
    help=(
        "Advanced Data Platform — ingest, validate, store, and analyse order data.\n\n"
        "Run 'datapipeline COMMAND --help' for detailed usage of any command."
    ),
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        pkg = _pkg_version("datapipeline")
        typer.echo(f"datapipeline {pkg}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(  # noqa: UP007
        None,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Print version and exit.",
    ),
) -> None:
    """Advanced Data Platform CLI."""


# ── Data generation helpers ───────────────────────────────────────────────────

_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "NL", "SE", "PL", "PT"]
_PRODUCTS = ["Widget", "Gadget", "Doohickey", "Thingamajig", "Whatsit"]
_NAMES = [
    "Alice Martin", "Bob Smith", "Carol Lee", "Dave Brown", "Eve White",
    "Frank Green", "Grace Hall", "Hank King", "Ivy Jones", "Jack Davis",
    "Karen Wilson", "Leo Taylor", "Mia Anderson", "Nate Thomas", "Olivia Jackson",
]


def _random_date(year: int = 2024) -> str:
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


def _make_row(i: int, invalid_rate: float = 0.05) -> dict:
    """Return a synthetic order row, with ~invalid_rate chance of being invalid."""
    if random.random() < invalid_rate:
        return {
            "order_id": "" if random.random() < 0.5 else f"ORD-{i:05d}",
            "customer_name": random.choice(_NAMES),
            "country": random.choice(_COUNTRIES),
            "product": random.choice(_PRODUCTS),
            "quantity": str(random.randint(-5, 0)),  # intentionally invalid
            "price": f"{random.uniform(5, 100):.2f}",
            "order_date": _random_date(),
        }
    return {
        "order_id": f"ORD-{i:05d}",
        "customer_name": random.choice(_NAMES),
        "country": random.choice(_COUNTRIES),
        "product": random.choice(_PRODUCTS),
        "quantity": str(random.randint(1, 20)),
        "price": f"{random.uniform(5, 150):.2f}",
        "order_date": _random_date(),
    }


# ── Commands ──────────────────────────────────────────────────────────────────

@app.command()
def run(
    data_dir: Path = typer.Option(
        Path("data/raw"),
        envvar="DATA_DIR",
        help="Directory containing raw input files.",
        show_default=True,
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Print a per-file breakdown (valid / invalid counts) after completion.",
    ),
    export: bool = typer.Option(
        True,
        "--export/--no-export",
        help="Write a partitioned Parquet snapshot to exports_dir after ingestion.",
    ),
) -> None:
    """Run the full async ingestion pipeline on all files in DATA_DIR.

    Files are processed concurrently. Valid rows are inserted into PostgreSQL;
    invalid rows are written to the ingestion_errors table for audit and replay.
    Exits with code 1 if any file produced validation errors.

    Examples:

        $ datapipeline run

        $ datapipeline run --data-dir /mnt/data/raw --verbose

        $ datapipeline run --no-export
    """
    from .config import settings
    from .logging_config import configure_logging
    from .pipeline.orchestrator import run_pipeline
    from .storage.database import init_db

    configure_logging(settings.log_level)

    async def _run() -> None:
        await init_db()
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        result = await run_pipeline(data_dir, factory)

        if verbose:
            typer.echo("\nPer-file results:")
            for file_result in result.file_results:
                typer.echo(
                    f"  {Path(file_result.path).name}: "
                    f"{file_result.valid} valid, {file_result.invalid} invalid"
                )
            typer.echo("")

        typer.echo(
            f"Pipeline complete — {result.files_processed} file(s), "
            f"{result.total_valid} valid row(s), {result.total_invalid} invalid row(s)"
        )

        if export:
            from .storage import fetch_all_orders
            from .storage.parquet_writer import write_parquet

            typer.echo(f"Writing Parquet export to {settings.exports_dir} …")
            async with factory() as session:
                records = await fetch_all_orders(session)
            write_parquet(records, settings.exports_dir)
            typer.echo(f"Parquet export complete ({len(records)} records).")

        if result.total_invalid > 0:
            raise typer.Exit(code=1)

    asyncio.run(_run())


@app.command()
def api(
    host: str = typer.Option("0.0.0.0", help="Bind host.", show_default=True),
    port: int = typer.Option(8000, help="Bind port.", show_default=True),
    reload: bool = typer.Option(
        False,
        "--reload/--no-reload",
        help="Enable auto-reload on source changes (development only).",
    ),
    workers: int = typer.Option(
        1,
        help="Number of uvicorn worker processes. Ignored when --reload is set.",
        show_default=True,
    ),
) -> None:
    """Start the FastAPI analytics API server.

    Use --reload during development for automatic hot-reload on file changes.
    In production, increase --workers to match available CPU cores (requires
    --no-reload, which is the default).

    Examples:

        $ datapipeline api

        $ datapipeline api --host 127.0.0.1 --port 9000 --reload

        $ datapipeline api --workers 4
    """
    import uvicorn

    uvicorn.run(
        "datapipeline.api.main:app",
        host=host,
        port=port,
        reload=reload,
        workers=1 if reload else workers,
    )


@app.command()
def migrate(
    revision: str = typer.Option(
        "head",
        help="Target Alembic revision (e.g. 'head', a revision ID, or '+1'/'-1').",
        show_default=True,
    ),
) -> None:
    """Apply pending Alembic database migrations.

    Upgrades the schema to the specified revision (default: head — the latest).
    Prints the current revision before and after the upgrade so you can confirm
    what changed.

    Examples:

        $ datapipeline migrate

        $ datapipeline migrate --revision 001_initial_schema
    """
    from alembic import command
    from alembic.config import Config
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import create_engine

    from .config import settings

    # Derive a synchronous URL for the revision check (Alembic uses sync connections).
    sync_url = settings.database_url.replace("+asyncpg", "").replace(
        "postgresql://", "postgresql+psycopg2://"
    )
    sync_url = sync_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")

    def _current_rev() -> str:
        try:
            engine = create_engine(sync_url)
            with engine.connect() as conn:
                ctx = MigrationContext.configure(conn)
                return ctx.get_current_revision() or "(none)"
        except Exception:
            return "(unknown)"

    before = _current_rev()
    typer.echo(f"Current revision : {before}")
    typer.echo(f"Upgrading to     : {revision}")

    cfg = Config("alembic.ini")
    command.upgrade(cfg, revision)

    after = _current_rev()
    typer.echo(f"New revision     : {after}")
    typer.echo("Migrations complete.")


@app.command()
def status() -> None:
    """Show current system status: database connectivity and migration revision.

    Checks the PostgreSQL connection, reports the active Alembic revision, and
    indicates whether an OpenAI API key is configured for unstructured parsing.
    Exits with code 1 if the database is unreachable.

    Examples:

        $ datapipeline status
    """
    import asyncio as _asyncio

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    from .config import settings

    typer.echo("Checking system status …\n")

    db_ok = False

    async def _check_db() -> str:
        nonlocal db_ok
        try:
            engine = create_async_engine(settings.database_url, pool_pre_ping=True)
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            await engine.dispose()
            db_ok = True
            return "connected"
        except Exception as exc:
            return f"UNREACHABLE — {exc}"

    db_status = _asyncio.run(_check_db())
    typer.echo(f"  Database      : {db_status}")

    # Alembic revision (sync path)
    try:
        from alembic.runtime.migration import MigrationContext
        from sqlalchemy import create_engine

        sync_url = settings.database_url.replace("+asyncpg", "").replace(
            "postgresql://", "postgresql+psycopg2://"
        )
        sync_url = sync_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
        engine = create_engine(sync_url)
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            rev = ctx.get_current_revision() or "(none)"
        typer.echo(f"  Migration rev : {rev}")
    except Exception:
        typer.echo("  Migration rev : (unavailable)")

    openai_status = "configured" if settings.openai_api_key else "not set (text parsing disabled)"
    typer.echo(f"  OpenAI key    : {openai_status}")
    typer.echo(f"  Extraction    : {settings.extraction_model}")
    typer.echo(f"  Embedding     : {settings.embedding_model}")

    if not db_ok:
        raise typer.Exit(code=1)


@app.command("generate-data")
def generate_data(
    output_dir: Path = typer.Option(
        Path("data/raw"),
        help="Directory to write generated sample files.",
        show_default=True,
    ),
    rows: int = typer.Option(
        500,
        help="Number of CSV rows to generate. Other formats scale proportionally.",
        show_default=True,
        min=1,
    ),
    seed: int = typer.Option(42, help="Random seed for reproducibility.", show_default=True),
    no_invalid: bool = typer.Option(
        False,
        "--no-invalid",
        help="Generate only valid rows (no intentional validation failures).",
    ),
) -> None:
    """Generate synthetic sample data files for all supported formats.

    Creates sample files covering CSV, Excel, JSON, NDJSON, XML, ZIP archive, and
    unstructured text. Row counts for non-CSV formats are proportional to --rows:
    Excel 40%, JSON/NDJSON/XML 20% each, ZIP 10%.

    By default ~5% of rows are intentionally invalid (missing order_id or negative
    quantity) to demonstrate validation rejection logging. Use --no-invalid to
    generate fully clean data.

    Examples:

        $ datapipeline generate-data

        $ datapipeline generate-data --rows 1000 --output-dir /tmp/data

        $ datapipeline generate-data --rows 50 --no-invalid --seed 0
    """
    import json
    import xml.etree.ElementTree as ET

    import pandas as pd

    random.seed(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    invalid_rate = 0.0 if no_invalid else 0.05

    excel_rows_n = max(1, int(rows * 0.4))
    small_n = max(1, int(rows * 0.2))
    zip_n = max(1, int(rows * 0.1))

    # CSV
    csv_rows = [_make_row(i, invalid_rate) for i in range(1, rows + 1)]
    pd.DataFrame(csv_rows).to_csv(output_dir / "orders.csv", index=False)
    typer.echo(f"  Created {output_dir}/orders.csv ({len(csv_rows)} rows)")

    # Excel — Excel-style column names
    excel_rows = [
        {
            "Order ID": r["order_id"],
            "Customer": r["customer_name"],
            "country": r["country"],
            "product": r["product"],
            "quantity": r["quantity"],
            "price": r["price"],
            "order_date": r["order_date"],
        }
        for r in [_make_row(i + 1000, invalid_rate) for i in range(excel_rows_n)]
    ]
    pd.DataFrame(excel_rows).to_excel(output_dir / "sales.xlsx", index=False)
    typer.echo(f"  Created {output_dir}/sales.xlsx ({len(excel_rows)} rows)")

    # JSON — camelCase field names
    json_rows = [
        {
            "orderId": r["order_id"],
            "customer": r["customer_name"],
            "country": r["country"],
            "product": r["product"],
            "quantity": r["quantity"],
            "price": r["price"],
            "order_date": r["order_date"],
        }
        for r in [_make_row(i + 2000, invalid_rate) for i in range(small_n)]
    ]
    with open(output_dir / "events.json", "w") as f:
        json.dump(json_rows, f, indent=2)
    typer.echo(f"  Created {output_dir}/events.json ({len(json_rows)} rows)")

    # NDJSON
    ndjson_rows = [_make_row(i + 3000, invalid_rate) for i in range(small_n)]
    with open(output_dir / "stream.ndjson", "w") as f:
        for row in ndjson_rows:
            f.write(
                json.dumps({
                    "orderId": row["order_id"],
                    "customer": row["customer_name"],
                    "country": row["country"],
                    "product": row["product"],
                    "quantity": row["quantity"],
                    "price": row["price"],
                    "order_date": row["order_date"],
                }) + "\n"
            )
    typer.echo(f"  Created {output_dir}/stream.ndjson ({len(ndjson_rows)} rows)")

    # XML — legacy-style
    xml_rows = [_make_row(i + 4000, invalid_rate) for i in range(small_n)]
    root = ET.Element("orders")
    for row in xml_rows:
        order_elem = ET.SubElement(root, "order")
        for key, value in row.items():
            child = ET.SubElement(order_elem, key)
            child.text = value
    ET.indent(root)
    ET.ElementTree(root).write(output_dir / "legacy.xml", encoding="unicode", xml_declaration=True)
    typer.echo(f"  Created {output_dir}/legacy.xml ({len(xml_rows)} rows)")

    # ZIP — archive containing a small CSV and JSON
    zip_csv_rows = [_make_row(i + 5000, invalid_rate) for i in range(zip_n)]
    zip_json_rows = [
        {
            "orderId": r["order_id"],
            "customer": r["customer_name"],
            "country": r["country"],
            "product": r["product"],
            "quantity": r["quantity"],
            "price": r["price"],
            "order_date": r["order_date"],
        }
        for r in [_make_row(i + 6000, invalid_rate) for i in range(zip_n)]
    ]
    tmp_csv = output_dir / "_tmp_archive_orders.csv"
    tmp_json = output_dir / "_tmp_archive_events.json"
    pd.DataFrame(zip_csv_rows).to_csv(tmp_csv, index=False)
    with open(tmp_json, "w") as f:
        json.dump(zip_json_rows, f)
    with zipfile.ZipFile(output_dir / "archive.zip", "w") as zf:
        zf.write(tmp_csv, arcname="archive_orders.csv")
        zf.write(tmp_json, arcname="archive_events.json")
    tmp_csv.unlink()
    tmp_json.unlink()
    typer.echo(f"  Created {output_dir}/archive.zip (CSV + JSON, {zip_n} rows each)")

    # Unstructured TEXT — natural-language order descriptions for AI extraction demo
    txt_rows = [r for r in [_make_row(i + 7000, 0.0) for i in range(10)] if r["order_id"]]
    lines = [
        f"Customer {r['customer_name']} from {r['country']} placed order "
        f"{r['order_id']} for {r['quantity']} unit(s) of {r['product']} "
        f"at ${r['price']} each on {r['order_date']}."
        for r in txt_rows[:10]
    ]
    (output_dir / "unstructured_orders.txt").write_text("\n".join(lines))
    typer.echo(f"  Created {output_dir}/unstructured_orders.txt ({len(lines)} order descriptions)")

    typer.echo(f"\nAll sample data written to {output_dir}/")
