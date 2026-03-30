"""FastAPI application factory with lifespan DB initialisation."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..logging_config import configure_logging
from ..storage.database import init_db
from .routes import health, metrics


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run startup tasks (DB init) before accepting requests."""
    configure_logging()
    await init_db()
    yield


app = FastAPI(
    title="Data Platform API",
    description="Read-only analytics API for the advanced data processing platform.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(metrics.router, prefix="/metrics")
