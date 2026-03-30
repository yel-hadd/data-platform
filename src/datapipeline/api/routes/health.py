"""Health check endpoint."""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """Response schema for the health endpoint."""

    status: str


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Return service health status.

    Returns:
        JSON object with status 'ok' when the service is running.
    """
    return HealthResponse(status="ok")
