FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# Create a non-root user whose UID/GID match the typical host developer (1000:1000).
# This prevents mounted-volume files from being created as root on the host.
ARG UID=1000
ARG GID=1000
RUN groupadd --gid "${GID}" appuser \
 && useradd --uid "${UID}" --gid "${GID}" --create-home appuser

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --frozen

COPY src/ src/
COPY sql/ sql/
COPY alembic/ alembic/
COPY alembic.ini ./

# Pre-create data directories so volume mounts land with correct ownership.
RUN mkdir -p data/raw data/exports data/processed \
 && chown -R appuser:appuser /app

USER appuser

CMD ["uv", "run", "python", "-m", "datapipeline", "--help"]
