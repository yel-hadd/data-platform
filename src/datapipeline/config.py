"""Application configuration loaded from environment variables or .env file."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central settings object; values are read from env vars or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = (
        "postgresql+asyncpg://pipeline:pipeline@localhost:5432/dataplatform"
    )
    log_level: str = "INFO"
    data_dir: Path = Path("data/raw")
    exports_dir: Path = Path("data/exports")
    chunk_size: int = 10_000
    openai_api_key: str | None = None
    embedding_model: str = "text-embedding-3-small"
    extraction_model: str = "openai:gpt-4o-mini"


settings = Settings()
