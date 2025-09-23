from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Read .env and allow extras (so future keys don’t crash)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",          # <- ignore unknown keys instead of raising
    )

    # ---- DB / scraping you already had ----
    DATABASE_URL: str = Field(default="postgresql+asyncpg://zain:Zain2025!!!@localhost:5432/chemdb")
    MAX_CONCURRENCY: int = Field(default=5)
    RATE_LIMIT_RPS: float = Field(default=2.0)
    REQUEST_TIMEOUT: float = Field(default=20.0)
    MAX_RETRIES: int = Field(default=3)
    RETRY_BACKOFF_BASE: float = Field(default=0.75)

    # ---- Add fields that exist in your .env ----
    SECRET_KEY: str | None = Field(default=None, alias="secret_key")
    REDIS_URL: str | None = Field(default=None, alias="redis_url")
    PUBCHEM_RATE_LIMIT: float | None = Field(default=None, alias="pubchem_rate_limit")
    DEBUG: bool | None = Field(default=None, alias="debug")
    TESSERACT_PATH: str | None = Field(default=None, alias="tesseract_path")

    # Tip: if you want to keep both env names and uppercase field names usable:
    # use alias as above; you can then access settings.SECRET_KEY, etc.

settings = Settings()
