from __future__ import annotations

import os
from pathlib import Path

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore

    _HAS_PYDANTIC_SETTINGS = True
except ImportError:
    from pydantic import BaseModel as BaseSettings  # type: ignore

    SettingsConfigDict = None  # type: ignore
    _HAS_PYDANTIC_SETTINGS = False


ROOT_DIR = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    # Service
    service_name: str = os.getenv("SERVICE_NAME", "hermes-analytics-service")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # PostgreSQL (Operational Store)
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "hermes_ops")
    postgres_user: str = os.getenv("POSTGRES_USER", "hermes")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "hermes_dev_pass")

    # TimescaleDB (Time-Series Store)
    timescale_host: str = os.getenv("TIMESCALE_HOST", "timescaledb")
    timescale_port: int = int(os.getenv("TIMESCALE_PORT", "5432"))
    timescale_db: str = os.getenv("TIMESCALE_DB", "hermes_metrics")
    timescale_user: str = os.getenv("TIMESCALE_USER", "hermes")
    timescale_password: str = os.getenv("TIMESCALE_PASSWORD", "hermes_dev_pass")

    # Redis (Hot Feature Store & Query Cache)
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

    # MinIO / Parquet Cold Archive & ML Metrics
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "hermes-archive")
    parquet_archive_dir: str = os.getenv(
        "PARQUET_ARCHIVE_DIR",
        str(ROOT_DIR / "data" / "parquet_archive"),
    )
    ml_metrics_path: str = os.getenv(
        "ML_METRICS_PATH",
        str(ROOT_DIR / "ml" / "models" / "v1" / "metrics.json"),
    )

    # Cache TTLs
    live_cache_ttl_seconds: int = int(os.getenv("LIVE_CACHE_TTL_SECONDS", "15"))
    historical_cache_ttl_seconds: int = int(os.getenv("HISTORICAL_CACHE_TTL_SECONDS", "120"))

    if _HAS_PYDANTIC_SETTINGS and SettingsConfigDict is not None:
        model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
