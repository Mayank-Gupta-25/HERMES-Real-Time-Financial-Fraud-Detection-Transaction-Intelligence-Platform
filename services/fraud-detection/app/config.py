from __future__ import annotations

import os
from pathlib import Path
from pydantic import ConfigDict

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:  # Fallback if pydantic-settings is not installed in host Python
    from pydantic import BaseModel as BaseSettings  # type: ignore[misc,assignment]

    def SettingsConfigDict(**kwargs):  # type: ignore[misc]
        return ConfigDict(**{k: v for k, v in kwargs.items() if k in ("extra", "populate_by_name")})


_SERVICE_ROOT = Path(__file__).resolve().parent.parent
_REPO_ROOT = _SERVICE_ROOT.parent.parent


def _resolve_default_path(rel_path: str) -> str:
    cwd_candidate = Path(rel_path)
    if cwd_candidate.exists():
        return str(cwd_candidate)
    service_candidate = _SERVICE_ROOT / rel_path
    if service_candidate.exists():
        return str(service_candidate)
    repo_candidate = _REPO_ROOT / rel_path
    if repo_candidate.exists():
        return str(repo_candidate)
    # Default to service-relative for app/* and repo-relative for ml/*
    if rel_path.startswith("ml/") or rel_path.startswith("ml\\"):
        return str(_REPO_ROOT / rel_path)
    return str(_SERVICE_ROOT / rel_path)


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_schema_registry_url: str = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    kafka_scored_topic: str = os.getenv("KAFKA_SCORED_TOPIC", "hermes.transactions.scored")
    kafka_flagged_topic: str = os.getenv("KAFKA_FLAGGED_TOPIC", "hermes.transactions.flagged")
    kafka_clean_topic: str = os.getenv("KAFKA_CLEAN_TOPIC", "hermes.transactions.clean")
    kafka_alerts_topic: str = os.getenv("KAFKA_ALERTS_TOPIC", "hermes.alerts.fraud")

    # Redis
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

    # JWT
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))

    # Rules & Models
    rule_version: str = os.getenv("RULE_VERSION", "rules-v1.0")
    model_version: str = os.getenv("MODEL_VERSION", "ensemble-v1.0.0")
    shadow_model_version: str = os.getenv("SHADOW_MODEL_VERSION", "heuristic-shadow-v1.0.0")
    transformation_version: str = os.getenv("TRANSFORMATION_VERSION", "features-v1.0.0")
    rules_path: str = os.getenv("RULES_PATH", _resolve_default_path("app/rules/default_rules.yaml"))
    scoring_weights_path: str = os.getenv(
        "SCORING_WEIGHTS_PATH", _resolve_default_path("app/rules/scoring_weights.json")
    )
    model_path: str = os.getenv("MODEL_PATH", _resolve_default_path("ml/models/v1"))
    warmup_min_events: int = int(os.getenv("WARMUP_MIN_EVENTS", "5"))

    # Service
    service_name: str = os.getenv("SERVICE_NAME", "hermes-fraud-detection")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
