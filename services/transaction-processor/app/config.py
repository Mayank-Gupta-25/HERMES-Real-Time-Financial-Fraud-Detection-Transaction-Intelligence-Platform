from __future__ import annotations

import os
from pydantic import BaseModel

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
    HAS_PYDANTIC_SETTINGS = True
except ImportError:
    BaseSettings = BaseModel  # type: ignore[misc,assignment]
    SettingsConfigDict = None  # type: ignore[assignment]
    HAS_PYDANTIC_SETTINGS = False


class Settings(BaseSettings):
    # Kafka & Schema Registry
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_schema_registry_url: str = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "flink-enrichment-cg")

    # Canonical Topics (Section 10.C)
    topic_transactions_raw: str = "hermes.transactions.raw"
    topic_transactions_enriched: str = "hermes.transactions.enriched"
    topic_transactions_scored: str = "hermes.transactions.scored"
    topic_transactions_flagged: str = "hermes.transactions.flagged"
    topic_transactions_clean: str = "hermes.transactions.clean"
    topic_alerts_fraud: str = "hermes.alerts.fraud"
    topic_metrics_realtime: str = "hermes.metrics.realtime"
    topic_dlq: str = "hermes.dlq"

    # Redis
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    profile_cache_ttl_sec: int = 60
    dedup_ttl_sec: int = 86400

    # Flink / Streaming Parameters (MOD-02)
    checkpoint_interval_ms: int = 10000
    watermark_max_out_of_orderness_ms: int = 5000
    warmup_min_events: int = 5

    # Downstream Services
    fraud_service_url: str = os.getenv("FRAUD_SERVICE_URL", "http://fraud-detection:8001")

    # JWT
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_expire_minutes: int = 60

    # Service
    service_name: str = "hermes-transaction-processor"
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    if HAS_PYDANTIC_SETTINGS and SettingsConfigDict is not None:
        model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
