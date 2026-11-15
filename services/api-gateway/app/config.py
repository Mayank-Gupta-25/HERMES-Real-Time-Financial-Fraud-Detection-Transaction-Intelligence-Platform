import os
from pydantic import ConfigDict

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:  # Fallback if pydantic-settings is not installed in host Python
    from pydantic import BaseModel as BaseSettings  # type: ignore[misc,assignment]

    def SettingsConfigDict(**kwargs):  # type: ignore[misc]
        return ConfigDict(**{k: v for k, v in kwargs.items() if k in ("extra", "populate_by_name")})


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_schema_registry_url: str = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    kafka_transactions_topic: str = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "hermes.transactions.raw")
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "hermes.dlq")

    # Redis
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

    # JWT & Auth
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))
    auth_enabled: bool = os.getenv("AUTH_ENABLED", "true").lower() in ("1", "true", "yes")

    # Rate Limiting
    rate_limit_enabled: bool = os.getenv("RATE_LIMIT_ENABLED", "true").lower() in ("1", "true", "yes")
    rate_limit_per_minute: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "1000"))

    # Service
    service_name: str = os.getenv("SERVICE_NAME", "hermes-api-gateway")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()

