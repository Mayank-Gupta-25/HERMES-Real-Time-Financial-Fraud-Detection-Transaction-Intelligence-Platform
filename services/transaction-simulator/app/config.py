"""
Module 4/5 — Simulator configuration, loaded from environment variables
(or a local .env file). Keeps the simulator's Kafka target and default
generation parameters out of code, per the project's .env.example convention.
Provides a clean stdlib/Pydantic BaseModel fallback if pydantic_settings is not installed.
"""
from __future__ import annotations

import os
from typing import Optional

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="SIMULATOR_", env_file=".env", extra="ignore")

        kafka_bootstrap_servers: str = "localhost:9092"
        kafka_topic: str = "hermes.transactions.raw"
        schema_version: str = "1.0"
        serialization_format: str = "json"
        random_seed: Optional[int] = 42

        default_tps: int = 1000
        default_fraud_rate: float = 0.05
        num_customers: int = 10_000
        num_merchants: int = 5_000

except ImportError:
    from pydantic import BaseModel, ConfigDict

    class Settings(BaseModel):  # type: ignore[no-redef]
        model_config = ConfigDict(extra="ignore")

        kafka_bootstrap_servers: str = os.getenv("SIMULATOR_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        kafka_topic: str = os.getenv("SIMULATOR_KAFKA_TOPIC", "hermes.transactions.raw")
        schema_version: str = os.getenv("SIMULATOR_SCHEMA_VERSION", "1.0")
        serialization_format: str = os.getenv("SIMULATOR_SERIALIZATION_FORMAT", "json")
        random_seed: Optional[int] = int(os.getenv("SIMULATOR_RANDOM_SEED", "42"))

        default_tps: int = int(os.getenv("SIMULATOR_DEFAULT_TPS", "1000"))
        default_fraud_rate: float = float(os.getenv("SIMULATOR_DEFAULT_FRAUD_RATE", "0.05"))
        num_customers: int = int(os.getenv("SIMULATOR_NUM_CUSTOMERS", "10000"))
        num_merchants: int = int(os.getenv("SIMULATOR_NUM_MERCHANTS", "5000"))


settings = Settings()
