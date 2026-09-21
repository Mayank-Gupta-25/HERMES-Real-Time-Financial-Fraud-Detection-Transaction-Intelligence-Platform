"""
Module 4/5 — Simulator configuration, loaded from environment variables
(or a local .env file). Keeps the simulator's Kafka target and default
generation parameters out of code, per the project's .env.example convention.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIMULATOR_", env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "hermes.transactions.raw"

    default_tps: int = 1000
    default_fraud_rate: float = 0.05
    num_customers: int = 10_000
    num_merchants: int = 5_000


settings = Settings()
