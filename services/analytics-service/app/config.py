from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_schema_registry_url: str = "http://schema-registry:8081"
    
    # Redis
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    
    # JWT
    jwt_secret_key: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60
    
    # Service
    service_name: str = "hermes-analytics-service"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"

settings = Settings()
