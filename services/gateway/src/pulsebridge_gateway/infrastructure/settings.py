from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PULSEBRIDGE_", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    stream_key: str = "pulsebridge.events"
    rate_limit_rps: int = 100
    idempotency_ttl_seconds: int = 86_400
    log_level: str = "INFO"
    use_memory: bool = False
    service_name: str = "pulsebridge-gateway"


def get_settings() -> Settings:
    return Settings()
