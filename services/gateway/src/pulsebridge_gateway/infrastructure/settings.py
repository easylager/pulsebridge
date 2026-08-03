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

    # Trimming the stream bounds Redis memory regardless of consumer health. An
    # exact trim would scan the whole stream on every write, so this is enforced
    # approximately at radix-node boundaries.
    stream_max_len: int = 1_000_000

    # Redis connections are the gateway's scarcest resource under fan-in: every
    # request needs one for the rate-limit script, the idempotency SET NX and the
    # XADD. Capping the pool makes saturation show up as latency rather than as
    # connection-refused errors from Redis.
    redis_max_connections: int = 100
    # How long a request may wait for a free connection before giving up. Long
    # enough to ride out a burst, short enough that overload surfaces as an
    # error the caller can retry rather than as an unbounded queue.
    redis_pool_timeout_seconds: float = 2.0
    redis_socket_timeout_seconds: float = 2.0
    redis_connect_timeout_seconds: float = 2.0


def get_settings() -> Settings:
    return Settings()
