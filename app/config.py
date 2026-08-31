"""Application configuration.

All settings are overridable via environment variables (case-insensitive
field names, e.g. ``MONGO_URI``) or an optional ``.env`` file.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "event-platform"
    log_level: str = "INFO"

    # --- Stores ---
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "event_platform"
    es_url: str = "http://localhost:9200"
    es_index: str = "events"
    redis_url: str = "redis://localhost:6379/0"

    # Store timeouts: bounded waits are what make the documented fail-open /
    # degraded paths reachable — a hung store must become an error, not a hang.
    mongo_server_selection_timeout_ms: int = 5000
    es_request_timeout_seconds: float = 10.0
    redis_socket_timeout_seconds: float = 2.0

    # --- Queue / worker ---
    queue_max_size: int = 10_000
    visibility_timeout_seconds: float = 30.0
    worker_concurrency: int = 2
    max_retries: int = 5
    backoff_base_seconds: float = 0.5
    backoff_max_seconds: float = 30.0
    dlq_max_size: int = 1_000

    # --- Deduplication ---
    dedup_ttl_seconds: int = 3600

    # --- Realtime stats cache ---
    realtime_window_minutes: int = 5
    realtime_ttl_seconds: int = 10
    realtime_max_ttl_seconds: int = 300

    # --- Rate limiting (POST /events) ---
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 120
    rate_limit_window_seconds: int = 60

    # --- Query bounds ---
    default_page_size: int = 50
    max_page_size: int = 500
    stats_max_buckets: int = 1000
    search_max_results: int = 100

    # --- Startup ---
    startup_connect_retries: int = 15
    startup_connect_delay_seconds: float = 2.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
