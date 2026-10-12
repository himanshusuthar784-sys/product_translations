from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration comes from environment variables (12-factor, stateless)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url_async: str
    database_url_sync: str

    redis_url: str = "redis://localhost:6379/5"
    celery_queue: str = "si_migration"
    celery_key_prefix: str = "si:"

    # Pools are per process. Worker pool is deliberately tiny: concurrency x pool_size
    # is the container's total connection budget.
    api_db_pool_size: int = 10
    api_db_max_overflow: int = 5
    worker_db_pool_size: int = 2
    worker_db_max_overflow: int = 0
    db_statement_timeout_ms: int = 120_000
    db_lock_timeout_ms: int = 10_000

    default_batch_size: int = 2000
    max_batch_size: int = 20_000
    max_batch_attempts: int = 5            # total tries per batch (survives worker crashes)
    batch_stale_after_seconds: int = 300   # running batch with older heartbeat => worker presumed dead
    batch_redeliver_after_seconds: int = 900  # pending batch not consumed since publish => re-publish
    pending_job_redeliver_after_seconds: int = 60  # job stuck in 'pending' (publish failed) => re-publish
    translate_chunk_size: int = 500
    translation_provider: str = "copy_english"

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
