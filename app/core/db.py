"""Database engines.

API     : async SQLAlchemy + asyncpg, one pooled engine per process (created at startup).
Workers : sync SQLAlchemy + psycopg3, one *tiny* pooled engine per forked child process.
          Engines must never cross a fork, so they are created lazily, per-pid.
"""
import os
import threading

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

APP_NAME_API = "si-api"
APP_NAME_WORKER = "si-worker"


# ----------------------------------------------------------------- async (FastAPI)
def make_async_engine() -> AsyncEngine:
    s = get_settings()
    return create_async_engine(
        s.database_url_async,
        pool_size=s.api_db_pool_size,
        max_overflow=s.api_db_max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=10,
        connect_args={
            "server_settings": {
                "application_name": APP_NAME_API,
                "statement_timeout": str(s.db_statement_timeout_ms),
                "lock_timeout": str(s.db_lock_timeout_ms),
            }
        },
    )


def make_async_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


# ------------------------------------------------------------- sync (Celery / scripts)
_engine: Engine | None = None
_engine_pid: int | None = None
_lock = threading.Lock()


def make_sync_engine(app_name: str = APP_NAME_WORKER) -> Engine:
    s = get_settings()
    return create_engine(
        s.database_url_sync,
        pool_size=s.worker_db_pool_size,
        max_overflow=s.worker_db_max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
        connect_args={
            "application_name": app_name,
            "options": (
                f"-c statement_timeout={s.db_statement_timeout_ms} "
                f"-c lock_timeout={s.db_lock_timeout_ms}"
            ),
        },
    )


def get_sync_engine() -> Engine:
    """Process-local engine: recreated if we find ourselves in a forked child."""
    global _engine, _engine_pid
    pid = os.getpid()
    with _lock:
        if _engine is None or _engine_pid != pid:
            _engine = make_sync_engine()
            _engine_pid = pid
        return _engine


def dispose_sync_engine() -> None:
    global _engine
    with _lock:
        if _engine is not None:
            _engine.dispose()
            _engine = None


def sync_session() -> Session:
    return sessionmaker(get_sync_engine(), expire_on_commit=False)()
