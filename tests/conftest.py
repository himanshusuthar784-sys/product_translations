"""Test harness.

* Integration tests run against a DEDICATED database `translations_test` on the compose Postgres,
  rebuilt from scratch through Alembic at session start (so the migrations themselves are tested).
  The dev database is never touched: URLs are rewritten before any app module is imported.
* Redis/Celery transport is replaced by an in-process `Broker` that records published messages and
  runs the real task functions on demand. Everything else (SQL, state machine, processors) is real.
"""
import os
import re
from collections import deque

# ---- must happen BEFORE `app` is imported anywhere (Settings is cached) --------------------------
_TEST_DB = "translations_test"


def _swap_db(url: str) -> str:
    return re.sub(r"/[^/?]+(\?.*)?$", rf"/{_TEST_DB}\1", url)


_BASE_SYNC = os.environ.get("DATABASE_URL_SYNC", "")
if _BASE_SYNC:  # no env => integration tests are skipped below
    os.environ["DATABASE_URL_SYNC"] = _swap_db(_BASE_SYNC)
    os.environ["DATABASE_URL_ASYNC"] = _swap_db(os.environ["DATABASE_URL_ASYNC"])
os.environ.setdefault("LOG_LEVEL", "WARNING")
# unit tests only need *valid-looking* settings; integration tests are skipped when no real DB is configured
os.environ.setdefault("DATABASE_URL_SYNC", "postgresql+psycopg://u:p@localhost:5432/unit_dummy")
os.environ.setdefault("DATABASE_URL_ASYNC", "postgresql+asyncpg://u:p@localhost:5432/unit_dummy")

import psycopg  # noqa: E402
import pytest  # noqa: E402
from sqlalchemy import text  # noqa: E402


def _admin_dsn() -> str:
    return re.sub(r"/[^/?]+(\?.*)?$", "/postgres", _BASE_SYNC).replace("postgresql+psycopg://", "postgresql://")


def _db_available() -> bool:
    if not _BASE_SYNC:
        return False
    try:
        with psycopg.connect(_admin_dsn(), connect_timeout=3):
            return True
    except Exception:  # noqa: BLE001
        return False


requires_db = pytest.mark.skipif(not _db_available(), reason="Postgres (DATABASE_URL_SYNC) not reachable")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/integration" in item.nodeid.replace("\\", "/") or "tests\\integration" in item.nodeid:
            item.add_marker(requires_db)


# ------------------------------------------------------------------------------------ database
@pytest.fixture(scope="session")
def _test_database():
    from app.core.config import get_settings

    assert get_settings().database_url_sync.rstrip("/").endswith(_TEST_DB), "refusing to run against a non-test DB"
    with psycopg.connect(_admin_dsn(), autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {_TEST_DB} WITH (FORCE)")
        c.execute(f"CREATE DATABASE {_TEST_DB}")

    from alembic import command
    from alembic.config import Config

    command.upgrade(Config("alembic.ini"), "head")  # the real migrations build the test schema
    from scripts import load_legacy

    with psycopg.connect(get_settings().database_url_sync.replace("postgresql+psycopg://", "postgresql://")) as c:
        c.execute(load_legacy.DDL)
    yield _TEST_DB


@pytest.fixture(scope="session")
def engine(_test_database):
    from app.core.db import get_sync_engine

    eng = get_sync_engine()
    yield eng
    eng.dispose()


@pytest.fixture
def wide(engine):
    """A wide pool so N threads really hit Postgres at the same instant (the app's worker pool is 2 by design)."""
    from sqlalchemy import create_engine

    from app.core.config import get_settings

    eng = create_engine(get_settings().database_url_sync, pool_size=24, max_overflow=0)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine):
    """Clean slate for every test: data tables emptied, language seed restored."""
    with engine.begin() as c:
        c.execute(
            text(
                "TRUNCATE product_translations, products, migration_batches, migration_jobs, "
                "legacy.shop_products_1001_test RESTART IDENTITY CASCADE"
            )
        )
        c.execute(text("DELETE FROM languages WHERE language_code NOT IN ('en','fr','bg')"))
        c.execute(text("UPDATE languages SET is_active = true"))
    yield engine


# ------------------------------------------------------------------------------ in-process broker
class Broker:
    """Records what would be published to Redis and runs the real tasks, like a worker would."""

    def __init__(self):
        self.plans: deque[str] = deque()
        self.batches: deque[int] = deque()
        self.published_batch_ids: list[int] = []
        self.fail_publish = False

    # patched into dispatch / tasks
    def publish_plan(self, job_id):
        if self.fail_publish:
            raise ConnectionError("redis down")
        self.plans.append(str(job_id))

    def publish_batches(self, ids):
        if self.fail_publish:
            raise ConnectionError("redis down")
        ids = list(ids)
        self.batches.extend(ids)
        self.published_batch_ids.extend(ids)
        return len(ids)

    def drain(self, max_messages: int = 10_000) -> int:
        """Consume messages like a worker. An exception (what `retry` raises when a task is called
        directly) re-queues the message, mimicking Celery's redelivery."""
        from app.workers import tasks

        handled = 0
        while (self.plans or self.batches) and handled < max_messages:
            handled += 1
            if self.plans:
                tasks.plan_job(self.plans.popleft())
                continue
            bid = self.batches.popleft()
            try:
                tasks.process_batch(bid)
            except Exception:  # noqa: BLE001
                self.batches.append(bid)
        return handled


@pytest.fixture
def broker(monkeypatch):
    b = Broker()
    import app.workers.dispatch as dispatch
    import app.workers.tasks as tasks

    monkeypatch.setattr(dispatch, "publish_plan", b.publish_plan)
    monkeypatch.setattr(dispatch, "publish_batches", b.publish_batches)
    monkeypatch.setattr(tasks, "publish_batches", b.publish_batches)
    # keep backoff instant so retries don't slow the suite
    import app.services.job_runtime as rt

    monkeypatch.setattr(rt, "backoff_seconds", lambda attempt: 0.0)
    return b


# ------------------------------------------------------------------------------------- API client
@pytest.fixture
def client(db, broker):
    from fastapi.testclient import TestClient

    from app.api.main import app

    with TestClient(app) as c:  # runs lifespan: creates the async engine
        yield c


# -------------------------------------------------------------------------------------- helpers
@pytest.fixture
def lang_id(db):
    def _get(code: str) -> int:
        with db.connect() as c:
            return c.execute(text("SELECT id FROM languages WHERE language_code = :c"), {"c": code}).scalar_one()

    return _get
