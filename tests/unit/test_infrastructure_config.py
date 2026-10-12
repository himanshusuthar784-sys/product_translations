"""Connection-pool settings, per-process engine handling, JSON logging and Celery reliability config."""
import json
import os

import structlog

from app.core import db as dbmod
from app.core.config import get_settings
from app.core.logging import configure_logging


def test_worker_engine_pool_is_tiny_and_bounded():
    eng = dbmod.make_sync_engine()
    try:
        s = get_settings()
        assert eng.pool.size() == s.worker_db_pool_size == 2
        assert eng.pool._max_overflow == s.worker_db_max_overflow == 0  # hard ceiling: 2 conns per process
        assert eng.pool._pre_ping is True
    finally:
        eng.dispose()


def test_api_engine_pool_settings():
    eng = dbmod.make_async_engine()
    s = get_settings()
    assert eng.pool.size() == s.api_db_pool_size
    assert eng.pool._max_overflow == s.api_db_max_overflow
    assert eng.pool._pre_ping is True


def test_sync_engine_is_cached_per_process_and_rebuilt_after_fork(monkeypatch):
    dbmod.dispose_sync_engine()
    e1 = dbmod.get_sync_engine()
    assert dbmod.get_sync_engine() is e1                      # same process -> same engine/pool
    monkeypatch.setattr(os, "getpid", lambda: 424242)          # simulate a forked child process
    e2 = dbmod.get_sync_engine()
    assert e2 is not e1                                        # "forked child" must never reuse the parent's pool
    dbmod.dispose_sync_engine()


def test_dispose_resets_engine():
    dbmod.dispose_sync_engine()
    e1 = dbmod.get_sync_engine()
    dbmod.dispose_sync_engine()
    assert dbmod.get_sync_engine() is not e1
    dbmod.dispose_sync_engine()


def test_json_logging_carries_bound_context(capsys):
    configure_logging()
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(job_id="J1", batch_id=7, language="fr", attempt=2)
    structlog.get_logger("t").info("batch_started", range_start=1)
    structlog.contextvars.clear_contextvars()
    line = [ln for ln in capsys.readouterr().out.splitlines() if "batch_started" in ln][-1]
    rec = json.loads(line)
    assert rec["event"] == "batch_started" and rec["level"] == "info"
    assert (rec["job_id"], rec["batch_id"], rec["language"], rec["attempt"], rec["range_start"]) == ("J1", 7, "fr", 2, 1)
    assert rec["timestamp"].endswith("Z")


def test_celery_is_configured_for_at_least_once_and_stateless_workers():
    from app.workers.celery_app import celery_app

    c = celery_app.conf
    assert c.task_acks_late is True
    assert c.task_reject_on_worker_lost is True
    assert c.worker_prefetch_multiplier == 1
    assert c.task_ignore_result is True                       # state lives in Postgres, not in Redis
    assert c.task_default_queue == get_settings().celery_queue
    opts = c.broker_transport_options
    assert opts["global_keyprefix"] == get_settings().celery_key_prefix
    assert opts["visibility_timeout"] > c.task_time_limit      # else Redis redelivers running tasks
    assert c.task_soft_time_limit < c.task_time_limit
    assert "reaper" in c.beat_schedule


def test_celery_does_not_swallow_our_json_logs():
    """Regression (found in a live kill-a-worker drill): with Celery's default stdout redirection, log lines
    written by task processes vanished. We own logging; Celery must not redirect or hijack it."""
    from celery.signals import setup_logging

    from app.workers.celery_app import celery_app

    assert celery_app.conf.worker_redirect_stdouts is False
    assert celery_app.conf.worker_hijack_root_logger is False
    # a connected receiver makes Celery skip its own logging setup, so ours (JSON) is the only one
    assert setup_logging.receivers, "setup_logging signal must be connected"


def test_task_names_are_registered():
    import app.workers.tasks  # noqa: F401
    from app.workers.celery_app import celery_app
    from app.workers.dispatch import BATCH_TASK, PLAN_TASK, REAP_TASK

    for name in (PLAN_TASK, BATCH_TASK, REAP_TASK):
        assert name in celery_app.tasks
