"""Celery application.

Design choices (all deliberate):
* Postgres is the source of truth for job/batch state  -> no result backend, `task_ignore_result`.
* `acks_late` + `reject_on_worker_lost`               -> a message is only removed after the task finished;
                                                        a killed worker's message is redelivered.
* `prefetch_multiplier=1`                             -> fair distribution, no hoarding of batches.
* Redis is shared with another project: dedicated DB index (from REDIS_URL), a namespaced queue and a
  global key prefix, so nothing collides.
"""
from celery import Celery
from celery.signals import setup_logging, worker_process_init, worker_process_shutdown

from app.core.config import get_settings
from app.core.db import dispose_sync_engine
from app.core.logging import configure_logging

settings = get_settings()

celery_app = Celery("si", broker=settings.redis_url, include=["app.workers.tasks"])

celery_app.conf.update(
    task_default_queue=settings.celery_queue,
    task_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=540,
    task_time_limit=600,
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "global_keyprefix": settings.celery_key_prefix,
        # must exceed the longest task, otherwise Redis redelivers a message that is still running
        "visibility_timeout": 3600,
        "socket_keepalive": True,
    },
    # Logging is owned by app.core.logging (JSON to stdout). Celery must neither hijack the root logger nor
    # redirect stdout into its own logger - that proxy is recursion-protected and silently swallowed our lines.
    worker_hijack_root_logger=False,
    worker_redirect_stdouts=False,
    beat_schedule={
        "reaper": {"task": "app.workers.tasks.reap", "schedule": 60.0, "options": {"expires": 55}},
    },
)


@setup_logging.connect
def _setup_logging(**_):
    configure_logging()  # connecting this signal tells Celery to skip its own logging setup (main + children + beat)


@worker_process_init.connect
def _init_worker_process(**_):
    configure_logging()  # belt and braces for each prefork child; the DB engine itself is created lazily, per pid


@worker_process_shutdown.connect
def _shutdown_worker_process(**_):
    dispose_sync_engine()
