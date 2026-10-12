"""Publishing helpers. Tasks are addressed by *name* so the API process never imports task code
(no import cycle with tasks.py, and the API stays light)."""
from collections.abc import Iterable

from app.core.config import get_settings
from app.workers.celery_app import celery_app

PLAN_TASK = "app.workers.tasks.plan_job"
BATCH_TASK = "app.workers.tasks.process_batch"
REAP_TASK = "app.workers.tasks.reap"


def publish_plan(job_id: str) -> None:
    celery_app.send_task(PLAN_TASK, args=[str(job_id)], queue=get_settings().celery_queue)


def publish_batches(batch_ids: Iterable[int]) -> int:
    queue = get_settings().celery_queue
    n = 0
    with celery_app.producer_pool.acquire(block=True) as producer:
        for bid in batch_ids:
            celery_app.send_task(BATCH_TASK, args=[int(bid)], queue=queue, producer=producer)
            n += 1
    return n
