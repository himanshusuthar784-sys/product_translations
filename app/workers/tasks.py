"""Celery tasks: thin wrappers around app.services.job_runtime (which holds the logic and is unit-testable)."""
import time

import structlog
from celery.exceptions import SoftTimeLimitExceeded

from app.core.db import get_sync_engine
from app.core.logging import get_logger
from app.services import job_runtime as rt
from app.workers.celery_app import celery_app
from app.workers.dispatch import publish_batches
from app.workers.errors import LostLockError
from app.workers.processors import BatchContext, get_processor

log = get_logger(__name__)


@celery_app.task(name="app.workers.tasks.plan_job", bind=True, max_retries=5)
def plan_job(self, job_id: str) -> dict:
    structlog.contextvars.bind_contextvars(job_id=job_id, task_id=self.request.id)
    engine = get_sync_engine()
    try:
        batch_ids = rt.plan_job(engine, job_id)
        n = publish_batches(batch_ids)
        rt.mark_enqueued(engine, batch_ids)
        return {"job_id": job_id, "published": n}
    except Exception as exc:  # noqa: BLE001
        log.error("plan_failed", error=str(exc), retries=self.request.retries)
        if self.request.retries >= self.max_retries:
            with engine.begin() as conn:
                conn.exec_driver_sql(
                    "UPDATE migration_jobs SET status = 'failed', error = %s, finished_at = now(), updated_at = now() "
                    "WHERE id = %s AND status IN ('pending', 'planning')",
                    (f"planning failed: {type(exc).__name__}: {exc}"[:2000], job_id),
                )
            raise
        raise self.retry(exc=exc, countdown=rt.backoff_seconds(self.request.retries + 1)) from exc
    finally:
        structlog.contextvars.clear_contextvars()


@celery_app.task(name="app.workers.tasks.process_batch", bind=True, max_retries=None)
def process_batch(self, batch_id: int) -> dict:
    me = rt.worker_id()
    structlog.contextvars.bind_contextvars(batch_id=batch_id, task_id=self.request.id, worker=me)
    engine = get_sync_engine()
    started = time.monotonic()
    job_id: str | None = None
    try:
        with engine.begin() as conn:
            claim = rt.claim_batch(conn, batch_id, me)
        if claim is None:
            log.info("batch_skipped", reason="not claimable (done, owned, or job not running)")
            return {"batch_id": batch_id, "status": "skipped"}
        job_id = claim.job_id
        structlog.contextvars.bind_contextvars(
            job_id=job_id, language=claim.language_code, attempt=claim.attempts, job_type=claim.job_type.value
        )
        log.info("batch_started", range_start=claim.range_start, range_end=claim.range_end)

        ctx = BatchContext(
            batch_id=batch_id,
            job_id=job_id,
            range_start=claim.range_start,
            range_end=claim.range_end,
            language_id=claim.language_id,
            language_code=claim.language_code,
            source_language_id=claim.source_language_id,
            source_language_code=claim.source_language_code,
            fields=list(claim.fields),
            provider=claim.provider,
            heartbeat=lambda: rt.heartbeat(engine, batch_id, me),
        )
        # ONE transaction: data + progress commit together or not at all.
        with engine.begin() as conn:
            processed = get_processor(claim.job_type).process(conn, ctx)
            rt.mark_succeeded(conn, batch_id, me, processed)

        rt.finalize_job_tx(engine, job_id)
        log.info("batch_succeeded", processed=processed, seconds=round(time.monotonic() - started, 3))
        return {"batch_id": batch_id, "status": "succeeded", "processed": processed}

    except LostLockError as exc:
        log.warning("batch_lost_lock", error=str(exc))  # someone else owns it now; no state change
        return {"batch_id": batch_id, "status": "lost_lock"}

    except (Exception, SoftTimeLimitExceeded) as exc:  # noqa: BLE001
        outcome = rt.release_or_fail(engine, batch_id, me, exc)
        if outcome is None:
            log.warning("batch_failure_ignored", reason="lock no longer held", error=str(exc))
            return {"batch_id": batch_id, "status": "lost_lock"}
        new_status, attempts, jid = outcome
        if new_status == "pending":
            delay = rt.backoff_seconds(attempts)
            log.warning("batch_retry", error=f"{type(exc).__name__}: {exc}", attempts=attempts, retry_in=round(delay, 1))
            raise self.retry(exc=exc, countdown=delay) from exc
        log.error("batch_failed", error=f"{type(exc).__name__}: {exc}", attempts=attempts, exc_info=True)
        rt.finalize_job_tx(engine, jid)  # may complete the job with errors
        return {"batch_id": batch_id, "status": "failed"}
    finally:
        structlog.contextvars.clear_contextvars()


@celery_app.task(name="app.workers.tasks.reap")
def reap() -> dict:
    """Beat-driven self-healing sweep (see job_runtime.reap)."""
    engine = get_sync_engine()
    res = rt.reap(engine)
    if res.republish_batches:
        publish_batches(res.republish_batches)
        rt.mark_enqueued(engine, res.republish_batches)
    if res.republish_jobs:
        from app.workers.dispatch import publish_plan

        for jid in res.republish_jobs:
            publish_plan(jid)
    if res.ran and (res.stale_requeued or res.stale_failed or res.republish_batches or res.republish_jobs or res.finalized):
        log.warning(
            "reaper_repaired",
            stale_requeued=res.stale_requeued,
            stale_failed=res.stale_failed,
            republished_batches=len(res.republish_batches),
            republished_jobs=len(res.republish_jobs),
            finalized_jobs=res.finalized,
        )
    return {
        "ran": res.ran,
        "stale_requeued": res.stale_requeued,
        "stale_failed": res.stale_failed,
        "republished_batches": len(res.republish_batches or []),
        "finalized": res.finalized,
    }
