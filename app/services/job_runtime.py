"""Worker-side job engine (sync). All state transitions are single atomic SQL statements so
any number of workers/beat instances can run concurrently without coordination.

State machines
  job   : pending -> planning -> running -> completed | completed_with_errors | failed
                 \\-> cancelled (from any active state)
  batch : pending -> running -> succeeded | failed          (running -> pending on retry/reap)
"""
import random
import socket
import os
from dataclasses import dataclass

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

from app.core.config import get_settings
from app.core.logging import get_logger
from app.models.enums import JobType
from app.services.fields import LEGACY_TABLE
from app.workers.errors import LostLockError, TransientError

log = get_logger(__name__)

SOURCE_TABLE = {JobType.IMPORT_BASE: LEGACY_TABLE, JobType.TRANSLATE: "products"}
REAPER_LOCK_KEY = 0x5349_0001  # 'SI' + 1


def worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def is_transient(exc: BaseException) -> bool:
    """Connection loss, deadlocks, serialization failures, lock/statement timeouts, provider throttling."""
    if isinstance(exc, (TransientError, OperationalError, InterfaceError)):
        return True
    return isinstance(exc, DBAPIError) and bool(exc.connection_invalidated)


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter: ~5s, 10s, 20s ... capped at 5 min."""
    return min(300.0, 5.0 * (2 ** max(0, attempt - 1))) * random.uniform(0.5, 1.5)


# ------------------------------------------------------------------------------ planning
_MARK_PLANNING = text(
    """
    UPDATE migration_jobs SET status = 'planning', started_at = COALESCE(started_at, now()), updated_at = now()
    WHERE id = :id AND status IN ('pending', 'planning')
    RETURNING job_type, batch_size
    """
)
_FINISH_PLANNING = text(
    """
    UPDATE migration_jobs
       SET status = 'running', total_records = :records, total_batches = :batches, updated_at = now()
     WHERE id = :id AND status = 'planning'
    """
)
_INSERT_BATCHES = text(
    """
    INSERT INTO migration_batches (job_id, batch_no, range_start, range_end, expected_count)
    SELECT CAST(:job_id AS uuid), t.n, t.s, t.e, t.c
    FROM unnest(CAST(:nos AS integer[]), CAST(:starts AS bigint[]), CAST(:ends AS bigint[]), CAST(:counts AS integer[]))
         AS t(n, s, e, c)
    ON CONFLICT (job_id, batch_no) DO NOTHING
    """
)


def plan_ranges(conn: Connection, job_type: JobType, batch_size: int) -> list[tuple[int, int, int, int]]:
    """Keyset ranges in ONE ordered pass over the PK index (no OFFSET): (batch_no, first_id, last_id, count)."""
    table = SOURCE_TABLE[job_type]  # constant mapping, not user input
    rows = conn.execute(
        text(
            f"""
            SELECT (rn - 1) / :bs AS batch_no, min(id) AS s, max(id) AS e, count(*) AS c
            FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM {table}) x
            GROUP BY 1 ORDER BY 1
            """
        ),
        {"bs": batch_size},
    ).all()
    return [(r.batch_no, r.s, r.e, r.c) for r in rows]


def plan_job(engine: Engine, job_id: str) -> list[int]:
    """Idempotent. Returns ids of batches that must be published (empty if nothing to do)."""
    with engine.begin() as conn:
        claimed = conn.execute(_MARK_PLANNING, {"id": job_id}).first()
        if claimed is None:
            log.info("plan_skipped", job_id=job_id, reason="job not pending/planning")
            return []
        ranges = plan_ranges(conn, claimed.job_type, claimed.batch_size)
        if ranges:
            conn.execute(
                _INSERT_BATCHES,
                {
                    "job_id": job_id,
                    "nos": [r[0] for r in ranges],
                    "starts": [r[1] for r in ranges],
                    "ends": [r[2] for r in ranges],
                    "counts": [r[3] for r in ranges],
                },
            )
        conn.execute(_FINISH_PLANNING, {"id": job_id, "records": sum(r[3] for r in ranges), "batches": len(ranges)})
        if not ranges:
            finalize_job(conn, job_id)  # nothing to migrate -> completed immediately
            return []
        ids = conn.execute(
            text("SELECT id FROM migration_batches WHERE job_id = :j AND status = 'pending' ORDER BY batch_no"),
            {"j": job_id},
        ).scalars().all()
    log.info("job_planned", job_id=job_id, batches=len(ids), records=sum(r[3] for r in ranges))
    return list(ids)


def mark_enqueued(engine: Engine, batch_ids: list[int]) -> None:
    if batch_ids:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE migration_batches SET enqueued_at = now() WHERE id = ANY(CAST(:ids AS bigint[]))"),
                {"ids": batch_ids},
            )


# -------------------------------------------------------------------------------- claim
@dataclass(frozen=True)
class Claim:
    batch_id: int
    job_id: str
    range_start: int
    range_end: int
    attempts: int
    language_id: int
    language_code: str
    source_language_id: int | None
    source_language_code: str | None
    job_type: JobType
    fields: list[str]
    provider: str | None


_CLAIM = text(
    """
    UPDATE migration_batches b
       SET status = 'running', attempts = b.attempts + 1, locked_by = :me, heartbeat_at = now(),
           started_at = COALESCE(b.started_at, now()), finished_at = NULL, error = NULL
      FROM migration_jobs j
      JOIN languages l ON l.id = j.language_id
      LEFT JOIN languages sl ON sl.id = j.source_language_id
     WHERE b.id = :id AND j.id = b.job_id AND j.status = 'running'
       AND (b.status = 'pending'
            OR (b.status = 'running' AND b.heartbeat_at < now() - make_interval(secs => :stale)))
    RETURNING b.id AS batch_id, CAST(b.job_id AS text) AS job_id, b.range_start, b.range_end, b.attempts,
              j.language_id, l.language_code, j.source_language_id, sl.language_code AS source_language_code,
              j.job_type, j.fields, j.provider
    """
)


def claim_batch(conn: Connection, batch_id: int, me: str) -> Claim | None:
    """Atomic claim. A duplicate delivery, a finished batch, a cancelled job or a live owner => None."""
    row = conn.execute(
        _CLAIM, {"id": batch_id, "me": me, "stale": get_settings().batch_stale_after_seconds}
    ).first()
    if row is None:
        return None
    return Claim(**{**row._mapping, "job_type": JobType(row.job_type)})


_HEARTBEAT = text(
    "UPDATE migration_batches SET heartbeat_at = now() WHERE id = :id AND locked_by = :me AND status = 'running'"
)


def heartbeat(engine: Engine, batch_id: int, me: str) -> None:
    """Own short autocommit connection: visible immediately, even though the data txn is still open."""
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        if c.execute(_HEARTBEAT, {"id": batch_id, "me": me}).rowcount != 1:
            raise LostLockError(f"batch {batch_id} is no longer owned by {me}")


_MARK_SUCCEEDED = text(
    """
    UPDATE migration_batches
       SET status = 'succeeded', processed_count = :n, error = NULL,
           finished_at = clock_timestamp(), heartbeat_at = clock_timestamp()   -- now() would be the txn START
     WHERE id = :id AND locked_by = :me AND status = 'running'
    """
)


def mark_succeeded(conn: Connection, batch_id: int, me: str, processed: int) -> None:
    """Runs in the SAME transaction as the data writes => data and progress commit atomically."""
    if conn.execute(_MARK_SUCCEEDED, {"id": batch_id, "me": me, "n": processed}).rowcount != 1:
        raise LostLockError(f"batch {batch_id} lost before commit")


_RELEASE_OR_FAIL = text(
    """
    UPDATE migration_batches
       SET status = CAST(CASE WHEN :transient AND attempts < :max_attempts THEN 'pending' ELSE 'failed' END
                         AS migration_batch_status),
           error = :error, locked_by = NULL,
           finished_at = CASE WHEN :transient AND attempts < :max_attempts THEN NULL ELSE now() END
     WHERE id = :id AND locked_by = :me AND status = 'running'
    RETURNING status, attempts, CAST(job_id AS text) AS job_id
    """
)


def release_or_fail(engine: Engine, batch_id: int, me: str, exc: BaseException) -> tuple[str, int, str] | None:
    """Failure bookkeeping. Returns (new_status, attempts, job_id) or None if we no longer own the batch."""
    with engine.begin() as conn:
        row = conn.execute(
            _RELEASE_OR_FAIL,
            {
                "id": batch_id,
                "me": me,
                "transient": is_transient(exc),
                "max_attempts": get_settings().max_batch_attempts,
                "error": f"{type(exc).__name__}: {exc}"[:2000],
            },
        ).first()
    return None if row is None else (str(row.status), int(row.attempts), row.job_id)


# ----------------------------------------------------------------------------- finalise
_FINALIZE = text(
    """
    UPDATE migration_jobs j
       SET status = CAST(CASE
                 WHEN NOT EXISTS (SELECT 1 FROM migration_batches b WHERE b.job_id = j.id AND b.status = 'failed')
                      THEN 'completed'
                 WHEN EXISTS (SELECT 1 FROM migration_batches b WHERE b.job_id = j.id AND b.status = 'succeeded')
                      THEN 'completed_with_errors'
                 ELSE 'failed' END AS migration_job_status),
           finished_at = now(), updated_at = now()
     WHERE j.id = :id AND j.status = 'running'
       AND NOT EXISTS (SELECT 1 FROM migration_batches b WHERE b.job_id = j.id AND b.status IN ('pending', 'running'))
    RETURNING status
    """
)


def finalize_job(conn: Connection, job_id: str) -> str | None:
    """Race-safe: every finishing batch calls this; only the one that sees "nothing left" wins."""
    row = conn.execute(_FINALIZE, {"id": job_id}).first()
    if row is not None:
        log.info("job_finished", job_id=job_id, status=str(row.status))
    return None if row is None else str(row.status)


def finalize_job_tx(engine: Engine, job_id: str) -> str | None:
    with engine.begin() as conn:
        return finalize_job(conn, job_id)


# ------------------------------------------------------------------------------ reaper
_REAP_STALE = text(
    """
    UPDATE migration_batches
       SET status = CAST(CASE WHEN attempts < :max_attempts THEN 'pending' ELSE 'failed' END AS migration_batch_status),
           error = 'worker lost: heartbeat stale', locked_by = NULL,
           finished_at = CASE WHEN attempts < :max_attempts THEN NULL ELSE now() END
     WHERE status = 'running' AND heartbeat_at < now() - make_interval(secs => :stale)
    RETURNING id, CAST(job_id AS text) AS job_id, status
    """
)
_REAP_LOST_PENDING = text(
    """
    SELECT b.id FROM migration_batches b JOIN migration_jobs j ON j.id = b.job_id
     WHERE j.status = 'running' AND b.status = 'pending'
       AND (b.enqueued_at IS NULL OR b.enqueued_at < now() - make_interval(secs => :redeliver))
     ORDER BY b.id LIMIT :cap
    """
)
_REAP_PENDING_JOBS = text(
    """
    SELECT CAST(id AS text) FROM migration_jobs
     WHERE (status = 'pending'  AND updated_at < now() - make_interval(secs => :pending_after))
        OR (status = 'planning' AND updated_at < now() - make_interval(secs => :planning_after))
     LIMIT 100
    """
)
_REAP_FINALIZABLE = text(
    "SELECT CAST(id AS text) FROM migration_jobs WHERE status = 'running' "
    "AND NOT EXISTS (SELECT 1 FROM migration_batches b WHERE b.job_id = migration_jobs.id "
    "AND b.status IN ('pending', 'running')) LIMIT 100"
)


@dataclass
class ReapResult:
    ran: bool = False
    stale_requeued: int = 0
    stale_failed: int = 0
    republish_batches: list[int] | None = None
    republish_jobs: list[str] | None = None
    finalized: int = 0


def reap(engine: Engine, republish_cap: int = 5000) -> ReapResult:
    """Self-healing sweep. Safe to run from many beats at once: guarded by a PG advisory lock."""
    s = get_settings()
    res = ReapResult(republish_batches=[], republish_jobs=[])
    with engine.begin() as conn:
        if not conn.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": REAPER_LOCK_KEY}).scalar_one():
            return res
        res.ran = True
        # 1. batches whose worker died
        for r in conn.execute(_REAP_STALE, {"stale": s.batch_stale_after_seconds, "max_attempts": s.max_batch_attempts}):
            if str(r.status) == "pending":
                res.stale_requeued += 1
                res.republish_batches.append(r.id)
            else:
                res.stale_failed += 1
        # 2. pending batches whose queue message was probably lost
        lost = conn.execute(_REAP_LOST_PENDING, {"redeliver": s.batch_redeliver_after_seconds, "cap": republish_cap}).scalars().all()
        res.republish_batches = sorted(set(res.republish_batches) | set(lost))
        # 3. jobs whose plan message was lost (publish failed at start)
        res.republish_jobs = list(
            conn.execute(
                _REAP_PENDING_JOBS,
                {
                    "pending_after": s.pending_job_redeliver_after_seconds,
                    "planning_after": s.batch_stale_after_seconds,
                },
            ).scalars()
        )
        # 4. jobs that should have finished but nobody noticed (incl. ones whose last batch step 1 just failed)
        for jid in conn.execute(_REAP_FINALIZABLE).scalars().all():
            if finalize_job(conn, jid):
                res.finalized += 1
    return res
