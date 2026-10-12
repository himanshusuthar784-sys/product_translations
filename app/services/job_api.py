"""API-side job operations (async). Workers never import this module."""
import asyncio
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.models import Language, MigrationJob, Product
from app.models.enums import ACTIVE_JOB_STATUSES, JobStatus, JobType
from app.schemas import BatchCounts, JobStatusResponse, StartMigrationRequest
from app.services.errors import Conflict, NotFound, Unprocessable
from app.services.fields import validate_fields
from app.workers.providers import provider_names

log = get_logger(__name__)


# ------------------------------------------------------------------------------- publish
async def _publish_plan(job_id: uuid.UUID) -> None:
    """Best effort. If Redis is down the job stays 'pending' and the reaper republishes it."""
    from app.workers.dispatch import publish_plan

    try:
        await asyncio.to_thread(publish_plan, str(job_id))
    except Exception as exc:  # noqa: BLE001
        log.error("publish_plan_failed", job_id=str(job_id), error=str(exc))


async def _publish_batches(ids: list[int]) -> bool:
    from app.workers.dispatch import publish_batches

    try:
        await asyncio.to_thread(publish_batches, ids)
        return True
    except Exception as exc:  # noqa: BLE001
        log.error("publish_batches_failed", count=len(ids), error=str(exc))  # reaper will recover (enqueued_at NULL)
        return False


# ---------------------------------------------------------------------------------- start
async def _get_language(session: AsyncSession, code: str) -> Language:
    lang = await session.scalar(select(Language).where(Language.language_code == code))
    if lang is None:
        raise NotFound("language", code)
    if not lang.is_active:
        raise Unprocessable("language_inactive", f"language '{code}' is not active")
    return lang


async def _job_by_idempotency_key(session: AsyncSession, key: str) -> MigrationJob | None:
    return await session.scalar(select(MigrationJob).where(MigrationJob.idempotency_key == key))


def _replay(existing: MigrationJob, lang_id: int, lang_code: str) -> tuple[MigrationJob, str, bool]:
    """Same key => same request => same job. A key reused for a *different* language is a client bug: refuse it
    explicitly rather than silently returning a job for another language."""
    if existing.language_id != lang_id:
        raise Conflict(
            "idempotency_key_reused",
            "this idempotency_key was already used for a different language",
            job_id=str(existing.id),
        )
    return existing, lang_code, False


async def create_job(session: AsyncSession, req: StartMigrationRequest) -> tuple[MigrationJob, str, bool]:
    """Returns (job, language_code, created).

    NOTE: after a failed commit `session.rollback()` expires every ORM object, and a lazy reload is
    illegal under asyncio (MissingGreenlet). So plain values are copied out of `lang` up front.
    """
    settings = get_settings()
    code = req.language_code.strip()
    lang = await _get_language(session, code)
    lang_id, lang_code, lang_is_default = lang.id, lang.language_code, lang.is_default

    if req.idempotency_key:
        existing = await _job_by_idempotency_key(session, req.idempotency_key)
        if existing is not None:
            return _replay(existing, lang_id, lang_code)

    try:
        fields = validate_fields(req.fields)
    except ValueError as exc:
        raise Unprocessable("invalid_fields", str(exc)) from exc

    batch_size = req.batch_size or settings.default_batch_size
    if batch_size > settings.max_batch_size:
        raise Unprocessable("invalid_batch_size", f"batch_size must be <= {settings.max_batch_size}")

    default_lang = await session.scalar(select(Language).where(Language.is_default.is_(True)))
    if default_lang is None:
        raise Unprocessable("no_default_language", "no default language configured")

    default_id, default_code = default_lang.id, default_lang.language_code
    if lang_is_default:
        job_type, source_id, provider = JobType.IMPORT_BASE, None, None
    else:
        job_type, source_id, provider = JobType.TRANSLATE, default_id, settings.translation_provider
        if provider not in provider_names():
            raise Unprocessable("unknown_provider", f"translation provider '{provider}' is not registered")
        active_import = await session.scalar(
            select(MigrationJob.id).where(
                MigrationJob.language_id == default_id,
                MigrationJob.job_type == JobType.IMPORT_BASE,
                MigrationJob.status.in_(ACTIVE_JOB_STATUSES),
            )
        )
        if active_import is not None:
            raise Conflict(
                "base_import_running",
                f"import for default language '{default_code}' is still running",
                job_id=str(active_import),
            )
        if await session.scalar(select(Product.id).limit(1)) is None:
            raise Unprocessable(
                "base_import_required",
                f"no products yet: run the import for the default language '{default_code}' first",
            )

    job_id = uuid.uuid4()
    job = MigrationJob(
        id=job_id,
        language_id=lang_id,
        source_language_id=source_id,
        job_type=job_type,
        fields=fields,
        batch_size=batch_size,
        status=JobStatus.PENDING,
        provider=provider,
        idempotency_key=req.idempotency_key,
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        # lost a race: either the same idempotency key, or another active job for this language
        if req.idempotency_key:
            existing = await _job_by_idempotency_key(session, req.idempotency_key)
            if existing is not None:
                return _replay(existing, lang_id, lang_code)
        active = await session.scalar(
            select(MigrationJob.id).where(
                MigrationJob.language_id == lang_id,
                MigrationJob.job_type == job_type,
                MigrationJob.status.in_(ACTIVE_JOB_STATUSES),
            )
        )
        raise Conflict(
            "job_already_active",
            f"a migration for language '{code}' is already active",
            job_id=str(active) if active else None,
        ) from None

    log.info("job_created", job_id=str(job_id), language=lang_code, job_type=job_type.value, batch_size=batch_size)
    await _publish_plan(job_id)
    return job, lang_code, True


# --------------------------------------------------------------------------------- status
_BATCH_AGG = text(
    """
    SELECT status, count(*) AS n, COALESCE(sum(processed_count), 0) AS processed
    FROM migration_batches WHERE job_id = :id GROUP BY status
    """
)
_FAILED_EXPECTED = text(
    "SELECT COALESCE(sum(expected_count), 0) FROM migration_batches WHERE job_id = :id AND status = 'failed'"
)


def _percentage(processed: int, total: int, status: JobStatus) -> float:
    if total > 0:
        return round(min(processed, total) * 100.0 / total, 2)
    return 100.0 if status in (JobStatus.COMPLETED,) else 0.0


async def get_job_status(session: AsyncSession, job_id: uuid.UUID) -> JobStatusResponse:
    job = await session.get(MigrationJob, job_id)
    if job is None:
        raise NotFound("job", str(job_id))
    lang = await session.get(Language, job.language_id)

    counts = BatchCounts()
    processed = 0
    for row in (await session.execute(_BATCH_AGG, {"id": job_id})).all():
        n = int(row.n)
        counts.total += n
        setattr(counts, str(row.status), n)
        if str(row.status) == "succeeded":
            processed = int(row.processed)
    failed_records = int((await session.execute(_FAILED_EXPECTED, {"id": job_id})).scalar_one())

    now = datetime.now(timezone.utc)
    elapsed = eta = None
    if job.started_at:
        elapsed = round(((job.finished_at or now) - job.started_at).total_seconds(), 2)
        if job.status == JobStatus.RUNNING and processed > 0 and job.total_records > processed:
            eta = round(elapsed * (job.total_records - processed) / processed, 1)

    return JobStatusResponse(
        job_id=job.id,
        status=job.status.value,
        language_code=lang.language_code,
        job_type=job.job_type.value,
        processed_records=processed,
        total_records=int(job.total_records),
        failed_records=failed_records,
        progress_percentage=_percentage(processed, int(job.total_records), job.status),
        batches=counts,
        batch_size=job.batch_size,
        fields=list(job.fields),
        provider=job.provider,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        elapsed_seconds=elapsed,
        eta_seconds=eta,
        error=job.error,
    )


# ----------------------------------------------------------------------------- listings
async def list_jobs(session: AsyncSession, *, language_code: str | None, status: str | None, limit: int, offset: int):
    if status is not None and status not in {s.value for s in JobStatus}:
        raise Unprocessable("invalid_status", f"unknown status '{status}'")
    rows = (
        await session.execute(
            text(
                """
                SELECT j.id, CAST(j.status AS text) AS status, l.language_code, CAST(j.job_type AS text) AS job_type,
                       j.total_records, j.created_at, j.finished_at, COALESCE(p.processed, 0) AS processed
                FROM migration_jobs j
                JOIN languages l ON l.id = j.language_id
                LEFT JOIN LATERAL (
                    SELECT sum(processed_count) AS processed FROM migration_batches b
                    WHERE b.job_id = j.id AND b.status = 'succeeded') p ON true
                WHERE (CAST(:lang AS text) IS NULL OR l.language_code = :lang)
                  AND (CAST(:status AS text) IS NULL OR CAST(j.status AS text) = :status)
                ORDER BY j.created_at DESC, j.id
                LIMIT :limit OFFSET :offset
                """
            ),
            {"lang": language_code, "status": status, "limit": limit, "offset": offset},
        )
    ).all()
    return [
        {
            "job_id": r.id,
            "status": r.status,
            "language_code": r.language_code,
            "job_type": r.job_type,
            "processed_records": int(r.processed),
            "total_records": int(r.total_records),
            "progress_percentage": _percentage(int(r.processed), int(r.total_records), JobStatus(r.status)),
            "created_at": r.created_at,
            "finished_at": r.finished_at,
        }
        for r in rows
    ]


async def list_batches(session: AsyncSession, job_id: uuid.UUID, *, status: str | None, limit: int, offset: int):
    if await session.get(MigrationJob, job_id) is None:
        raise NotFound("job", str(job_id))
    if status is not None and status not in {"pending", "running", "succeeded", "failed"}:
        raise Unprocessable("invalid_status", f"unknown batch status '{status}'")
    rows = (
        await session.execute(
            text(
                """
                SELECT batch_no, CAST(status AS text) AS status, range_start, range_end, expected_count,
                       processed_count, attempts, locked_by, error, started_at, finished_at
                FROM migration_batches
                WHERE job_id = :id AND (CAST(:status AS text) IS NULL OR CAST(status AS text) = :status)
                ORDER BY batch_no LIMIT :limit OFFSET :offset
                """
            ),
            {"id": job_id, "status": status, "limit": limit, "offset": offset},
        )
    ).all()
    return [dict(r._mapping) for r in rows]


# ------------------------------------------------------------------------ cancel / retry
async def cancel_job(session: AsyncSession, job_id: uuid.UUID) -> JobStatus:
    row = (
        await session.execute(
            text(
                "UPDATE migration_jobs SET status = 'cancelled', finished_at = now(), updated_at = now() "
                "WHERE id = :id AND status IN ('pending', 'planning', 'running') RETURNING status"
            ),
            {"id": job_id},
        )
    ).first()
    await session.commit()
    if row is not None:
        log.info("job_cancelled", job_id=str(job_id))
        return JobStatus.CANCELLED
    job = await session.get(MigrationJob, job_id)
    if job is None:
        raise NotFound("job", str(job_id))
    raise Conflict("job_not_cancellable", f"job is already {job.status.value}")


async def retry_failed(session: AsyncSession, job_id: uuid.UUID) -> tuple[JobStatus, int]:
    job = await session.get(MigrationJob, job_id)
    if job is None:
        raise NotFound("job", str(job_id))
    if job.status not in (JobStatus.COMPLETED_WITH_ERRORS, JobStatus.FAILED):
        raise Conflict("job_not_retryable", f"only failed jobs can be retried (job is {job.status.value})")

    if job.total_batches == 0:  # failed while planning: plan again from scratch
        new_status = JobStatus.PENDING
        ids: list[int] = []
    else:
        new_status = JobStatus.RUNNING
        ids = list(
            (
                await session.execute(
                    text(
                        "UPDATE migration_batches SET status = 'pending', attempts = 0, error = NULL, locked_by = NULL, "
                        "finished_at = NULL, enqueued_at = NULL WHERE job_id = :id AND status = 'failed' RETURNING id"
                    ),
                    {"id": job_id},
                )
            ).scalars()
        )
    try:
        await session.execute(
            text(
                "UPDATE migration_jobs SET status = CAST(:s AS migration_job_status), finished_at = NULL, error = NULL, "
                "updated_at = now() WHERE id = :id"
            ),
            {"s": new_status.value, "id": job_id},
        )
        await session.commit()
    except IntegrityError:
        await session.rollback()  # also undoes the batch reset
        raise Conflict("job_already_active", "another migration for this language is active") from None

    log.info("job_retry", job_id=str(job_id), batches=len(ids))
    if new_status == JobStatus.PENDING:
        await _publish_plan(job_id)
    elif ids and await _publish_batches(ids):
        await session.execute(
            text("UPDATE migration_batches SET enqueued_at = now() WHERE id = ANY(CAST(:ids AS bigint[]))"),
            {"ids": ids},
        )
        await session.commit()
    return new_status, len(ids)
