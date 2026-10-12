import uuid

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.schemas import (
    ActionResponse,
    BatchList,
    JobList,
    JobStatusResponse,
    StartMigrationRequest,
    StartMigrationResponse,
)
from app.services import job_api

router = APIRouter(prefix="/migration", tags=["migration"])

ERRORS = {404: {"description": "not found"}, 409: {"description": "conflict"}, 422: {"description": "invalid request"}}


@router.post("/start", response_model=StartMigrationResponse, status_code=status.HTTP_202_ACCEPTED, responses=ERRORS)
async def start_migration(
    body: StartMigrationRequest, response: Response, session: AsyncSession = Depends(get_session)
):
    """Create a migration job for one language and enqueue it. Returns immediately (202).

    The default language (en) imports the legacy table into `products` + translations;
    any other language translates from the default language through the configured provider.
    """
    job, language_code, created = await job_api.create_job(session, body)
    if not created:
        response.status_code = status.HTTP_200_OK  # idempotent replay
    response.headers["Location"] = f"/migration/status/{job.id}"
    return StartMigrationResponse(
        job_id=job.id, status=job.status.value, language_code=language_code, job_type=job.job_type.value, created=created
    )


@router.get("/status/{job_id}", response_model=JobStatusResponse, responses=ERRORS)
async def migration_status(job_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    """Progress of a job: records, percentage, batch counts, ETA."""
    return await job_api.get_job_status(session, job_id)


@router.get("/jobs", response_model=JobList)
async def list_jobs(
    language_code: str | None = None,
    status: str | None = Query(default=None, description="pending|planning|running|completed|completed_with_errors|failed|cancelled"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    items = await job_api.list_jobs(session, language_code=language_code, status=status, limit=limit, offset=offset)
    return JobList(items=items, limit=limit, offset=offset)


@router.get("/jobs/{job_id}/batches", response_model=BatchList, responses=ERRORS)
async def list_batches(
    job_id: uuid.UUID,
    status: str | None = Query(default=None, description="pending|running|succeeded|failed"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    items = await job_api.list_batches(session, job_id, status=status, limit=limit, offset=offset)
    return BatchList(items=items, limit=limit, offset=offset)


@router.post("/jobs/{job_id}/cancel", response_model=ActionResponse, responses=ERRORS)
async def cancel(job_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    """Stop a job: no further batches are claimed; in-flight batches finish."""
    st = await job_api.cancel_job(session, job_id)
    return ActionResponse(job_id=job_id, status=st.value)


@router.post("/jobs/{job_id}/retry", response_model=ActionResponse, status_code=status.HTTP_202_ACCEPTED, responses=ERRORS)
async def retry(job_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    """Re-run only the failed batches of a failed / completed_with_errors job."""
    st, n = await job_api.retry_failed(session, job_id)
    return ActionResponse(job_id=job_id, status=st.value, detail=f"{n} batch(es) re-queued")
