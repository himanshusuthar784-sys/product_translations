"""Job management tables: migration_jobs (one per run) and migration_batches (one per id range)."""
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base
from app.models.enums import BatchStatus, JobStatus, JobType

TS = DateTime(timezone=True)


def _enum(cls, name: str):
    return SAEnum(cls, name=name, values_callable=lambda e: [m.value for m in e], create_type=False)


class MigrationJob(Base):
    __tablename__ = "migration_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    language_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey("languages.id", ondelete="RESTRICT"), nullable=False)
    source_language_id: Mapped[int | None] = mapped_column(SmallInteger, ForeignKey("languages.id", ondelete="RESTRICT"))
    job_type: Mapped[JobType] = mapped_column(_enum(JobType, "migration_job_type"), nullable=False)
    fields: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    batch_size: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        _enum(JobStatus, "migration_job_status"), nullable=False, server_default=JobStatus.PENDING.value
    )
    provider: Mapped[str | None] = mapped_column(String(64))
    total_records: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    total_batches: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    error: Mapped[str | None] = mapped_column(Text)
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(TS)
    finished_at: Mapped[datetime | None] = mapped_column(TS)
    updated_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        # one *active* job per (language, type): duplicate "start" -> 409 with the existing job
        Index(
            "uq_migration_jobs_active",
            "language_id",
            "job_type",
            unique=True,
            postgresql_where=text("status IN ('pending','planning','running')"),
        ),
        Index(
            "uq_migration_jobs_idempotency_key",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_migration_jobs_created_at", text("created_at DESC")),
    )


class MigrationBatch(Base):
    __tablename__ = "migration_batches"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("migration_jobs.id", ondelete="CASCADE"), nullable=False
    )
    batch_no: Mapped[int] = mapped_column(Integer, nullable=False)
    range_start: Mapped[int] = mapped_column(BigInteger, nullable=False)  # inclusive product id
    range_end: Mapped[int] = mapped_column(BigInteger, nullable=False)    # inclusive product id
    expected_count: Mapped[int] = mapped_column(Integer, nullable=False)
    processed_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    status: Mapped[BatchStatus] = mapped_column(
        _enum(BatchStatus, "migration_batch_status"), nullable=False, server_default=BatchStatus.PENDING.value
    )
    attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    locked_by: Mapped[str | None] = mapped_column(String(128))
    heartbeat_at: Mapped[datetime | None] = mapped_column(TS)
    enqueued_at: Mapped[datetime | None] = mapped_column(TS)  # last time a queue message was published
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(TS)
    finished_at: Mapped[datetime | None] = mapped_column(TS)

    __table_args__ = (
        # makes planning idempotent; also serves the job_id FK and ordered listing
        UniqueConstraint("job_id", "batch_no", name="uq_migration_batches_job_id_batch_no"),
        # progress aggregation = index-only scan
        Index("ix_migration_batches_job_status", "job_id", "status", postgresql_include=["processed_count"]),
        # reaper: tiny partial index over in-flight batches only
        Index("ix_migration_batches_running_heartbeat", "heartbeat_at", postgresql_where=text("status = 'running'")),
    )
