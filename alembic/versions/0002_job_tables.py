"""job management tables: migration_jobs, migration_batches

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-12
"""
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TYPE migration_job_type AS ENUM ('import_base', 'translate')")
    op.execute(
        "CREATE TYPE migration_job_status AS ENUM "
        "('pending', 'planning', 'running', 'completed', 'completed_with_errors', 'failed', 'cancelled')"
    )
    op.execute("CREATE TYPE migration_batch_status AS ENUM ('pending', 'running', 'succeeded', 'failed')")

    op.execute(
        """
        CREATE TABLE migration_jobs (
            id                 uuid                  NOT NULL,
            language_id        smallint              NOT NULL,
            source_language_id smallint,
            job_type           migration_job_type    NOT NULL,
            fields             text[]                NOT NULL,
            batch_size         integer               NOT NULL CHECK (batch_size > 0),
            status             migration_job_status  NOT NULL DEFAULT 'pending',
            provider           varchar(64),
            total_records      bigint                NOT NULL DEFAULT 0,
            total_batches      integer               NOT NULL DEFAULT 0,
            error              text,
            idempotency_key    varchar(128),
            created_at         timestamptz           NOT NULL DEFAULT now(),
            started_at         timestamptz,
            finished_at        timestamptz,
            updated_at         timestamptz           NOT NULL DEFAULT now(),
            CONSTRAINT pk_migration_jobs PRIMARY KEY (id),
            CONSTRAINT fk_migration_jobs_language_id_languages
                FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT,
            CONSTRAINT fk_migration_jobs_source_language_id_languages
                FOREIGN KEY (source_language_id) REFERENCES languages (id) ON DELETE RESTRICT
        )
        """
    )
    # one ACTIVE job per (language, type) - the DB enforces it, not application code
    op.execute(
        "CREATE UNIQUE INDEX uq_migration_jobs_active ON migration_jobs (language_id, job_type) "
        "WHERE status IN ('pending', 'planning', 'running')"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_migration_jobs_idempotency_key ON migration_jobs (idempotency_key) "
        "WHERE idempotency_key IS NOT NULL"
    )
    op.execute("CREATE INDEX ix_migration_jobs_created_at ON migration_jobs (created_at DESC)")

    op.execute(
        """
        CREATE TABLE migration_batches (
            id              bigint                   GENERATED ALWAYS AS IDENTITY,
            job_id          uuid                     NOT NULL,
            batch_no        integer                  NOT NULL,
            range_start     bigint                   NOT NULL,
            range_end       bigint                   NOT NULL,
            expected_count  integer                  NOT NULL,
            processed_count integer                  NOT NULL DEFAULT 0,
            status          migration_batch_status   NOT NULL DEFAULT 'pending',
            attempts        smallint                 NOT NULL DEFAULT 0,
            locked_by       varchar(128),
            heartbeat_at    timestamptz,
            enqueued_at     timestamptz,
            error           text,
            started_at      timestamptz,
            finished_at     timestamptz,
            CONSTRAINT pk_migration_batches PRIMARY KEY (id),
            CONSTRAINT fk_migration_batches_job_id_migration_jobs
                FOREIGN KEY (job_id) REFERENCES migration_jobs (id) ON DELETE CASCADE,
            CONSTRAINT uq_migration_batches_job_id_batch_no UNIQUE (job_id, batch_no),
            CONSTRAINT ck_migration_batches_range CHECK (range_start <= range_end)
        )
        """
    )
    # progress aggregation (GROUP BY status, SUM(processed_count)) = index-only scan
    op.execute(
        "CREATE INDEX ix_migration_batches_job_status ON migration_batches (job_id, status) "
        "INCLUDE (processed_count)"
    )
    # reaper: only in-flight rows are indexed
    op.execute(
        "CREATE INDEX ix_migration_batches_running_heartbeat ON migration_batches (heartbeat_at) "
        "WHERE status = 'running'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE migration_batches")
    op.execute("DROP TABLE migration_jobs")
    op.execute("DROP TYPE migration_batch_status")
    op.execute("DROP TYPE migration_job_status")
    op.execute("DROP TYPE migration_job_type")
