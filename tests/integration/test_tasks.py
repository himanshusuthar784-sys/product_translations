"""The real Celery task functions (plan_job / process_batch) driven through the in-process broker.

Failure injection wraps the real processor, so everything else (claim, transaction, finalisation) is genuine.
"""
import random
import threading

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.core.config import get_settings
from app.services import job_runtime as rt
from app.workers import tasks
from app.workers.processors.import_base import ImportBaseProcessor
from tests.helpers import batch_status, job_status, make_ctx, make_job, rows, run_processor, scalar, seed_legacy, seed_legacy_bulk, snapshot

TR = "product_id, language_id, key"


def expected_translation_rows(counts: dict) -> int:
    return sum(counts.values())


def start_import(db, broker, n=250, bs=100):
    counts = seed_legacy_bulk(db, n)
    jid = make_job(db, "en", status="pending", batch_size=bs)
    tasks.plan_job(jid)
    return jid, counts


def wrap_processor(monkeypatch, behaviour):
    """behaviour(original, self, conn, ctx, call_no) -> result. Wraps the real ImportBaseProcessor.process."""
    original, calls = ImportBaseProcessor.process, {"n": 0}

    def wrapper(self, conn, ctx):
        calls["n"] += 1
        return behaviour(original, self, conn, ctx, calls["n"])

    monkeypatch.setattr(ImportBaseProcessor, "process", wrapper)
    return calls


def db_error():
    return OperationalError("UPDATE ...", {}, Exception("server closed the connection unexpectedly"))


# ---------------------------------------------------------------------------------------- happy path
def test_plan_publishes_all_batches_and_workers_complete_the_job(db, broker):
    jid, counts = start_import(db, broker, n=250, bs=100)
    assert len(broker.batches) == 3
    assert scalar(db, "SELECT count(*) FROM migration_batches WHERE enqueued_at IS NOT NULL") == 3   # planner stamps publish time
    broker.drain()
    assert job_status(db, jid) == "completed"
    assert scalar(db, "SELECT count(*) FROM products") == 250
    assert scalar(db, "SELECT count(*) FROM product_translations") == expected_translation_rows(counts)
    b = rows(db, "SELECT CAST(status AS text) AS status, attempts, processed_count, expected_count, locked_by, finished_at FROM migration_batches")
    assert all(r.status == "succeeded" and r.attempts == 1 and r.processed_count == r.expected_count and r.finished_at for r in b)
    assert sum(r.processed_count for r in b) == 250


def test_translate_job_end_to_end_through_tasks(db, broker):
    jid, counts = start_import(db, broker, n=120, bs=50)
    broker.drain()
    fr_job = make_job(db, "fr", status="pending", batch_size=50)
    tasks.plan_job(fr_job)
    broker.drain()
    assert job_status(db, fr_job) == "completed"
    n = lambda code: scalar(db, "SELECT count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code=:c", c=code)
    assert n("fr") == n("en") == expected_translation_rows(counts)


def test_task_return_values_describe_the_outcome(db, broker):
    jid, _ = start_import(db, broker, n=30, bs=10)
    bid = broker.batches.popleft()
    assert tasks.process_batch(bid) == {"batch_id": bid, "status": "succeeded", "processed": 10}
    assert tasks.process_batch(bid) == {"batch_id": bid, "status": "skipped"}


# --------------------------------------------------------------------------------- duplicate delivery
def test_duplicate_batch_delivery_is_harmless_and_writes_nothing(db, broker):
    jid, _ = start_import(db, broker, n=30, bs=10)
    ids = list(broker.batches)
    broker.drain()
    snap = (snapshot(db, "products", "id"), snapshot(db, "product_translations", TR))
    for bid in ids:                                   # every message redelivered (at-least-once transport)
        assert tasks.process_batch(bid)["status"] == "skipped"
    assert (snapshot(db, "products", "id"), snapshot(db, "product_translations", TR)) == snap
    assert scalar(db, "SELECT max(attempts) FROM migration_batches") == 1


def test_duplicate_plan_delivery_does_not_duplicate_batches_or_work(db, broker):
    jid, _ = start_import(db, broker, n=30, bs=10)
    tasks.plan_job(jid)
    tasks.plan_job(jid)
    assert scalar(db, "SELECT count(*) FROM migration_batches") == 3 and len(broker.batches) == 3
    broker.drain()
    assert job_status(db, jid) == "completed"


# --------------------------------------------------------------------------------------- transient errors
def test_transient_db_error_is_retried_and_the_job_still_completes_once(db, broker, monkeypatch):
    jid, counts = start_import(db, broker, n=250, bs=100)

    def behaviour(orig, self, conn, ctx, n):
        if n == 1:
            raise db_error()
        return orig(self, conn, ctx)

    wrap_processor(monkeypatch, behaviour)
    first = broker.batches[0]
    with pytest.raises(OperationalError):             # what Celery's self.retry() surfaces when called directly
        tasks.process_batch(first)
    r = rows(db, "SELECT CAST(status AS text) AS status, attempts, locked_by, error FROM migration_batches WHERE id=:b", b=first)[0]
    assert (r.status, r.attempts, r.locked_by) == ("pending", 1, None) and "OperationalError" in r.error
    assert job_status(db, jid) == "running"
    broker.drain()                                    # redelivered message succeeds
    assert job_status(db, jid) == "completed"
    assert scalar(db, "SELECT attempts FROM migration_batches WHERE id=:b", b=first) == 2
    assert scalar(db, "SELECT count(*) FROM product_translations") == expected_translation_rows(counts)   # no duplicates


def test_transient_errors_beyond_the_attempt_limit_fail_the_batch(db, broker, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_batch_attempts", 3)
    jid, _ = start_import(db, broker, n=10, bs=10)            # single batch
    wrap_processor(monkeypatch, lambda orig, self, conn, ctx, n: (_ for _ in ()).throw(db_error()))
    broker.drain()
    r = rows(db, "SELECT CAST(status AS text) AS status, attempts, error FROM migration_batches")[0]
    assert (r.status, r.attempts) == ("failed", 3) and "OperationalError" in r.error
    assert job_status(db, jid) == "failed"
    assert scalar(db, "SELECT count(*) FROM products") == 0


def test_error_message_is_kept_for_diagnosis_between_retries(db, broker, monkeypatch):
    jid, _ = start_import(db, broker, n=10, bs=10)

    def behaviour(orig, self, conn, ctx, n):
        if n == 1:
            raise db_error()
        return orig(self, conn, ctx)

    wrap_processor(monkeypatch, behaviour)
    with pytest.raises(OperationalError):
        tasks.process_batch(broker.batches.popleft())
    assert "server closed the connection" in scalar(db, "SELECT error FROM migration_batches")
    tasks.process_batch(scalar(db, "SELECT id FROM migration_batches"))
    assert scalar(db, "SELECT error FROM migration_batches") is None       # cleared on the successful attempt


# --------------------------------------------------------------------------------------- permanent errors
def test_permanent_error_fails_only_that_batch_and_the_job_completes_with_errors(db, broker, monkeypatch):
    jid, counts = start_import(db, broker, n=300, bs=100)

    def behaviour(orig, self, conn, ctx, n):
        if ctx.range_start == 101:                    # the second batch is poisoned
            raise ValueError("corrupt row")
        return orig(self, conn, ctx)

    wrap_processor(monkeypatch, behaviour)
    broker.drain()
    st = {r.batch_no: r.status for r in rows(db, "SELECT batch_no, CAST(status AS text) AS status FROM migration_batches")}
    assert st == {0: "succeeded", 1: "failed", 2: "succeeded"}
    assert scalar(db, "SELECT attempts FROM migration_batches WHERE batch_no = 1") == 1       # no pointless retries
    assert "ValueError: corrupt row" in scalar(db, "SELECT error FROM migration_batches WHERE batch_no = 1")
    assert job_status(db, jid) == "completed_with_errors"
    assert scalar(db, "SELECT count(*) FROM products") == 200                                  # healthy batches kept


def test_failed_batch_leaves_no_partial_data_even_though_the_processor_had_already_written(db, broker, monkeypatch):
    """Atomicity: the processor completes its writes and *then* the batch fails => everything rolls back."""
    jid, _ = start_import(db, broker, n=10, bs=10)

    def behaviour(orig, self, conn, ctx, n):
        orig(self, conn, ctx)
        raise ValueError("boom after writing")

    wrap_processor(monkeypatch, behaviour)
    broker.drain()
    assert scalar(db, "SELECT count(*) FROM products") == 0
    assert scalar(db, "SELECT count(*) FROM product_translations") == 0
    assert job_status(db, jid) == "failed"


def test_then_fixing_the_cause_and_resetting_the_batch_completes_the_job(db, broker, monkeypatch):
    jid, counts = start_import(db, broker, n=200, bs=100)
    flag = {"poison": True}

    def behaviour(orig, self, conn, ctx, n):
        if flag["poison"] and ctx.range_start == 101:
            raise ValueError("corrupt row")
        return orig(self, conn, ctx)

    wrap_processor(monkeypatch, behaviour)
    broker.drain()
    assert job_status(db, jid) == "completed_with_errors"
    flag["poison"] = False
    with db.begin() as c:                              # what POST /retry does
        ids = c.execute(text("UPDATE migration_batches SET status='pending', attempts=0, error=NULL, finished_at=NULL WHERE status='failed' RETURNING id")).scalars().all()
        c.execute(text("UPDATE migration_jobs SET status='running', finished_at=NULL"))
    broker.publish_batches(ids)
    broker.drain()
    assert job_status(db, jid) == "completed"
    assert scalar(db, "SELECT count(*) FROM products") == 200


# ---------------------------------------------------------------------------------- lost lock / takeover
def test_worker_that_lost_its_lock_commits_nothing_and_records_no_failure(db, broker, monkeypatch):
    jid, _ = start_import(db, broker, n=10, bs=10)

    def behaviour(orig, self, conn, ctx, n):
        result = orig(self, conn, ctx)
        with db.begin() as other:                      # the reaper handed the batch to someone else meanwhile
            other.execute(text("UPDATE migration_batches SET locked_by = 'thief:9'"))
        return result

    wrap_processor(monkeypatch, behaviour)
    out = tasks.process_batch(broker.batches.popleft())
    assert out["status"] == "lost_lock"
    assert scalar(db, "SELECT count(*) FROM products") == 0                     # rolled back
    r = rows(db, "SELECT CAST(status AS text) AS status, locked_by, error FROM migration_batches")[0]
    assert (r.status, r.locked_by, r.error) == ("running", "thief:9", None)    # the new owner's state is untouched
    assert job_status(db, jid) == "running"


def test_batch_abandoned_by_a_killed_worker_is_taken_over_when_its_heartbeat_is_stale(db, broker):
    jid, counts = start_import(db, broker, n=10, bs=10)
    bid = broker.batches.popleft()
    with db.begin() as c:                              # worker claimed the batch, then was SIGKILLed
        c.execute(text("UPDATE migration_batches SET status='running', attempts=1, locked_by='dead:1', "
                       "heartbeat_at = now() - interval '10 minutes'"))
    assert tasks.process_batch(bid)["status"] == "succeeded"
    assert scalar(db, "SELECT attempts FROM migration_batches") == 2
    assert job_status(db, jid) == "completed" and scalar(db, "SELECT count(*) FROM products") == 10


def test_batch_with_a_live_heartbeat_is_not_stolen(db, broker):
    jid, _ = start_import(db, broker, n=10, bs=10)
    bid = broker.batches.popleft()
    with db.begin() as c:
        c.execute(text("UPDATE migration_batches SET status='running', attempts=1, locked_by='alive:1', heartbeat_at = now()"))
    assert tasks.process_batch(bid)["status"] == "skipped"
    assert scalar(db, "SELECT count(*) FROM products") == 0


# ------------------------------------------------------------------------------------------ cancel
def test_cancelled_job_processes_nothing_further(db, broker):
    jid, _ = start_import(db, broker, n=300, bs=100)
    with db.begin() as c:
        c.execute(text("UPDATE migration_jobs SET status='cancelled'"))
    broker.drain()
    assert job_status(db, jid) == "cancelled"
    assert scalar(db, "SELECT count(*) FROM products") == 0
    assert scalar(db, "SELECT count(*) FROM migration_batches WHERE status = 'pending'") == 3


def test_cancel_mid_flight_keeps_finished_work_and_stays_cancelled(db, broker):
    jid, _ = start_import(db, broker, n=300, bs=100)
    tasks.process_batch(broker.batches.popleft())      # first batch done
    with db.begin() as c:
        c.execute(text("UPDATE migration_jobs SET status='cancelled'"))
    broker.drain()
    assert job_status(db, jid) == "cancelled"          # finalisation must not resurrect it
    assert scalar(db, "SELECT count(*) FROM products") == 100


# ------------------------------------------------------------------------------------------ planning
def test_plan_failure_is_retried_then_marks_the_job_failed(db, broker, monkeypatch):
    seed_legacy(db, 5)
    jid = make_job(db, "en", status="pending")
    monkeypatch.setattr(rt, "plan_job", lambda engine, job_id: (_ for _ in ()).throw(RuntimeError("planner exploded")))
    # eager apply() performs the retries recursively (no real delay), so the whole chain runs here:
    # attempt, retry, retry ... until max_retries is exhausted, and only THEN is the job marked failed.
    attempts = []
    original = rt.plan_job
    monkeypatch.setattr(rt, "plan_job", lambda engine, job_id: (attempts.append(1), original(engine, job_id))[1])
    res = tasks.plan_job.apply(args=[jid])
    assert res.state == "FAILURE"
    assert len(attempts) == tasks.plan_job.max_retries + 1              # 1 try + max_retries retries
    assert job_status(db, jid) == "failed"
    assert "planner exploded" in scalar(db, "SELECT error FROM migration_jobs")


def test_plan_of_an_empty_source_completes_without_publishing(db, broker):
    jid = make_job(db, "en", status="pending")
    out = tasks.plan_job(jid)
    assert out["published"] == 0 and job_status(db, jid) == "completed" and not broker.batches


# --------------------------------------------------------------------------------- real concurrency
def test_many_workers_racing_over_the_same_batches_still_produce_exactly_the_right_data(db, wide, broker, monkeypatch):
    """8 'workers' each try EVERY batch in random order (maximal duplicate delivery + contention)."""
    monkeypatch.setattr(tasks, "get_sync_engine", lambda: wide)
    jid, counts = start_import(db, broker, n=2000, bs=100)               # 20 batches
    ids = list(broker.batches)
    broker.batches.clear()
    barrier, errors = threading.Barrier(8), []

    def worker(seed):
        order = ids[:]
        random.Random(seed).shuffle(order)
        barrier.wait()
        for bid in order:
            try:
                tasks.process_batch(bid)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]

    assert not errors, errors
    assert job_status(db, jid) == "completed"
    assert scalar(db, "SELECT count(*) FROM products") == 2000
    assert scalar(db, "SELECT count(*) FROM product_translations") == expected_translation_rows(counts)
    b = rows(db, "SELECT attempts, processed_count, CAST(status AS text) AS status FROM migration_batches")
    assert len(b) == 20 and all(r.status == "succeeded" and r.attempts == 1 and r.processed_count == 100 for r in b)  # each batch ran ONCE


def test_two_languages_can_be_migrated_concurrently(db, wide, broker, monkeypatch):
    monkeypatch.setattr(tasks, "get_sync_engine", lambda: wide)
    jid, counts = start_import(db, broker, n=400, bs=100)
    broker.drain()
    fr, bg = make_job(db, "fr", status="pending", batch_size=100), make_job(db, "bg", status="pending", batch_size=100)
    tasks.plan_job(fr)
    tasks.plan_job(bg)
    ids = list(broker.batches)
    broker.batches.clear()
    ts = [threading.Thread(target=lambda chunk=ids[i::4]: [tasks.process_batch(b) for b in chunk]) for i in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert job_status(db, fr) == job_status(db, bg) == "completed"
    per_lang = dict(rows(db, "SELECT l.language_code, count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id GROUP BY 1"))
    assert per_lang == {"en": expected_translation_rows(counts), "fr": expected_translation_rows(counts), "bg": expected_translation_rows(counts)}
