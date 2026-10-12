"""The self-healing sweep: dead workers, lost queue messages, lost plan messages, forgotten finalisation."""
import threading

import pytest
from sqlalchemy import text

from app.core.config import get_settings
from app.services import job_runtime as rt
from tests.helpers import batch_status, job_status, make_batch, make_job, rows, scalar, seed_legacy

STALE = 300 + 120   # comfortably past BATCH_STALE_AFTER_SECONDS
LONG_AGO = 900 + 120


@pytest.fixture
def job(db):
    return make_job(db, "fr", status="running")


def test_stale_running_batch_is_requeued(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10, status="running", locked_by="dead:1", attempts=1, hb_age=STALE)
    res = rt.reap(db)
    assert res.ran and res.stale_requeued == 1 and bid in res.republish_batches
    r = rows(db, "SELECT CAST(status AS text) AS status, locked_by, error FROM migration_batches")[0]
    assert (r.status, r.locked_by) == ("pending", None) and "heartbeat stale" in r.error


def test_stale_running_batch_that_exhausted_its_attempts_is_failed_not_requeued(db, job, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_batch_attempts", 3)
    bid = make_batch(db, job, 0, 1, 10, 10, status="running", locked_by="dead:1", attempts=3, hb_age=STALE)
    res = rt.reap(db)
    assert res.stale_failed == 1 and res.stale_requeued == 0 and bid not in res.republish_batches
    assert batch_status(db, bid) == "failed"
    assert job_status(db, job) == "failed"                          # last open batch failed => job finalised in the same sweep


def test_a_live_running_batch_is_left_alone(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10, status="running", locked_by="alive:1", attempts=1, hb_age=10)
    res = rt.reap(db)
    assert res.stale_requeued == 0 and batch_status(db, bid) == "running" and bid not in res.republish_batches


def test_pending_batch_never_published_is_republished(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)                          # enqueued_at IS NULL (publish failed)
    assert bid in rt.reap(db).republish_batches


def test_recently_published_pending_batch_is_not_republished(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10, enq_age=30)
    assert bid not in rt.reap(db).republish_batches


def test_pending_batch_whose_message_is_overdue_is_republished(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10, enq_age=LONG_AGO)
    assert bid in rt.reap(db).republish_batches


def test_pending_batches_of_cancelled_or_finished_jobs_are_never_republished(db):
    ids = []
    for lang, st in (("fr", "cancelled"), ("bg", "completed")):
        ids.append(make_batch(db, make_job(db, lang, status=st), 0, 1, 10, 10))
    assert not set(ids) & set(rt.reap(db).republish_batches)


def test_republish_is_capped(db, job):
    for i in range(10):
        make_batch(db, job, i, i * 10 + 1, i * 10 + 10, 10)
    assert len(rt.reap(db, republish_cap=4).republish_batches) == 4


def test_pending_job_whose_plan_message_was_lost_is_republished(db):
    jid = make_job(db, "fr", status="pending")
    assert jid not in rt.reap(db).republish_jobs                    # brand new: give the queue a chance
    with db.begin() as c:
        c.execute(text("UPDATE migration_jobs SET updated_at = now() - interval '5 minutes'"))
    assert jid in rt.reap(db).republish_jobs


def test_long_planning_job_is_only_republished_after_the_stale_window(db):
    jid = make_job(db, "fr", status="planning")
    with db.begin() as c:
        c.execute(text("UPDATE migration_jobs SET updated_at = now() - interval '2 minutes'"))
    assert jid not in rt.reap(db).republish_jobs                    # planning 10M rows legitimately takes a while
    with db.begin() as c:
        c.execute(text("UPDATE migration_jobs SET updated_at = now() - interval '10 minutes'"))
    assert jid in rt.reap(db).republish_jobs


def test_running_job_with_nothing_left_is_finalised(db, job):
    make_batch(db, job, 0, 1, 10, 10, status="succeeded")           # last worker died right after its commit
    res = rt.reap(db)
    assert res.finalized == 1 and job_status(db, job) == "completed"


def test_running_job_with_open_batches_is_not_finalised(db, job):
    make_batch(db, job, 0, 1, 10, 10, status="succeeded")
    make_batch(db, job, 1, 11, 20, 10, enq_age=5)
    assert rt.reap(db).finalized == 0 and job_status(db, job) == "running"


def test_reap_is_idempotent(db, job):
    make_batch(db, job, 0, 1, 10, 10, status="running", locked_by="dead:1", attempts=1, hb_age=STALE)
    rt.reap(db)
    second = rt.reap(db)
    assert second.stale_requeued == 0                                # already repaired


def test_reaper_is_exclusive_across_instances_via_advisory_lock(db, job):
    make_batch(db, job, 0, 1, 10, 10, status="running", locked_by="dead:1", attempts=1, hb_age=STALE)
    holder = db.connect()
    try:
        holder.execute(text("SELECT pg_advisory_lock(:k)"), {"k": rt.REAPER_LOCK_KEY})   # another beat is mid-sweep
        res = rt.reap(db)
        assert res.ran is False and res.stale_requeued == 0
        assert scalar(db, "SELECT count(*) FROM migration_batches WHERE status = 'running'") == 1   # untouched
    finally:
        holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": rt.REAPER_LOCK_KEY})
        holder.close()
    assert rt.reap(db).ran is True                                   # lock released with the sweep's transaction


def test_concurrent_reapers_never_double_repair(db, wide, job):
    for i in range(5):
        make_batch(db, job, i, i * 10 + 1, i * 10 + 10, 10, status="running", locked_by="dead:1", attempts=1, hb_age=STALE)
    out, barrier = [], threading.Barrier(12)

    def go():
        barrier.wait()
        out.append(rt.reap(wide))

    ts = [threading.Thread(target=go) for _ in range(12)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sum(r.stale_requeued for r in out) == 5                   # each stale batch repaired exactly once overall


# ------------------------------------------------------------------------------ the beat task, end to end
def test_reap_task_republishes_through_the_broker_and_the_job_then_completes(db, broker):
    """Simulates: worker killed mid-batch AND queue messages lost. Beat alone must bring the job home."""
    from app.workers import tasks

    seed_legacy(db, 25)
    jid = make_job(db, "en", status="pending", batch_size=10)
    ids = rt.plan_job(db, jid)                                       # planned, but nothing was ever published
    with db.begin() as c:                                            # and batch 0 was claimed by a worker that died
        c.execute(text("UPDATE migration_batches SET status='running', attempts=1, locked_by='dead:1', "
                       "heartbeat_at = now() - interval '10 minutes' WHERE id = :b"), {"b": ids[0]})

    out = tasks.reap()
    assert out["ran"] and out["stale_requeued"] == 1 and out["republished_batches"] == 3
    assert scalar(db, "SELECT count(*) FROM migration_batches WHERE enqueued_at IS NOT NULL") == 3
    broker.drain()
    assert job_status(db, jid) == "completed"
    assert scalar(db, "SELECT count(*) FROM products") == 25
    assert scalar(db, "SELECT sum(processed_count) FROM migration_batches") == 25
    assert tasks.reap()["republished_batches"] == 0                  # nothing left to repair


def test_reap_task_republishes_lost_plan_messages(db, broker):
    from app.workers import tasks

    seed_legacy(db, 5)
    jid = make_job(db, "en", status="pending")
    with db.begin() as c:
        c.execute(text("UPDATE migration_jobs SET updated_at = now() - interval '5 minutes'"))
    tasks.reap()
    assert list(broker.plans) == [jid]
    broker.drain()
    assert job_status(db, jid) == "completed" and scalar(db, "SELECT count(*) FROM products") == 5
