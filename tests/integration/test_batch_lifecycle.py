"""Claiming, heartbeats, success/failure bookkeeping and job finalisation (the concurrency-critical SQL)."""
import threading

import pytest
from sqlalchemy import text

from app.core.config import get_settings
from app.services import job_runtime as rt
from app.workers.errors import LostLockError, TransientError
from tests.helpers import batch_status, job_status, make_batch, make_job, rows, scalar

ME, OTHER = "worker-A:1", "worker-B:2"


@pytest.fixture
def job(db):
    jid = make_job(db, "fr", status="running", fields=["title", "brand"])
    return jid


def claim(db, bid, me=ME):
    with db.begin() as c:
        return rt.claim_batch(c, bid, me)


# ------------------------------------------------------------------------------------------- claim
def test_claim_marks_running_and_returns_everything_the_worker_needs(db, job):
    bid = make_batch(db, job, 0, 5, 15, 11)
    cl = claim(db, bid)
    assert cl is not None
    assert (cl.batch_id, cl.job_id, cl.range_start, cl.range_end, cl.attempts) == (bid, job, 5, 15, 1)
    assert (cl.language_code, cl.source_language_code, cl.job_type.value, cl.fields, cl.provider) == (
        "fr", "en", "translate", ["title", "brand"], "copy_english")
    r = rows(db, "SELECT CAST(status AS text) AS status, locked_by, attempts, heartbeat_at, started_at FROM migration_batches")[0]
    assert (r.status, r.locked_by, r.attempts) == ("running", ME, 1) and r.heartbeat_at and r.started_at


def test_second_claim_of_the_same_batch_is_rejected(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    assert claim(db, bid, ME) is not None
    assert claim(db, bid, OTHER) is None                    # duplicate delivery: fresh heartbeat => hands off
    assert scalar(db, "SELECT locked_by FROM migration_batches") == ME
    assert scalar(db, "SELECT attempts FROM migration_batches") == 1


@pytest.mark.parametrize("batch_status_", ["succeeded", "failed"])
def test_finished_batches_cannot_be_claimed(db, job, batch_status_):
    bid = make_batch(db, job, 0, 1, 10, 10, status=batch_status_)
    assert claim(db, bid) is None


@pytest.mark.parametrize("job_status_", ["pending", "planning", "completed", "failed", "cancelled", "completed_with_errors"])
def test_batches_of_non_running_jobs_cannot_be_claimed(db, job_status_):
    jid = make_job(db, "fr", status=job_status_)
    bid = make_batch(db, jid, 0, 1, 10, 10)
    assert claim(db, bid) is None
    assert batch_status(db, bid) == "pending"


def test_unknown_batch_id_is_not_claimable(db):
    assert claim(db, 987654) is None


def test_stale_running_batch_can_be_taken_over_but_a_live_one_cannot(db, job):
    stale = make_batch(db, job, 0, 1, 10, 10, status="running", locked_by=OTHER, attempts=1, hb_age=get_settings().batch_stale_after_seconds + 60)
    live = make_batch(db, job, 1, 11, 20, 10, status="running", locked_by=OTHER, attempts=1, hb_age=5)
    cl = claim(db, stale, ME)
    assert cl is not None and cl.attempts == 2
    assert scalar(db, "SELECT locked_by FROM migration_batches WHERE id=:b", b=stale) == ME
    assert claim(db, live, ME) is None


def test_exactly_one_of_many_concurrent_claimers_wins(db, wide, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    results, barrier = [], threading.Barrier(20)

    def go(i):
        barrier.wait()
        results.append(claim(wide, bid, f"w{i}:1"))

    threads = [threading.Thread(target=go, args=(i,)) for i in range(20)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(r is not None for r in results) == 1
    assert scalar(db, "SELECT attempts FROM migration_batches") == 1


# ---------------------------------------------------------------------------------------- heartbeat
def test_heartbeat_refreshes_timestamp_immediately(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid)
    with db.begin() as c:
        c.execute(text("UPDATE migration_batches SET heartbeat_at = now() - interval '200 seconds'"))
    rt.heartbeat(db, bid, ME)
    assert scalar(db, "SELECT heartbeat_at > now() - interval '5 seconds' FROM migration_batches")


def test_heartbeat_is_visible_even_while_the_data_transaction_is_still_open(db, job):
    """The reaper must see a live worker's heartbeat: it uses its own autocommit connection."""
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid)
    with db.begin() as c:
        c.execute(text("UPDATE migration_batches SET heartbeat_at = now() - interval '200 seconds'"))
    with db.connect() as data_conn:
        trans = data_conn.begin()
        data_conn.execute(text("SELECT 1"))                               # data txn is open...
        rt.heartbeat(db, bid, ME)                                         # ...heartbeat from another connection
        assert scalar(db, "SELECT heartbeat_at > now() - interval '5 seconds' FROM migration_batches")
        trans.rollback()


def test_heartbeat_by_a_non_owner_raises_lost_lock(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid, ME)
    with pytest.raises(LostLockError):
        rt.heartbeat(db, bid, OTHER)


def test_heartbeat_after_the_batch_was_reassigned_raises(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10, status="running", locked_by=ME, attempts=1, hb_age=1000)
    assert claim(db, bid, OTHER) is not None                              # taken over (stale)
    with pytest.raises(LostLockError):
        rt.heartbeat(db, bid, ME)


# ---------------------------------------------------------------------------------------- success
def test_mark_succeeded_records_count_and_finish_time(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid)
    with db.begin() as c:
        rt.mark_succeeded(c, bid, ME, 10)
    r = rows(db, "SELECT CAST(status AS text) AS status, processed_count, finished_at, error FROM migration_batches")[0]
    assert (r.status, r.processed_count, r.error) == ("succeeded", 10, None) and r.finished_at


def test_mark_succeeded_without_the_lock_raises_and_rolls_back_the_data_written_with_it(db, job):
    """Data + progress are one transaction: if we lost the lock, the data must vanish too."""
    from tests.helpers import seed_legacy
    seed_legacy(db, 1)
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid, ME)
    with pytest.raises(LostLockError):
        with db.begin() as c:
            c.execute(text("INSERT INTO products (id, shop_id, product_id, handle, gender, age_group) VALUES (1, 3, 1, 'h', 'f', 'a')"))
            rt.mark_succeeded(c, bid, OTHER, 1)                           # not the owner
    assert scalar(db, "SELECT count(*) FROM products") == 0
    assert batch_status(db, bid) == "running"


# ---------------------------------------------------------------------------------------- failure
def release(db, bid, exc, me=ME):
    return rt.release_or_fail(db, bid, me, exc)


def test_transient_error_returns_batch_to_pending_while_attempts_remain(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid)
    status, attempts, jid = release(db, bid, TransientError("429"))
    assert (status, attempts, jid) == ("pending", 1, job)
    r = rows(db, "SELECT locked_by, error, finished_at FROM migration_batches")[0]
    assert r.locked_by is None and "TransientError: 429" in r.error and r.finished_at is None


def test_transient_error_fails_the_batch_once_attempts_are_exhausted(db, job, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_batch_attempts", 3)
    bid = make_batch(db, job, 0, 1, 10, 10)
    outcomes = []
    for _ in range(3):
        claim(db, bid)
        outcomes.append(release(db, bid, TransientError("still down"))[0])
    assert outcomes == ["pending", "pending", "failed"]
    assert scalar(db, "SELECT attempts FROM migration_batches") == 3 and scalar(db, "SELECT finished_at IS NOT NULL FROM migration_batches")


def test_permanent_error_fails_immediately_even_on_first_attempt(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid)
    assert release(db, bid, ValueError("bad data"))[0] == "failed"
    assert "ValueError: bad data" in scalar(db, "SELECT error FROM migration_batches")


def test_error_text_is_truncated(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid)
    release(db, bid, ValueError("x" * 50_000))
    assert len(scalar(db, "SELECT error FROM migration_batches")) <= 2000


def test_release_by_a_non_owner_changes_nothing(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid, ME)
    assert release(db, bid, ValueError("x"), me=OTHER) is None
    assert batch_status(db, bid) == "running"


def test_a_failed_batch_can_be_claimed_again_only_after_reset(db, job):
    bid = make_batch(db, job, 0, 1, 10, 10)
    claim(db, bid)
    release(db, bid, ValueError("x"))
    assert claim(db, bid) is None


# ------------------------------------------------------------------------------------- finalisation
def finalize(db, jid):
    return rt.finalize_job_tx(db, jid)


def test_job_completes_when_every_batch_succeeded(db, job):
    make_batch(db, job, 0, 1, 10, 10, status="succeeded")
    make_batch(db, job, 1, 11, 20, 10, status="succeeded")
    assert finalize(db, job) == "completed" and job_status(db, job) == "completed"
    assert scalar(db, "SELECT finished_at IS NOT NULL FROM migration_jobs")


def test_job_completes_with_errors_when_some_failed_and_some_succeeded(db, job):
    make_batch(db, job, 0, 1, 10, 10, status="succeeded")
    make_batch(db, job, 1, 11, 20, 10, status="failed")
    assert finalize(db, job) == "completed_with_errors"


def test_job_fails_when_every_batch_failed(db, job):
    make_batch(db, job, 0, 1, 10, 10, status="failed")
    make_batch(db, job, 1, 11, 20, 10, status="failed")
    assert finalize(db, job) == "failed"


@pytest.mark.parametrize("open_status", ["pending", "running"])
def test_job_is_not_finalised_while_any_batch_is_open(db, job, open_status):
    make_batch(db, job, 0, 1, 10, 10, status="succeeded")
    make_batch(db, job, 1, 11, 20, 10, status=open_status)
    assert finalize(db, job) is None and job_status(db, job) == "running"


def test_finalise_never_touches_non_running_jobs(db):
    for st in ("cancelled", "completed", "failed", "pending"):
        jid = make_job(db, "fr" if st != "pending" else "bg", status=st)
        make_batch(db, jid, 0, 1, 10, 10, status="succeeded")
        assert finalize(db, jid) is None and job_status(db, jid) == st


def test_concurrent_finalisers_exactly_one_wins(db, wide, job):
    make_batch(db, job, 0, 1, 10, 10, status="succeeded")
    out, barrier = [], threading.Barrier(20)

    def go():
        barrier.wait()
        out.append(finalize(wide, job))

    ts = [threading.Thread(target=go) for _ in range(20)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sorted(o for o in out if o) == ["completed"]


def test_finalise_inside_planning_transaction_for_empty_jobs(db, job):
    with db.begin() as c:
        assert rt.finalize_job(c, job) == "completed"                 # zero batches => nothing open => done
