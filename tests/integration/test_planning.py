"""Job planning: keyset ranges, idempotent re-planning, edge cases."""
import pytest
from sqlalchemy import text

from app.models.enums import JobType
from app.services import job_runtime as rt
from tests.helpers import job_status, legacy_row, make_job, rows, scalar, seed_legacy, seed_legacy_bulk


def ranges(db, job_type=JobType.IMPORT_BASE, bs=2000):
    with db.connect() as c:
        return rt.plan_ranges(c, job_type, bs)


def test_ten_thousand_records_in_batches_of_two_thousand_matches_the_assessment_example(db):
    seed_legacy_bulk(db, 10_000)
    assert ranges(db) == [
        (0, 1, 2000, 2000), (1, 2001, 4000, 2000), (2, 4001, 6000, 2000), (3, 6001, 8000, 2000), (4, 8001, 10000, 2000)]


def test_remainder_goes_into_a_last_smaller_batch(db):
    seed_legacy(db, 25)
    assert ranges(db, bs=10) == [(0, 1, 10, 10), (1, 11, 20, 10), (2, 21, 25, 5)]


def test_batch_larger_than_data_gives_one_batch(db):
    seed_legacy(db, 7)
    assert ranges(db, bs=2000) == [(0, 1, 7, 7)]


def test_batch_size_of_one(db):
    seed_legacy(db, 3)
    assert ranges(db, bs=1) == [(0, 1, 1, 1), (1, 2, 2, 1), (2, 3, 3, 1)]


def test_sparse_ids_are_chunked_by_row_count_not_by_id_span(db):
    """The real dump has ids 392..23101 for 10,000 rows: batches must hold *rows*, not id spans."""
    seed_legacy(db, ids=[392, 393, 500, 23000, 23101, 23102])
    assert ranges(db, bs=2) == [(0, 392, 393, 2), (1, 500, 23000, 2), (2, 23101, 23102, 2)]


def test_ranges_are_contiguous_non_overlapping_and_cover_every_row(db):
    seed_legacy(db, ids=sorted({(i * 7919) % 100_000 + 1 for i in range(1, 3001)}))
    r = ranges(db, bs=250)
    total = scalar(db, "SELECT count(*) FROM legacy.shop_products_1001_test")
    assert sum(x[3] for x in r) == total
    for (_, s, e, c), (_, s2, _e2, _c2) in zip(r, r[1:]):
        assert e < s2                                      # no overlap
    for _, s, e, c in r:
        assert scalar(db, "SELECT count(*) FROM legacy.shop_products_1001_test WHERE id BETWEEN :s AND :e", s=s, e=e) == c
    assert [x[0] for x in r] == list(range(len(r)))


def test_empty_source_has_no_ranges(db):
    assert ranges(db) == []


def test_translate_jobs_are_planned_over_products_not_the_legacy_table(db):
    seed_legacy(db, 5)
    assert ranges(db, JobType.TRANSLATE) == []                       # products still empty
    with db.begin() as c:
        c.execute(text("INSERT INTO products (id, shop_id, product_id, handle, gender, age_group) VALUES (1,3,1,'h','f','a'),(2,3,2,'h','f','a')"))
    assert ranges(db, JobType.TRANSLATE) == [(0, 1, 2, 2)]


# ------------------------------------------------------------------------------------------ plan_job
def _pending_ids(db, jid):
    return [r.id for r in rows(db, "SELECT id FROM migration_batches WHERE job_id = CAST(:j AS uuid) ORDER BY batch_no", j=jid)]


def test_plan_job_creates_batches_sets_totals_and_goes_running(db):
    seed_legacy(db, 25)
    jid = make_job(db, "en", status="pending", batch_size=10)
    ids = rt.plan_job(db, jid)
    assert ids == _pending_ids(db, jid) and len(ids) == 3
    j = rows(db, "SELECT CAST(status AS text) AS status, total_records, total_batches, started_at FROM migration_jobs WHERE id=CAST(:j AS uuid)", j=jid)[0]
    assert (j.status, j.total_records, j.total_batches) == ("running", 25, 3) and j.started_at is not None
    assert [(r.range_start, r.range_end, r.expected_count, r.status, r.attempts) for r in rows(
        db, "SELECT range_start, range_end, expected_count, CAST(status AS text) AS status, attempts FROM migration_batches ORDER BY batch_no")] == [
        (1, 10, 10, "pending", 0), (11, 20, 10, "pending", 0), (21, 25, 5, "pending", 0)]


def test_plan_job_is_idempotent_when_delivered_twice(db):
    seed_legacy(db, 25)
    jid = make_job(db, "en", status="pending", batch_size=10)
    first = rt.plan_job(db, jid)
    again = rt.plan_job(db, jid)                              # duplicate message after the job is already running
    assert again == [] and _pending_ids(db, jid) == first


def test_replanning_after_a_crash_mid_planning_does_not_duplicate_batches(db):
    seed_legacy(db, 25)
    jid = make_job(db, "en", status="planning", batch_size=10)        # worker died after marking 'planning'
    with db.begin() as c:                                             # ...and had already inserted a batch
        c.execute(text("INSERT INTO migration_batches (job_id, batch_no, range_start, range_end, expected_count) "
                       "VALUES (CAST(:j AS uuid), 0, 1, 10, 10)"), {"j": jid})
    ids = rt.plan_job(db, jid)
    assert len(ids) == 3 and scalar(db, "SELECT count(*) FROM migration_batches") == 3
    assert job_status(db, jid) == "running"


@pytest.mark.parametrize("status", ["cancelled", "completed", "failed", "running"])
def test_plan_job_ignores_jobs_that_are_not_pending_or_planning(db, status):
    seed_legacy(db, 5)
    jid = make_job(db, "en", status=status)
    assert rt.plan_job(db, jid) == []
    assert scalar(db, "SELECT count(*) FROM migration_batches") == 0 and job_status(db, jid) == status


def test_plan_job_for_empty_source_completes_immediately(db):
    jid = make_job(db, "en", status="pending")
    assert rt.plan_job(db, jid) == []
    j = rows(db, "SELECT CAST(status AS text) AS status, total_records, total_batches, finished_at FROM migration_jobs")[0]
    assert (j.status, j.total_records, j.total_batches) == ("completed", 0, 0) and j.finished_at is not None


def test_plan_job_unknown_job_is_a_noop(db):
    assert rt.plan_job(db, "00000000-0000-0000-0000-000000000000") == []


def test_mark_enqueued_stamps_only_the_given_batches(db):
    seed_legacy(db, 25)
    jid = make_job(db, "en", status="pending", batch_size=10)
    ids = rt.plan_job(db, jid)
    assert scalar(db, "SELECT count(*) FROM migration_batches WHERE enqueued_at IS NOT NULL") == 0
    rt.mark_enqueued(db, ids[:2])
    assert scalar(db, "SELECT count(*) FROM migration_batches WHERE enqueued_at IS NOT NULL") == 2
    rt.mark_enqueued(db, [])                                          # empty list must not error


def test_plan_scales_to_many_batches(db):
    seed_legacy_bulk(db, 10_000)
    jid = make_job(db, "en", status="pending", batch_size=10)         # 1000 batches
    assert len(rt.plan_job(db, jid)) == 1000
    assert scalar(db, "SELECT sum(expected_count) FROM migration_batches") == 10_000
