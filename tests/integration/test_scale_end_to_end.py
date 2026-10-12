"""The assessment scenario at full size: 10,000 products, batch size 2,000, three languages, re-runs."""
import time

import pytest
from sqlalchemy import text

from tests.helpers import rows, scalar, seed_legacy_bulk, snapshot

N = 10_000


@pytest.fixture
def full_run(client, db, broker):
    counts = seed_legacy_bulk(db, N)

    def run(code):
        r = client.post("/migration/start", json={"language_code": code, "batch_size": 2000})
        assert r.status_code == 202, r.text
        broker.drain()
        return client.get(f"/migration/status/{r.json()['job_id']}").json()

    return run, counts


def per_language(db):
    return dict(rows(db, "SELECT l.language_code, count(*) FROM product_translations t JOIN languages l ON l.id = t.language_id GROUP BY 1"))


def test_ten_thousand_products_three_languages(full_run, db):
    run, counts = full_run
    started = time.monotonic()

    en = run("en")
    assert (en["status"], en["processed_records"], en["total_records"], en["progress_percentage"]) == ("completed", N, N, 100.0)
    assert en["batches"] == {"total": 5, "pending": 0, "running": 0, "succeeded": 5, "failed": 0}   # 1-2000, 2001-4000, ...
    assert [(b.range_start, b.range_end) for b in rows(db, "SELECT range_start, range_end FROM migration_batches ORDER BY batch_no")] == [
        (1, 2000), (2001, 4000), (4001, 6000), (6001, 8000), (8001, 10000)]
    assert scalar(db, "SELECT count(*) FROM products") == N

    for code in ("fr", "bg"):
        st = run(code)
        assert (st["status"], st["processed_records"], st["progress_percentage"]) == ("completed", N, 100.0)

    per_key = dict(rows(db, "SELECT key, count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='en' GROUP BY 1"))
    assert per_key == counts                                                # every non-empty legacy value, nothing else
    expected = sum(counts.values())
    assert per_language(db) == {"en": expected, "fr": expected, "bg": expected}
    assert scalar(db, "SELECT count(*) FROM product_translations") == 3 * expected
    print(f"\n10k x 3 languages migrated in {time.monotonic() - started:.1f}s through the real job engine")


def test_values_are_faithful_to_the_source(full_run, db):
    run, _ = full_run
    run("en")
    run("fr")
    assert scalar(db, "SELECT count(*) FROM legacy.shop_products_1001_test l JOIN product_translations t ON t.product_id = l.id "
                  "JOIN languages g ON g.id = t.language_id AND g.language_code = 'en' WHERE t.key = 'title' AND t.value->>'value' <> l.title") == 0
    assert scalar(db, "SELECT count(*) FROM legacy.shop_products_1001_test l JOIN products p ON p.id = l.id "
                      "WHERE p.shop_id <> l.shop_id OR p.product_id <> l.product_id OR p.handle <> l.handle OR p.is_deleted <> (l.is_deleted <> 0)") == 0
    assert scalar(db, "SELECT count(*) FROM products WHERE is_deleted") == N // 200
    assert scalar(db, "SELECT count(*) FROM products WHERE shop_id = 19") == N // 3000


def test_every_rerun_is_a_noop_on_data(full_run, db):
    run, _ = full_run
    for code in ("en", "fr", "bg"):
        run(code)
    snap_p, snap_t = snapshot(db, "products", "id"), snapshot(db, "product_translations", "product_id, language_id, key")
    for code in ("en", "fr", "bg"):                        # run everything again
        assert run(code)["status"] == "completed"
    assert snapshot(db, "products", "id") == snap_p
    assert snapshot(db, "product_translations", "product_id, language_id, key") == snap_t
    assert scalar(db, "SELECT count(*) FROM migration_jobs") == 6        # history kept: 6 jobs, 0 changed rows


def test_no_dead_tuples_after_reruns(full_run, db):
    run, _ = full_run
    for code in ("en", "fr"):
        run(code)
    with db.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        c.execute(text("VACUUM (ANALYZE) product_translations"))
    base = scalar(db, "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'product_translations'")
    for code in ("en", "fr"):
        run(code)
    after = scalar(db, "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'product_translations'")
    assert after - base < 50, f"re-runs created {after - base} dead tuples (IS DISTINCT FROM guard broken?)"


def test_progress_is_monotonic_while_the_job_runs(client, db, broker):
    from app.workers import tasks

    seed_legacy_bulk(db, 1000)
    jid = client.post("/migration/start", json={"language_code": "en", "batch_size": 100}).json()["job_id"]
    tasks.plan_job(broker.plans.popleft())
    seen = []
    while broker.batches:
        tasks.process_batch(broker.batches.popleft())
        seen.append(client.get(f"/migration/status/{jid}").json()["progress_percentage"])
    assert seen == sorted(seen) and seen[0] == 10.0 and seen[-1] == 100.0 and len(seen) == 10
