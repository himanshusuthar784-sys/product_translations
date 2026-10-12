"""HTTP API: validation, the start -> status flow, duplicate/idempotent requests, cancel/retry, products, languages."""
import threading
import uuid

import pytest
from sqlalchemy import text

from app.workers import tasks
from app.workers.processors.import_base import ImportBaseProcessor
from tests.helpers import job_status, make_job, rows, scalar, schema_fingerprint, seed_legacy, seed_legacy_bulk


def start(client, language_code="en", **body):
    return client.post("/migration/start", json={"language_code": language_code, **body})


def run_to_completion(client, broker, language_code="en", **body) -> dict:
    r = start(client, language_code, **body)
    assert r.status_code == 202, r.text
    broker.drain()
    st = client.get(f"/migration/status/{r.json()['job_id']}").json()
    return st


@pytest.fixture
def imported(client, db, broker):
    """25 English products imported through the API."""
    seed_legacy(db, 25)
    st = run_to_completion(client, broker, "en", batch_size=10)
    assert st["status"] == "completed"
    return st


# ============================================================================================ start
class TestStartValidation:
    def test_unknown_language_is_404(self, client):
        r = start(client, "xx")
        assert r.status_code == 404 and r.json()["error"]["code"] == "not_found"

    def test_inactive_language_is_422(self, client, db):
        with db.begin() as c:
            c.execute(text("UPDATE languages SET is_active = false WHERE language_code = 'bg'"))
        r = start(client, "bg")
        assert r.status_code == 422 and r.json()["error"]["code"] == "language_inactive"

    @pytest.mark.parametrize("fields", [["nope"], ["title", "description"], []])
    def test_bad_fields_are_422(self, client, fields):
        r = start(client, "en", fields=fields)
        assert r.status_code == 422 and r.json()["error"]["code"] == "invalid_fields"

    def test_batch_size_above_the_configured_maximum_is_422(self, client):
        r = start(client, "en", batch_size=10**7)
        assert r.status_code == 422 and r.json()["error"]["code"] == "invalid_batch_size"

    @pytest.mark.parametrize("body", [{"language_code": "en", "batch_size": 0}, {"language_code": "en", "batch_size": -1},
                                      {}, {"language_code": "e"}, {"language_code": "en", "fields": "title"}])
    def test_malformed_bodies_get_a_structured_422(self, client, body):
        r = client.post("/migration/start", json=body)
        assert r.status_code == 422 and r.json()["error"]["code"] == "validation_error"
        assert r.json()["error"]["details"]

    def test_translating_before_the_default_language_is_imported_is_422(self, client, db):
        seed_legacy(db, 3)
        r = start(client, "fr")
        assert r.status_code == 422 and r.json()["error"]["code"] == "base_import_required"
        assert scalar(db, "SELECT count(*) FROM migration_jobs") == 0          # nothing was created

    def test_translating_while_the_default_import_is_still_running_is_409(self, client, db, broker):
        seed_legacy(db, 3)
        en = start(client, "en").json()["job_id"]
        with db.begin() as c:                                                   # some products already exist
            c.execute(text("INSERT INTO products (id, shop_id, product_id, handle, gender, age_group) VALUES (1,3,1,'h','f','a')"))
        r = start(client, "fr")
        assert r.status_code == 409 and r.json()["error"]["code"] == "base_import_running"
        assert r.json()["error"]["job_id"] == en

    def test_unknown_provider_configured_is_422(self, client, db, imported, monkeypatch):
        from app.core.config import get_settings
        monkeypatch.setattr(get_settings(), "translation_provider", "ghost")
        r = start(client, "fr")
        assert r.status_code == 422 and r.json()["error"]["code"] == "unknown_provider"


class TestStartHappyPath:
    def test_returns_202_with_job_id_location_and_enqueues_exactly_one_plan_message(self, client, db, broker):
        seed_legacy(db, 5)
        r = start(client, "en", batch_size=2)
        assert r.status_code == 202
        body = r.json()
        assert body == {"job_id": body["job_id"], "status": "pending", "language_code": "en", "job_type": "import_base", "created": True}
        assert r.headers["location"] == f"/migration/status/{body['job_id']}"
        uuid.UUID(body["job_id"])
        assert list(broker.plans) == [body["job_id"]]
        j = rows(db, "SELECT CAST(status AS text) AS status, batch_size, fields, provider, CAST(job_type AS text) AS jt FROM migration_jobs")[0]
        assert (j.status, j.batch_size, j.jt, j.provider) == ("pending", 2, "import_base", None)
        assert j.fields == ["title", "meta_title", "brand", "tags", "google_product_category"]

    def test_job_type_is_derived_from_the_language(self, client, db, broker, imported):
        r = start(client, "fr")
        assert r.json()["job_type"] == "translate"
        assert rows(db, "SELECT provider FROM migration_jobs WHERE id = CAST(:j AS uuid)", j=r.json()["job_id"])[0].provider == "copy_english"

    def test_default_batch_size_comes_from_settings_and_field_subset_is_stored(self, client, db, broker):
        seed_legacy(db, 5)
        from app.core.config import get_settings
        start(client, "en", fields=["tags", "title"])
        j = rows(db, "SELECT batch_size, fields FROM migration_jobs")[0]
        assert j.batch_size == get_settings().default_batch_size and j.fields == ["title", "tags"]     # normalised order

    def test_response_carries_a_request_id_echoed_or_generated(self, client):
        assert client.get("/health", headers={"x-request-id": "abc-123"}).headers["x-request-id"] == "abc-123"
        assert len(client.get("/health").headers["x-request-id"]) >= 16


# ============================================================================== duplicate requests
class TestDuplicateRequests:
    def test_same_request_while_active_is_409_and_points_at_the_running_job(self, client, db, broker):
        seed_legacy(db, 5)
        first = start(client, "en").json()["job_id"]
        second = start(client, "en")
        assert second.status_code == 409
        err = second.json()["error"]
        assert err["code"] == "job_already_active" and err["job_id"] == first
        assert scalar(db, "SELECT count(*) FROM migration_jobs") == 1
        assert len(broker.plans) == 1                                      # the duplicate enqueued nothing

    def test_duplicate_is_rejected_at_every_active_stage(self, client, db, broker):
        seed_legacy(db, 5)
        first = start(client, "en").json()["job_id"]
        for st in ("pending", "planning", "running"):
            with db.begin() as c:
                c.execute(text("UPDATE migration_jobs SET status = CAST(:s AS migration_job_status)"), {"s": st})
            assert start(client, "en").status_code == 409

    def test_other_languages_are_not_blocked_by_an_active_job(self, client, db, broker, imported):
        assert start(client, "fr").status_code == 202
        assert start(client, "bg").status_code == 202

    def test_a_new_request_is_allowed_once_the_previous_job_is_finished(self, client, db, broker, imported):
        again = start(client, "en")
        assert again.status_code == 202 and again.json()["job_id"] != imported["job_id"]

    def test_rerun_after_completion_changes_nothing(self, client, db, broker, imported):
        before = (scalar(db, "SELECT count(*) FROM products"), scalar(db, "SELECT count(*) FROM product_translations"),
                  rows(db, "SELECT product_id, key, xmin::text FROM product_translations ORDER BY 1, 2"))
        st = run_to_completion(client, broker, "en", batch_size=10)
        assert st["status"] == "completed" and st["processed_records"] == 25
        after = (scalar(db, "SELECT count(*) FROM products"), scalar(db, "SELECT count(*) FROM product_translations"),
                 rows(db, "SELECT product_id, key, xmin::text FROM product_translations ORDER BY 1, 2"))
        assert after == before

    def test_ten_simultaneous_identical_requests_create_exactly_one_job(self, client, db, broker):
        seed_legacy(db, 5)
        out, barrier = [], threading.Barrier(10)

        def go():
            barrier.wait()
            out.append(start(client, "en"))

        ts = [threading.Thread(target=go) for _ in range(10)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        codes = sorted(r.status_code for r in out)
        assert codes == [202] + [409] * 9, codes
        winner = next(r.json()["job_id"] for r in out if r.status_code == 202)
        assert {r.json()["error"]["job_id"] for r in out if r.status_code == 409} == {winner}
        assert scalar(db, "SELECT count(*) FROM migration_jobs") == 1 and len(broker.plans) == 1


class TestIdempotencyKey:
    def test_same_key_returns_the_original_job_without_creating_another(self, client, db, broker):
        seed_legacy(db, 5)
        a = start(client, "en", idempotency_key="import-2026-10-12")
        b = start(client, "en", idempotency_key="import-2026-10-12")
        assert (a.status_code, a.json()["created"]) == (202, True)
        assert (b.status_code, b.json()["created"]) == (200, False)
        assert a.json()["job_id"] == b.json()["job_id"]
        assert scalar(db, "SELECT count(*) FROM migration_jobs") == 1 and len(broker.plans) == 1

    def test_key_still_returns_the_same_job_after_it_finished_so_a_retried_client_never_re_runs_it(self, client, db, broker):
        seed_legacy(db, 5)
        st = run_to_completion(client, broker, "en", idempotency_key="k-1")
        replay = start(client, "en", idempotency_key="k-1")
        assert replay.status_code == 200 and replay.json()["job_id"] == st["job_id"] and replay.json()["status"] == "completed"
        assert scalar(db, "SELECT count(*) FROM migration_jobs") == 1

    def test_a_different_key_after_completion_starts_a_new_job(self, client, db, broker):
        seed_legacy(db, 5)
        run_to_completion(client, broker, "en", idempotency_key="k-1")
        assert start(client, "en", idempotency_key="k-2").status_code == 202

    def test_ten_simultaneous_requests_with_the_same_key_converge_on_one_job(self, client, db, broker):
        seed_legacy(db, 5)
        out, barrier = [], threading.Barrier(10)

        def go():
            barrier.wait()
            out.append(start(client, "en", idempotency_key="race-key"))

        ts = [threading.Thread(target=go) for _ in range(10)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert all(r.status_code in (200, 202) for r in out), [r.text for r in out]
        assert len({r.json()["job_id"] for r in out}) == 1
        assert sum(r.json()["created"] for r in out) == 1
        assert scalar(db, "SELECT count(*) FROM migration_jobs") == 1

    def test_same_key_for_a_different_language_is_refused_not_silently_replayed(self, client, db, broker, imported):
        first = start(client, "fr", idempotency_key="shared-key")
        assert first.status_code == 202
        clash = start(client, "bg", idempotency_key="shared-key")
        assert clash.status_code == 409
        err = clash.json()["error"]
        assert err["code"] == "idempotency_key_reused" and err["job_id"] == first.json()["job_id"]
        assert scalar(db, "SELECT count(*) FROM migration_jobs WHERE idempotency_key = 'shared-key'") == 1
        assert scalar(db, "SELECT count(*) FROM migration_jobs j JOIN languages l ON l.id=j.language_id WHERE l.language_code='bg'") == 0

    def test_same_key_with_different_batch_size_or_fields_replays_the_original_unchanged(self, client, db, broker):
        seed_legacy(db, 5)
        a = start(client, "en", idempotency_key="k", batch_size=2)
        b = start(client, "en", idempotency_key="k", batch_size=100, fields=["title"])
        assert b.status_code == 200 and b.json()["job_id"] == a.json()["job_id"]
        j = rows(db, "SELECT batch_size, fields FROM migration_jobs")[0]
        assert j.batch_size == 2 and len(j.fields) == 5                      # the original request wins

    def test_key_is_length_limited(self, client):
        assert start(client, "en", idempotency_key="x" * 129).status_code == 422


class TestRedisOutage:
    def test_start_still_succeeds_when_publishing_fails_and_the_reaper_recovers_the_job(self, client, db, broker):
        seed_legacy(db, 10)
        broker.fail_publish = True
        r = start(client, "en", batch_size=5)
        assert r.status_code == 202                                       # accepted: the job row is the commitment
        jid = r.json()["job_id"]
        assert job_status(db, jid) == "pending" and not broker.plans
        broker.fail_publish = False
        with db.begin() as c:
            c.execute(text("UPDATE migration_jobs SET updated_at = now() - interval '5 minutes'"))
        tasks.reap()                                                      # beat tick
        broker.drain()
        assert job_status(db, jid) == "completed" and scalar(db, "SELECT count(*) FROM products") == 10


# ============================================================================================ status
class TestStatus:
    def test_shape_matches_the_assessment_sample_plus_extras(self, client, imported):
        st = imported
        for key in ("job_id", "status", "processed_records", "total_records", "progress_percentage"):
            assert key in st
        assert (st["status"], st["processed_records"], st["total_records"], st["progress_percentage"]) == ("completed", 25, 25, 100.0)
        assert st["batches"] == {"total": 3, "pending": 0, "running": 0, "succeeded": 3, "failed": 0}
        assert st["failed_records"] == 0 and st["language_code"] == "en" and st["job_type"] == "import_base"
        assert st["batch_size"] == 10 and st["started_at"] and st["finished_at"] and st["error"] is None

    def test_progress_while_running_counts_only_finished_batches(self, client, db, broker):
        seed_legacy(db, 50)
        jid = start(client, "en", batch_size=10).json()["job_id"]
        pending = client.get(f"/migration/status/{jid}").json()
        assert pending["status"] == "pending" and pending["progress_percentage"] == 0.0 and pending["batches"]["total"] == 0
        tasks.plan_job(broker.plans.popleft())
        for _ in range(2):
            tasks.process_batch(broker.batches.popleft())
        st = client.get(f"/migration/status/{jid}").json()
        assert (st["status"], st["processed_records"], st["total_records"], st["progress_percentage"]) == ("running", 20, 50, 40.0)
        assert st["batches"] == {"total": 5, "pending": 3, "running": 0, "succeeded": 2, "failed": 0}
        assert st["eta_seconds"] is not None and st["elapsed_seconds"] is not None
        broker.drain()
        done = client.get(f"/migration/status/{jid}").json()
        assert done["status"] == "completed" and done["progress_percentage"] == 100.0 and done["eta_seconds"] is None

    def test_in_flight_batch_is_reported_as_running_but_not_counted_as_processed(self, client, db, broker):
        seed_legacy(db, 20)
        jid = start(client, "en", batch_size=10).json()["job_id"]
        tasks.plan_job(broker.plans.popleft())
        with db.begin() as c:
            c.execute(text("UPDATE migration_batches SET status='running', locked_by='w:1', heartbeat_at=now() WHERE batch_no = 0"))
        st = client.get(f"/migration/status/{jid}").json()
        assert st["batches"]["running"] == 1 and st["processed_records"] == 0

    def test_empty_source_job_is_immediately_complete_at_100_percent(self, client, db, broker):
        st = run_to_completion(client, broker, "en")
        assert (st["status"], st["total_records"], st["progress_percentage"]) == ("completed", 0, 100.0)

    def test_unknown_job_is_404_and_malformed_id_is_422(self, client):
        assert client.get(f"/migration/status/{uuid.uuid4()}").status_code == 404
        assert client.get("/migration/status/not-a-uuid").status_code == 422


# ======================================================================================= listings
class TestListings:
    @pytest.fixture
    def three_jobs(self, client, db, broker, imported):
        fr = run_to_completion(client, broker, "fr")
        bg = start(client, "bg").json()["job_id"]
        return imported["job_id"], fr["job_id"], bg

    def test_newest_first_with_progress(self, client, three_jobs):
        en, fr, bg = three_jobs
        items = client.get("/migration/jobs").json()["items"]
        assert [i["job_id"] for i in items] == [bg, fr, en]
        by = {i["job_id"]: i for i in items}
        assert by[en]["progress_percentage"] == 100.0 and by[bg]["status"] == "pending" and by[bg]["progress_percentage"] == 0.0

    def test_filters_and_pagination(self, client, three_jobs):
        en, fr, bg = three_jobs
        assert [i["job_id"] for i in client.get("/migration/jobs?language_code=fr").json()["items"]] == [fr]
        assert {i["job_id"] for i in client.get("/migration/jobs?status=completed").json()["items"]} == {en, fr}
        page = client.get("/migration/jobs?limit=1&offset=1").json()
        assert [i["job_id"] for i in page["items"]] == [fr] and (page["limit"], page["offset"]) == (1, 1)
        assert client.get("/migration/jobs?language_code=es").json()["items"] == []

    @pytest.mark.parametrize("qs", ["status=exploded", "limit=0", "limit=1000", "offset=-1"])
    def test_invalid_query_params_are_422(self, client, qs):
        assert client.get(f"/migration/jobs?{qs}").status_code == 422

    def test_batches_listing_with_filter_and_paging(self, client, db, imported):
        jid = imported["job_id"]
        items = client.get(f"/migration/jobs/{jid}/batches").json()["items"]
        assert [(b["batch_no"], b["range_start"], b["range_end"], b["expected_count"], b["processed_count"], b["status"]) for b in items] == [
            (0, 1, 10, 10, 10, "succeeded"), (1, 11, 20, 10, 10, "succeeded"), (2, 21, 25, 5, 5, "succeeded")]
        assert client.get(f"/migration/jobs/{jid}/batches?status=failed").json()["items"] == []
        assert [b["batch_no"] for b in client.get(f"/migration/jobs/{jid}/batches?limit=1&offset=2").json()["items"]] == [2]

    def test_batches_errors(self, client, imported):
        assert client.get(f"/migration/jobs/{uuid.uuid4()}/batches").status_code == 404
        assert client.get(f"/migration/jobs/{imported['job_id']}/batches?status=bogus").status_code == 422


# =============================================================================== cancel and retry
class TestCancel:
    def test_cancelling_stops_further_work(self, client, db, broker):
        seed_legacy(db, 30)
        jid = start(client, "en", batch_size=10).json()["job_id"]
        tasks.plan_job(broker.plans.popleft())
        tasks.process_batch(broker.batches.popleft())
        r = client.post(f"/migration/jobs/{jid}/cancel")
        assert r.status_code == 200 and r.json()["status"] == "cancelled"
        broker.drain()
        st = client.get(f"/migration/status/{jid}").json()
        assert st["status"] == "cancelled" and st["processed_records"] == 10 and st["batches"]["pending"] == 2

    def test_cancelling_frees_the_language_for_a_new_job(self, client, db, broker):
        seed_legacy(db, 5)
        jid = start(client, "en").json()["job_id"]
        assert start(client, "en").status_code == 409
        client.post(f"/migration/jobs/{jid}/cancel")
        assert start(client, "en").status_code == 202

    def test_cancel_finished_job_is_409_and_unknown_is_404(self, client, imported):
        assert client.post(f"/migration/jobs/{imported['job_id']}/cancel").status_code == 409
        assert client.post(f"/migration/jobs/{uuid.uuid4()}/cancel").status_code == 404


class TestRetry:
    @staticmethod
    def poison(monkeypatch, flag):
        original = ImportBaseProcessor.process

        def wrapper(self, conn, ctx):
            if flag["on"] and ctx.range_start == 11:
                raise ValueError("corrupt row")
            return original(self, conn, ctx)

        monkeypatch.setattr(ImportBaseProcessor, "process", wrapper)

    def test_failed_batches_are_reported_then_retried_to_completion(self, client, db, broker, monkeypatch):
        seed_legacy(db, 30)
        flag = {"on": True}
        self.poison(monkeypatch, flag)
        st = run_to_completion(client, broker, "en", batch_size=10)
        assert st["status"] == "completed_with_errors"
        assert (st["processed_records"], st["failed_records"], st["progress_percentage"]) == (20, 10, 66.67)
        assert st["batches"]["failed"] == 1
        failed = client.get(f"/migration/jobs/{st['job_id']}/batches?status=failed").json()["items"][0]
        assert failed["batch_no"] == 1 and "corrupt row" in failed["error"] and failed["attempts"] == 1

        flag["on"] = False
        r = client.post(f"/migration/jobs/{st['job_id']}/retry")
        assert r.status_code == 202 and r.json() == {"job_id": st["job_id"], "status": "running", "detail": "1 batch(es) re-queued"}
        assert list(broker.batches) and scalar(db, "SELECT enqueued_at IS NOT NULL FROM migration_batches WHERE batch_no = 1")
        broker.drain()
        done = client.get(f"/migration/status/{st['job_id']}").json()
        assert (done["status"], done["processed_records"], done["failed_records"], done["progress_percentage"]) == ("completed", 30, 0, 100.0)
        assert scalar(db, "SELECT count(*) FROM products") == 30

    def test_only_failed_batches_are_re_run(self, client, db, broker, monkeypatch):
        seed_legacy(db, 30)
        flag = {"on": True}
        self.poison(monkeypatch, flag)
        st = run_to_completion(client, broker, "en", batch_size=10)
        flag["on"] = False
        done_attempts = {r.batch_no: r.attempts for r in rows(db, "SELECT batch_no, attempts FROM migration_batches")}
        client.post(f"/migration/jobs/{st['job_id']}/retry")
        assert len(broker.batches) == 1
        broker.drain()
        after = {r.batch_no: r.attempts for r in rows(db, "SELECT batch_no, attempts FROM migration_batches")}
        assert after[0] == done_attempts[0] and after[2] == done_attempts[2] and after[1] == 1   # reset to 0 then claimed once

    def test_retry_is_refused_for_jobs_that_are_not_failed(self, client, db, broker, imported):
        assert client.post(f"/migration/jobs/{imported['job_id']}/retry").status_code == 409
        pending = start(client, "fr").json()["job_id"]
        assert client.post(f"/migration/jobs/{pending}/retry").status_code == 409
        assert client.post(f"/migration/jobs/{uuid.uuid4()}/retry").status_code == 404

    def test_retry_is_refused_when_another_job_for_the_language_is_active_and_nothing_is_reset(self, client, db, broker, monkeypatch):
        seed_legacy(db, 30)
        self.poison(monkeypatch, {"on": True})
        st = run_to_completion(client, broker, "en", batch_size=10)
        assert start(client, "en", batch_size=10).status_code == 202              # a newer active job
        r = client.post(f"/migration/jobs/{st['job_id']}/retry")
        assert r.status_code == 409 and r.json()["error"]["code"] == "job_already_active"
        assert scalar(db, "SELECT count(*) FROM migration_batches WHERE status = 'failed'") == 1     # rolled back
        assert job_status(db, st["job_id"]) == "completed_with_errors"

    def test_job_that_failed_while_planning_is_re_planned(self, client, db, broker):
        seed_legacy(db, 5)
        jid = make_job(db, "en", status="failed")
        r = client.post(f"/migration/jobs/{jid}/retry")
        assert r.status_code == 202 and r.json()["status"] == "pending"
        assert list(broker.plans) == [jid]
        broker.drain()
        assert job_status(db, jid) == "completed"


# ========================================================================= languages / no-DDL promise
class TestLanguagesAndNoSchemaChange:
    def test_list(self, client):
        assert [(l["language_code"], l["is_default"]) for l in client.get("/languages").json()] == [("en", True), ("fr", False), ("bg", False)]

    def test_add_validate_and_reject_duplicates(self, client):
        r = client.post("/languages", json={"name": "Spanish", "language_code": "ES"})
        assert r.status_code == 201 and r.json()["language_code"] == "es" and r.json()["is_default"] is False
        assert client.post("/languages", json={"name": "Spanish again", "language_code": "es"}).status_code == 409
        assert client.post("/languages", json={"name": "Bad", "language_code": "en_US"}).status_code == 422
        assert client.post("/languages", json={"name": "", "language_code": "de"}).status_code == 422

    def test_a_language_cannot_be_added_as_a_second_default(self, client):
        assert client.post("/languages", json={"name": "X", "language_code": "xx", "is_default": True}).json()["is_default"] is False

    def test_new_language_and_new_field_need_zero_schema_changes(self, client, db, broker, imported):
        """The headline requirement, proven: schema fingerprint is identical before and after."""
        before = schema_fingerprint(db)
        assert client.post("/languages", json={"name": "Spanish", "language_code": "es"}).status_code == 201
        assert client.post("/languages", json={"name": "German", "language_code": "de"}).status_code == 201
        for code in ("es", "de"):
            assert run_to_completion(client, broker, code)["status"] == "completed"
        for key, val in (("features", "Waterproof"), ("specifications", {"w": 10}), ("seo_keywords", ["a", "b"]), ("ingredients", "n/a")):
            assert client.put("/products/1/translations/es", json={"translations": {key: val}}).status_code == 200
        assert schema_fingerprint(db) == before
        got = client.get("/products/1?lang=es").json()["translations"]
        assert got["features"] == "Waterproof" and got["specifications"] == {"w": 10} and got["seo_keywords"] == ["a", "b"]
        assert got["title"] == "Product 1"


# ===================================================================================== products read
class TestProducts:
    def test_reads_requested_language(self, client, db, broker, imported):
        run_to_completion(client, broker, "fr")
        p = client.get("/products/1?lang=fr").json()
        assert p["language"] == "fr" and p["translations"]["title"] == "Product 1" and p["fallback_keys"] == []
        assert (p["id"], p["shop_id"], p["handle"], p["is_deleted"]) == (1, 3, "product-1", False)

    def test_missing_keys_fall_back_to_the_default_language_and_say_so(self, client, db, broker, imported):
        p = client.get("/products/1?lang=fr").json()                       # fr not migrated yet
        assert p["translations"]["title"] == "Product 1" and "title" in p["fallback_keys"]
        assert client.get("/products/1?lang=fr&fallback=false").json()["translations"] == {}

    def test_default_language_is_used_when_lang_is_omitted(self, client, imported):
        p = client.get("/products/1").json()
        assert p["language"] == "en" and p["fallback_keys"] == [] and "title" in p["translations"]

    def test_404s(self, client, imported):
        assert client.get("/products/999999").status_code == 404
        assert client.get("/products/1?lang=zz").status_code == 404

    def test_human_edit_survives_re_running_the_migration(self, client, db, broker, imported):
        run_to_completion(client, broker, "fr")
        assert client.put("/products/1/translations/fr", json={"translations": {"title": "Mon titre"}}).status_code == 200
        assert rows(db, "SELECT CAST(source AS text) AS s FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE product_id=1 AND key='title' AND l.language_code='fr'")[0].s == "human"
        run_to_completion(client, broker, "fr")
        assert client.get("/products/1?lang=fr").json()["translations"]["title"] == "Mon titre"
        assert client.get("/products/2?lang=fr").json()["translations"]["title"] == "Product 2"      # others unaffected

    def test_put_validation(self, client, imported):
        assert client.put("/products/1/translations/fr", json={"translations": {}}).status_code == 422
        assert client.put("/products/1/translations/fr", json={"translations": {"Bad Key": "x"}}).status_code == 422
        assert client.put("/products/999/translations/fr", json={"translations": {"title": "x"}}).status_code == 404
        assert client.put("/products/1/translations/zz", json={"translations": {"title": "x"}}).status_code == 404

    def test_put_to_inactive_language_is_422(self, client, db, imported):
        with db.begin() as c:
            c.execute(text("UPDATE languages SET is_active = false WHERE language_code = 'bg'"))
        assert client.put("/products/1/translations/bg", json={"translations": {"title": "x"}}).status_code == 422

    def test_unicode_roundtrip(self, client, imported):
        client.put("/products/1/translations/bg", json={"translations": {"title": "Мебели — Möbel 日本"}})
        assert client.get("/products/1?lang=bg").json()["translations"]["title"] == "Мебели — Möbel 日本"


# ============================================================================================ health
class TestHealth:
    def test_liveness_never_touches_dependencies(self, client):
        assert client.get("/health").json() == {"status": "ok"}

    @staticmethod
    def fake_redis(monkeypatch, ok):
        class Fake:
            async def ping(self):
                if not ok:
                    raise ConnectionError("down")

            async def aclose(self):
                pass

        monkeypatch.setattr("app.api.routes.health.aioredis.from_url", lambda *a, **k: Fake())

    def test_ready_when_dependencies_are_up(self, client, monkeypatch):
        self.fake_redis(monkeypatch, True)
        r = client.get("/ready")
        assert r.status_code == 200 and r.json() == {"status": "ready", "checks": {"postgres": "ok", "redis": "ok"}}

    def test_not_ready_when_redis_is_down(self, client, monkeypatch):
        self.fake_redis(monkeypatch, False)
        r = client.get("/ready")
        assert r.status_code == 503 and r.json()["checks"]["redis"].startswith("error") and r.json()["checks"]["postgres"] == "ok"
