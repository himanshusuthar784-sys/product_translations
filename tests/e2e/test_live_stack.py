"""Black-box smoke test against the RUNNING docker-compose stack (real Redis + real Celery workers).

    docker compose up -d
    E2E_BASE_URL=http://api:8000 docker compose run --rm --no-deps -e E2E_BASE_URL=http://api:8000 tools pytest tests/e2e -q

Skipped unless E2E_BASE_URL is set. Requires the legacy table to be loaded (scripts/load_legacy.py).
Idempotent by design: re-running it only re-confirms the same end state.
"""
import os
import time

import httpx
import pytest

BASE = os.environ.get("E2E_BASE_URL")
pytestmark = [pytest.mark.e2e, pytest.mark.skipif(not BASE, reason="E2E_BASE_URL not set")]

EXPECTED_ROWS_PER_LANGUAGE = 39_983     # real 10k dataset: 10000 title + 10000 brand + 9981 tags + 9992 category + 10 meta_title


@pytest.fixture(scope="module")
def http():
    with httpx.Client(base_url=BASE, timeout=30) as c:
        yield c


def wait_done(http, job_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = http.get(f"/migration/status/{job_id}").json()
        if st["status"] in ("completed", "completed_with_errors", "failed", "cancelled"):
            return st
        time.sleep(0.5)
    raise AssertionError(f"job {job_id} did not finish in {timeout}s: {st}")


def start_and_wait(http, code):
    r = http.post("/migration/start", json={"language_code": code, "batch_size": 2000})
    if r.status_code == 409:                                      # an identical job is already running: join it
        job_id = r.json()["error"]["job_id"]
    else:
        assert r.status_code == 202, r.text
        job_id = r.json()["job_id"]
    return wait_done(http, job_id)


def test_stack_is_healthy(http):
    assert http.get("/health").json() == {"status": "ok"}
    assert http.get("/ready").json()["checks"] == {"postgres": "ok", "redis": "ok"}


@pytest.mark.parametrize("code", ["en", "fr", "bg"])
def test_full_migration_per_language(http, code):
    st = start_and_wait(http, code)
    assert st["status"] == "completed", st
    assert st["processed_records"] == st["total_records"] == 10_000 and st["progress_percentage"] == 100.0
    assert st["batches"]["succeeded"] == 5 and st["batches"]["failed"] == 0


def test_data_is_complete_and_readable_in_every_language(http):
    for code in ("en", "fr", "bg"):
        p = http.get("/products/392", params={"lang": code}).json()
        assert p["translations"]["title"] == "IMAX Nordiak Glass Vases - Set of 2" and p["fallback_keys"] == []


def test_duplicate_start_while_running_is_rejected_or_a_clean_rerun(http):
    first = http.post("/migration/start", json={"language_code": "fr", "batch_size": 500})
    second = http.post("/migration/start", json={"language_code": "fr", "batch_size": 500})
    assert first.status_code == 202
    assert second.status_code in (409, 202)                       # 409 while first is active; 202 only if it already finished
    if second.status_code == 409:
        assert second.json()["error"]["job_id"] == first.json()["job_id"]
    wait_done(http, first.json()["job_id"])
    if second.status_code == 202:
        wait_done(http, second.json()["job_id"])
