# Test and verification report

What was run, what it proves, and the real evidence. Everything below was produced on this machine against the real Docker stack (Postgres 16.3, Redis 7, Celery 5.4, FastAPI), not mocked. Date of the runs: 2026-10-12.

## 1. Result at a glance

| Check | Result |
|---|---|
| Automated suite (unit + integration), 3 full runs after the final code change | **2 fully green (328 passed, 0 failed)**; **1 run had a single unexplained failure** (327 passed, 1 failed; see section 6). The 6 live-stack tests are skipped in these runs by design |
| Live-stack black-box tests (real Redis + real Celery workers, real 10,000-row data) | **6 passed** |
| Real 10,000-product migration, three languages | **39,983 translation rows per language, 0 duplicates, 0 mismatches vs legacy** |
| Kill-a-worker drill on the live stack | Job finished at **400 of 400 batches (100 %)** with no duplicate or lost data |
| Total tests defined | **334** (235 integration, 93 unit, 6 live-stack) |

## 2. How to reproduce

```bash
# everything except the live-stack tests (uses its own database `translations_test`, never touches real data)
docker compose run --rm --no-deps tools pytest -q

# live-stack tests (needs: docker compose up -d, legacy data loaded)
docker compose run --rm --no-deps -e E2E_BASE_URL=http://api:8000 tools pytest tests/e2e -q
```

| Run | Command | Outcome |
|---|---|---|
| Full suite, first run after the last code change | `pytest -q` | 327 passed, **1 failed**, 6 skipped in 105 s (`test_filters_and_pagination`, see section 6) |
| Full suite, next run | `pytest -q` | **328 passed, 6 skipped** in 110 s |
| Full suite, run after that | `pytest -q` | **328 passed, 6 skipped** in 121 s |
| Live stack | `pytest tests/e2e` with `E2E_BASE_URL` | **6 passed** in 12 s |
| API file repeated 4 times in a row | `pytest tests/integration/test_api.py` | 68 passed, 4 of 4 times |
| Concurrency tests repeated 3 times | `-k "concurrent or exactly_one"` | 3 passed, 3 of 3 times |

## 3. What the 334 tests cover

| File | Tests | What it proves |
|---|---:|---|
| `unit/test_load_legacy_parser.py` | 18 | The MariaDB dump parser: quote and backslash escapes, NULLs, commas and parentheses inside strings, corrupted characters kept verbatim, malformed input rejected |
| `unit/test_extract_mhtml.py` | 4 | Rebuilding the SQL from the saved Dropbox page; fails loudly if no SQL is found |
| `unit/test_fields_and_schemas.py` | 30 | Field whitelist (SQL-injection attempt is just an "unknown field"), language-code normalisation, translation-key rules, request limits |
| `unit/test_runtime_helpers.py` | 25 | Which errors are retryable vs permanent, backoff bounds and jitter, progress-percentage maths |
| `unit/test_providers_and_enums.py` | 8 | Provider contract and registry; the enum order that encodes `fallback < import < machine < human` |
| `unit/test_infrastructure_config.py` | 8 | Pool sizes (worker pool 2, overflow 0), engine rebuilt after fork, JSON logs carry job/batch context, Celery reliability settings, **regression for the swallowed-logs bug** |
| `integration/test_schema.py` | 25 | Real Alembic schema: exact index inventory (no redundant indexes), `EXPLAIN` shows the PK and listing index are used, every integrity rule, Alembic up, down and up again |
| `integration/test_import_processor.py` | 24 | Legacy to products + English translations: mapping, UTC timestamps, blanks skipped, **re-run rewrites zero rows** (checked by row `xmin`), source precedence, whole-batch rollback |
| `integration/test_translate_processor.py` | 18 | Translation via providers: fallback copy, machine upgrades fallback, human never overwritten, chunking and heartbeats, provider errors leave no partial data |
| `integration/test_planning.py` | 19 | 10,000 rows with batch size 2000 gives exactly 1-2000, 2001-4000, ... 8001-10000; sparse ids; remainder; crash during planning; idempotent re-plan |
| `integration/test_batch_lifecycle.py` | 33 | Atomic claim (20 threads, exactly 1 wins), heartbeat, stale takeover, lost-lock rollback, retry vs fail rules, job finalisation (20 threads, exactly 1 winner) |
| `integration/test_reaper.py` | 17 | Dead worker repaired, lost messages re-published, lost plan re-published, advisory lock keeps beats exclusive, end-to-end beat recovery |
| `integration/test_tasks.py` | 20 | Real Celery task functions with injected failures: transient retry, permanent failure, attempts exhausted, atomic rollback, lost lock, cancel, duplicate deliveries, **8 racing workers over the same 20 batches** |
| `integration/test_api.py` | 68 | Validation, start and status flow, **10 simultaneous identical requests gives 1 job**, idempotency keys, Redis outage, cancel and retry, listings, languages, products, health |
| `integration/test_scale_end_to_end.py` | 5 | The assessment scenario: 10,000 products, batch 2000, three languages, full re-run changes nothing, no dead tuples |
| `integration/test_load_legacy_db.py` | 6 | Loader: type conversion, idempotent reload, bad input leaves the existing data untouched |
| `e2e/test_live_stack.py` | 6 | Black-box against the running stack with real Redis and workers |

## 4. Real evidence from the live system

### 4.1 Database state after migrating the real 10,000 products

Query results taken from the running Postgres (`translations` database):

```
legacy rows                          10000
products                             10000
translations en / import             39983
translations fr / fallback           39983
translations bg / fallback           39983
duplicate (product, language, key)       0
jobs completed                           9
batches succeeded                      457   (none failed)
```
Per key for English: `title` 10000, `brand` 10000, `google_product_category` 9992, `tags` 9981, `meta_title` 10. That is 39,983, exactly the figure predicted before any code was written from profiling the dump (empty source values are intentionally not stored).

Integrity cross-checks against the legacy table:
```
products whose shop_id / product_id / handle / is_deleted differ from legacy        0
English titles that differ from the legacy title                                    0
```

### 4.2 The assessment example, reproduced
10,000 records with batch size 2000 produced exactly five batches (1-2000, 2001-4000, 4001-6000, 6001-8000, 8001-10000). Status polling showed `processed_records` climbing and the job ending at `completed 10000/10000, 100.0 %, batches succeeded = 5`.

### 4.3 Duplicate requests (real HTTP, live stack)
```
POST /migration/start {fr}   ->  202 {"job_id":"204ae29a-...","status":"pending","created":true}
POST /migration/start {fr}   ->  409 {"error":{"code":"job_already_active","job_id":"204ae29a-..."}}   <- same job
POST /migration/start {fr} before en was imported -> 422 base_import_required
```
Automated, with 10 threads firing at the same instant: exactly one `202` and nine `409`, all pointing at one job. With one shared idempotency key: ten `200/202` answers, one `created:true`, one job.

### 4.4 Kill-a-worker drill (live stack, real `docker kill`)
Setup: two workers, a `fr` job of 400 batches (batch size 25), staleness windows temporarily set to 20 s and 30 s (production defaults are 300 s and 900 s). One worker was killed one second after the start.

```
12:03:04 running   4125/10000  41.25%  ok=165 run=4 pend=231 fail=0
12:03:12 running   4750/10000  47.5%   ok=190 run=2 pend=208 fail=0
12:03:20 running   4750/10000  47.5%   (stalled: dead worker's batches stuck)
12:03:36 running   4750/10000  47.5%
12:03:44 running   4925/10000  49.25%  <- reaper fires, work resumes
12:03:52 completed 10000/10000 100.0%  ok=400 run=0 pend=0 fail=0
```
Afterwards in the database:
```
attempts = 1 : 398 batches        attempts = 2 : 2 batches          (only 2 batches needed a retry)
batches republished by the reaper : 210
total duration                    : 46.7 s
rows per language                 : bg 39983, en 39983, fr 39983    (no duplicates, nothing lost)
```

### 4.4b Timings and memory (live stack, real data)
Batch size 2000, 5 batches per language, 1 worker container (4 processes):

| Job | Whole job | Avg batch | Max batch |
|---|---:|---:|---:|
| `en` import, 10,000 records | 0.74 s | 0.25 s | 0.26 s |
| `fr` translate, 10,000 records | 1.05 s | 0.51 s | 0.54 s |
| `bg` translate, 10,000 records | 0.98 s | 0.47 s | 0.51 s |

Planning 10,000 rows: about 0.4 s. A single 5,000-row batch: about 0.9 to 1.1 s. Kill drill (400 tiny batches plus a dead worker): 46.7 s total.

Memory: `worker` container about 249 MiB (183 MiB idle), `api` about 74 MiB, `beat` about 62 MiB, Postgres about 148 MiB. App connections to Postgres: 8 from workers (4 processes times pool of 2), 1 from the API. These are snapshots, not a controlled peak test. Details of how memory is kept bounded are in [HOW_IT_WORKS.md](HOW_IT_WORKS.md), Q20.

Test suite timing: unit tests 93 in about 4 s; full suite (unit plus integration) about 110 to 120 s for 328 tests.

### 4.5 Live-stack black-box tests
`pytest tests/e2e` against the running API: **6 passed**: stack healthy; full migration of `en`, `fr`, `bg` each ends `completed` with 10,000 of 10,000 records and 5 succeeded batches; a real product is readable in all three languages; duplicate start is rejected or runs cleanly.

### 4.6 Services and endpoints (live)
Containers up: `si-db` (healthy, port 5433), `api` (port 8010), `worker`, `beat`. `GET /health`, `/ready`, `/docs`, `/redoc` and `/openapi.json` all return HTTP 200; `/ready` reports Postgres and Redis both `ok`. The other project's containers (`tdebiz-*`) were never touched and are still up.

## 5. Bugs the testing found (all fixed, each with a regression test)

| # | Found by | Problem | Fix |
|---|---|---|---|
| 1 | API tests | Every malformed request returned **500** instead of a clean 422 (my validation handler passed arguments FastAPI does not accept) | Handler now returns only `loc`, `msg`, `type` |
| 2 | Import tests | A value consisting only of tabs or newlines was imported as a real translation | Blank check now uses a "contains non-whitespace" regex |
| 3 | Live duplicate request | `409` path crashed with a 500 (`MissingGreenlet`): ORM objects were read after a rollback | Plain values are copied out before the commit |
| 4 | Live kill drill | Worker task logs never appeared (Celery's stdout redirection swallowed them) | Celery no longer redirects stdout; our JSON logging owns the setup |
| 5 | Edge-case review | Reusing one idempotency key for a different language silently replayed the other language's job | Now `409 idempotency_key_reused` |
| 6 | Measuring timings | Batch `finished_at` used `now()`, which is the transaction *start*, so batch durations were under-reported | Uses `clock_timestamp()`; the 138 tests touching it re-ran green |

Several test-only mistakes were also found and corrected (for example a test that expected a bitmap scan plan on an empty table); they are not product bugs.

## 6. Honest notes and limits

- **One unexplained failure.** In the first of three full-suite runs after the final change, 1 test (`TestListings::test_filters_and_pagination`, which checks the ordering of three jobs created in quick succession) failed once with 327 passed. I did not capture its error message, so I cannot say exactly which assertion failed. It was **not reproduced**: that test passed 4 of 4 times alone, the whole API file passed 4 of 4 times, and the two following full runs were 328 passed. It was not root-caused. The likely suspect is an ordering tie or timing in that listing test, not the production code, but that is a hypothesis, not a finding. If it recurs, look at the `ORDER BY created_at DESC` in `list_jobs`.
- **Real translation is not exercised.** The default provider copies English (`source = fallback`). The provider plug-in path is tested with fake providers; no real machine-translation service was called.
- **The kill-a-worker drill was manual**, not an automated test (it needs real containers). Its building blocks (stale takeover, reaper republish, lost lock) are all automated.
- **Not tested:** a full Postgres outage mid-run (reasoned about in the FAQ, recovery is by the reaper), authentication (not implemented), very large volumes (the largest real run is 10,000 products; the 10M-product design is argued in the README, not benchmarked).
- **Data-quality caveat:** 94 of the 10,000 source rows contain corrupted characters inherited from the supplied file; they are loaded as provided and reported.
