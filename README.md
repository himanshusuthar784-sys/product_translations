# Multi-language product translations: FastAPI + Celery + Redis + Postgres

A stateless, horizontally scalable service that moves translatable product fields out of the legacy `products` table into a generic translations table, and migrates them per language with asynchronous, idempotent, resumable background jobs and progress APIs.

> **New to the code?** Read **[docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md)** first. It explains the request flow, the idempotency key, duplicate requests, failure handling and how to test, in plain language.

## Quick start

```bash
cp .env.example .env
docker compose up -d si-db
python scripts/extract_sql_from_mhtml.py shop_products_1001_test.sql.mhtml data/shop_products_1001_test.sql
docker compose run --rm --no-deps tools python scripts/load_legacy.py     # 10,000 legacy rows -> schema `legacy`
docker compose up -d api worker beat                                       # also runs `alembic upgrade head`
```
- API + interactive docs: <http://localhost:8010/docs> (host port 8010 because 8000 is taken by another project here)
- Postgres: `localhost:5433` (db `translations`, user `si`). Redis: the existing `tdebiz-redis-1`, **DB 5**, key prefix `si:`
- Scale workers: `docker compose up -d --scale worker=4`
- Tests: `docker compose run --rm --no-deps tools pytest -q` (own database `translations_test`; never touches your data)
- Postman: [`postman/translations-migration.postman_collection.json`](postman/translations-migration.postman_collection.json)

Run order: `en` first (imports legacy data into `products` + English translations), then any other language (`fr`, `bg`, ...).

## API

| Method and path | Purpose |
|---|---|
| `POST /migration/start` | Create a job for a language, returns **202** + `job_id` immediately |
| `GET /migration/status/{job_id}` | `status`, `processed_records`, `total_records`, `progress_percentage`, batch counts, ETA |
| `GET /migration/jobs`, `GET /migration/jobs/{id}/batches` | Monitoring (filters and paging) |
| `POST /migration/jobs/{id}/cancel`, `/retry` | Stop a job / re-run only its failed batches |
| `GET/POST /languages` | List languages / add one through the API (data only: no schema change, no deploy) |
| `GET /products/{id}?lang=fr` | Read path with fallback to the default language |
| `PUT /products/{id}/translations/{lang}` | Human edit; accepts **any new key** with no schema change |
| `GET /health`, `GET /ready` | Liveness / readiness (Postgres + Redis) |

## Architecture decisions

### Why this schema
`products` keeps only non-translatable attributes. Everything translatable is a **row** in `product_translations(product_id, language_id, key, value jsonb, source)`:

- **New language** = one `POST /languages` call (it stores a row, nothing more). **New field** = a new `key` value via the API. Neither needs DDL, a deploy, or a data migration. Both are proven by a test that fingerprints the whole schema before and after.
- `value` is `jsonb {"value": ...}`, so a field can be text, a list (tags, SEO keywords) or an object (specifications).
- `source` (`fallback < import < machine < human`, a Postgres enum whose declaration order *is* the precedence) stops a job from overwriting a stronger translation. A human edit survives every re-run.
- Integrity is in the database, not just in code: unique `(shop_id, product_id)`, composite PK, FKs (`ON DELETE CASCADE` for products, `RESTRICT` for languages), exactly one default language, and `CHECK (value ? 'value')`.

Deliberate choices that differ from the initial sketch:
- **`product_translations` has no surrogate `id`.** The composite PK `(product_id, language_id, key)` is the primary key, the `ON CONFLICT` target and the lookup path, in one index. At 10M products Ã— 50 languages Ã— 5 fields that is about 2.5B rows, so a second index is very expensive.
- **`products` index is `(shop_id, created_at DESC, id DESC)`, not `(shop_id, product_id, created_at, id)`.** After a unique `(shop_id, product_id)` the later columns can never help ordering. The new one serves "latest products of a shop" with keyset pagination (verified with `EXPLAIN`: no sort step).

### Why this processing strategy
- **Postgres is the single source of truth** for job and batch state (`migration_jobs`, `migration_batches`). Redis is only a mailbox, with no result backend. Losing Redis loses messages, never state. Workers and API hold nothing locally, so they scale freely.
- **Keyset batches, not OFFSET.** Planning is one ordered pass over the PK index. Batches hold a fixed number of *rows* even when ids are sparse (the dump has ids 392 to 23101).
- **One transaction per batch** covers the data and the progress mark. No half-done batch exists, and progress is always true.
- **Atomic claim, heartbeat, reaper.** A claim is one `UPDATE ... WHERE status='pending'`, so duplicate deliveries do nothing. A killed worker is detected by its stale heartbeat and its work is re-queued. The reaper is guarded by a Postgres advisory lock so several beat instances are safe.
- **Delivery is at-least-once, effects are exactly-once.** `acks_late` + `reject_on_worker_lost`, with idempotent writes (`ON CONFLICT ... DO UPDATE ... WHERE IS DISTINCT FROM`).
- **Set-based SQL for the import** (`INSERT ... SELECT ... CROSS JOIN LATERAL (VALUES ...)`): no Python row loop, constant memory.
- **Pluggable `TranslationProvider`.** The default copies English as `source='fallback'`; a real MT/LLM provider is one class + `TRANSLATION_PROVIDER=<name>`.

### Connection management
- API: async SQLAlchemy + asyncpg, pool 10 + 5 overflow, `pool_pre_ping`, recycle 30 min.
- Worker: sync SQLAlchemy + psycopg3, engine created **per child process** (never shared across a fork), **pool 2, overflow 0**. The 2 are one connection for the batch transaction plus one short-lived autocommit connection for heartbeats. Connection budget = `workers Ã— concurrency Ã— 2`.
- Every connection sets `application_name`, `statement_timeout` and `lock_timeout`.

## Performance strategy

| Table | Indexes (nothing else) | Why |
|---|---|---|
| `product_translations` | PK `(product_id, language_id, key)` | Serves the upsert, "all translations of a product" and "a product in one language" by prefix. A test asserts this inventory. |
| `products` | PK `(id)`, UNIQUE `(shop_id, product_id)`, `(shop_id, created_at DESC, id DESC)` | Business key + shop listing. No single-column `shop_id` / `is_deleted` / `google_status` indexes (the legacy dump had them). |
| `languages` | UNIQUE `(language_code)`, partial UNIQUE `(is_default) WHERE is_default` | |
| `migration_jobs` | partial UNIQUE `(language_id, job_type) WHERE active`, partial UNIQUE `(idempotency_key)`, `(created_at DESC)` | The active-job index is the duplicate-request guard. |
| `migration_batches` | UNIQUE `(job_id, batch_no)`, `(job_id, status) INCLUDE (processed_count)`, partial `(heartbeat_at) WHERE running` | Progress is an index-only scan. The reaper index only holds in-flight rows. |

Other measures: empty source values are never materialised (39,983 rows per language instead of 50,000); unchanged rows are not rewritten (zero dead tuples on re-runs, tested); batch size is configurable; progress is computed from batch rows, so there is no hot counter row.

Measured on this machine (10,000 products, batches of 2000, 1 worker container): `en` import **0.74 s**, `fr` **1.05 s**, `bg` **0.98 s** (about 3 s for all three); about 0.25 to 0.5 s per 2,000-row batch. Memory: worker container about 249 MiB, API about 74 MiB. Memory is bounded by design (set-based SQL for the import, 500-item provider chunks, one batch per process at a time, tiny messages, no result backend, capped pools); see [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md) Q19 and Q20.

## Scalability

| Growth | What to do |
|---|---|
| **More languages (50+)** | Insert rows. Jobs are per language and independent, so they run concurrently. The partial unique index allows one active job per language. |
| **More fields** | Add the key to `TRANSLATABLE_FIELDS` (one line) if it comes from a legacy column, or write it through the API or provider. No DDL. |
| **10M+ products** | About 5,000 batches per language, which is trivial for the planner. Add workers (`--scale worker=N`). Raise `batch_size` as needed. |
| **Translations table at ~2.5B rows** | **Hash-partition by `product_id`** (the PK already contains it, so no key change). Add `(language_id, key, product_id)` *only if* a by-language query appears. Per-language progress already comes from the job tables, not `COUNT(*)`. |
| **Many replicas** | Put **PgBouncer** (transaction mode) in front of Postgres when `replicas Ã— pool > max_connections`. Add read replicas for `GET /products`. |
| **Hot reads** | Cache `(product, lang)` responses in Redis; invalidate on translation writes. |
| **Slow external translator** | The provider contract already handles batching, `TransientError` retries with backoff, and heartbeats per chunk. Add a rate limiter inside the provider. |

## Assumptions
- Legacy timestamps are UTC (MySQL `timestamp` carries no zone).
- An empty or whitespace-only source value means "no translation". Reads fall back to the default language.
- `handle` stays in `products` as specified. If slugs must be localised, add `handle` as a translation key later, with no DDL.
- `tags` is stored as the original comma-separated string (lossless); it can become a JSON array later.
- **Data-quality caveat:** 94 of the 10,000 source rows contain U+FFFD replacement characters (for example `MÃ¶bel` became `Mï¿½ï¿½ï¿½ï¿½bel`). The supplied file is a browser-saved preview of a latin1 dump, so the original characters are already lost. They are loaded verbatim and reported by the loader. If you have the raw `.sql` from the Dropbox link, run `load_legacy.py` on it directly.
- The dump has `tags`, `meta_title`, etc. but no `description` fields, so the initial keys are `title, meta_title, brand, tags, google_product_category`.
- Docker: the existing `tdebiz-db-1` is **not touched**; a dedicated Postgres container is used. Redis is shared with that project, isolated by DB index 5 and the `si:` key prefix.

## Deviations from the original plan (and why)
- The reaper re-publishes lost queue messages using a per-batch `enqueued_at` column (database only) instead of inspecting Redis queue length. It is deterministic and needs no broker introspection.
- `translation_source` enum order is `fallback < import < machine < human`, so precedence is a plain `<=` in SQL.

## Tests

`328 passed` (plus 6 live-stack tests that run with `E2E_BASE_URL`): unit tests (parser, extractor, validation, backoff and failure classification, providers, pool and logging config), integration tests against real Postgres (schema and every integrity rule, `EXPLAIN` checks, Alembic up/down/up, import and translate processors, planning, claim and heartbeat protocol, reaper, task-level failure injection, true multi-thread races, duplicate and idempotent API requests, cancel and retry, 10k-row end-to-end), and a black-box suite for the running stack.
Two real bugs were caught by the tests (a 500 on malformed requests, a whitespace edge case in the import) and one by a live kill-a-worker drill (worker logs were being swallowed by Celery). All are fixed and covered by regression tests.

## AI usage
This was built with **Claude (Claude Code)** as a pair-programmer: it profiled the supplied dump (row count, NULL and empty ratios, the corrupted characters), proposed and challenged the schema and index choices (the redundant products index, the surrogate key), wrote the migrations, services, workers and test suite, and ran the live failure drills. Design decisions and trade-offs were reviewed with the owner before implementation (provider strategy, index shape, primary key).

## Layout
```
app/api         FastAPI app, routes          app/services    job_api (API side), job_runtime (worker side), fields
app/workers     celery_app, tasks, processors/, providers/      app/models      SQLAlchemy models
alembic/        0001 core schema + seed, 0002 job tables        scripts/        extract + load legacy dump
tests/          unit, integration, e2e       docs/           HOW_IT_WORKS.md      postman/
```
