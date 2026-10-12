# How it works: the request-to-database flow, in plain language

This document explains what happens from the moment the frontend calls the API until the data is in the database, what happens when something goes wrong, and what happens when the same request arrives twice. It ends with step-by-step instructions to test everything yourself.

---

## 1. The big picture

Five pieces work together. Only **Postgres** remembers anything. The rest can be stopped, killed or multiplied at any time.

| Piece | What it is | Its one job | Remembers state? |
|---|---|---|---|
| **API** (FastAPI) | `app/api/` | Accepts requests, validates them, writes a *job* row, answers immediately | No |
| **Redis** | the existing `redis-1`, DB index 5 | A mailbox: carries "please do this" messages from API to workers | No (only messages in transit) |
| **Worker** (Celery) | `app/workers/` | Takes messages from Redis and does the actual copying and translating | No |
| **Beat + reaper** | one small scheduler | Every 60 s checks for stuck or lost work and repairs it | No |
| **Postgres** | dedicated `si-db` container, port 5433 | Stores the products, the translations, **and the progress of every job** | **Yes, the only one** |

```
 Frontend
    |  POST /migration/start
    v
  API  ------ 1. write job row (status=pending) --------------> Postgres
    |
    '------- 2. put message "plan job X" ----> Redis ----> Worker
                                                              |
                  3. worker cuts the work into batches -------+--> Postgres (batch rows)
                  4. worker puts one message per batch -------+--> Redis
                                                              |
        Worker(s), any number, in parallel:                   v
          5. claim a batch -> copy/translate -> save -> mark done   (ONE transaction)
          6. last batch done? -> mark job completed

  Frontend polls  GET /migration/status/{job_id}  -> API reads batch rows from Postgres

  Beat every 60 s:  "any batch stuck? any message lost?"  -> repair
```

**Why this design is safe to scale:** API and workers keep nothing in memory or on disk that matters. If a container dies, another one picks up from what is written in Postgres. You can run 1 or 50 of each.

---

## 2. The two job tables

Everything about progress lives in two tables:

**`migration_jobs`**: one row per "migrate language X" request

| column | meaning |
|---|---|
| `id` | the **job_id** returned to the frontend (UUID created by the server) |
| `language_id` | which language is being migrated |
| `job_type` | `import_base` (for `en`: legacy table to products and translations) or `translate` (for fr, bg and others: copy from `en`) |
| `status` | `pending`, `planning`, `running`, `completed`, `completed_with_errors`, `failed`, `cancelled` |
| `total_records`, `total_batches` | filled in once planning is done |
| `idempotency_key` | optional label from the client (see section 4) |

**`migration_batches`**: one row per slice of work (for example products 1 to 2000)

| column | meaning |
|---|---|
| `batch_no`, `range_start`, `range_end` | which slice (product id range) |
| `status` | `pending`, `running`, `succeeded`, `failed` |
| `attempts` | how many times a worker has tried this batch |
| `locked_by`, `heartbeat_at` | which worker owns it and when it last said "I'm alive" |
| `processed_count` | filled only when the batch succeeded; this is what the progress bar adds up |
| `error` | the last error message, for diagnosis |
| `enqueued_at` | when its message was last put in Redis |

---

## 3. Life of one request, step by step

Example: the frontend asks to migrate **French**.

### Step 1: Frontend calls `POST /migration/start`
```json
{ "language_code": "fr", "batch_size": 2000, "idempotency_key": "fr-import-2026-10-12" }
```
Only `language_code` is required. Code: [`app/api/routes/migration.py`](../app/api/routes/migration.py).

### Step 2: API validates (before touching anything)
Code: `create_job` in [`app/services/job_api.py`](../app/services/job_api.py). In order:

1. Is `language_code` a known, active language? No: **404** or **422**.
2. Was an `idempotency_key` sent that we already know? Yes: return the **original job**, create nothing (see section 4).
3. Are `fields` valid (only whitelisted names like `title`, `brand`) and is `batch_size` sane? No: **422**.
4. For a non-default language: has the English import happened? Is it still running? No products yet: **422**. Import still running: **409**.

### Step 3: API writes the job row, then answers
It inserts one `migration_jobs` row with `status = pending`, then publishes the message *"plan job X"* to Redis and returns:

```
HTTP 202 Accepted
{ "job_id": "e18928dc-...", "status": "pending", "language_code": "fr", "job_type": "translate", "created": true }
```
Plus a `Location: /migration/status/e18928dc-...` header. **The API does not wait for any real work.** This takes milliseconds, even for 10 million products.

> The job row is written **first**, the message **second**. If Redis is down at that moment, the job simply stays `pending` and the reaper re-sends the message within about a minute (section 6). The request is never lost.

### Step 4: A worker "plans" the job
Code: `plan_job` in [`app/services/job_runtime.py`](../app/services/job_runtime.py), wrapped by the task in [`app/workers/tasks.py`](../app/workers/tasks.py).

1. Marks the job `planning`.
2. Reads the source ids in order and cuts them into slices of `batch_size` *rows*. This is a keyset, not OFFSET, so it is fast and stays fast with millions of rows. 10,000 products with batch size 2000 give 5 batches: 1 to 2000, 2001 to 4000, and so on. (The real data has gaps in its ids, such as 392 to 23101, so slices are counted by rows, not by id distance.)
3. Inserts the batch rows. `UNIQUE (job_id, batch_no)` plus `ON CONFLICT DO NOTHING` means that planning twice never creates duplicates.
4. Sets `total_records`, `total_batches`, and the job to `running`.
5. Publishes one message per batch to Redis: *"process batch 456"*.

### Step 5: Workers process batches in parallel
Any number of workers each take one message at a time (`prefetch = 1`). For each batch, code `process_batch` in `tasks.py`:

1. **Claim** (one atomic SQL statement):
   *"If this batch is `pending`, and its job is `running`, mark it `running`, add 1 to `attempts`, write my name and the time."*
   If the statement changes 0 rows (someone else has it, it is already done, or the job was cancelled) the worker does nothing and moves on.
2. **Do the work inside ONE database transaction**:
   - `en`: copy product columns into `products`, and title/brand/tags/meta_title/category into `product_translations`. Pure SQL, no Python loop.
   - `fr`, `bg`: read the English translations of this id range, hand them to the translation provider (default: copy English and mark it `fallback`), write the results.
   - Writes use `INSERT ... ON CONFLICT DO UPDATE ... WHERE value IS DISTINCT FROM ...`, so running the same batch again changes nothing.
   - While working, the worker sends a **heartbeat** ("still alive") on a separate connection.
3. **Mark the batch `succeeded` and store `processed_count`**, in the **same transaction** as the data. Either the data and the progress are both saved, or neither is.
4. **Check if the job is finished**: "are there any batches still `pending` or `running`?" If not, one atomic statement sets the job to `completed` (or `completed_with_errors` / `failed`). Only one worker can win this.

### Step 6: The frontend polls `GET /migration/status/{job_id}`
The API sums the batch rows and replies:

```json
{
  "job_id": "e18928dc-...", "status": "running",
  "processed_records": 6000, "total_records": 10000, "progress_percentage": 60.0,
  "failed_records": 0,
  "batches": { "total": 5, "pending": 1, "running": 1, "succeeded": 3, "failed": 0 },
  "elapsed_seconds": 4.2, "eta_seconds": 2.8
}
```
`processed_records` counts **only fully finished batches**, so the bar never moves backwards and never over-reports.

---

## 4. The idempotency key

**What it is.** A label the **client** chooses to say *"this is the same request as before."* It is optional.

**Who creates it.** Not the server. The frontend (or whoever calls the API) makes it up, for example `"fr-import-2026-10-12"` or a UUID generated once per user click. The important rule is: **if you retry because of a timeout, send the same key; for a genuinely new run, send a new key.**
(Do not confuse it with `job_id`: that one **is** created by the server, in `create_job`, as a random UUID.)

**What the server does with it.**
1. It is stored on the job row in `migration_jobs.idempotency_key`.
2. The database enforces `UNIQUE (idempotency_key)` (only where a key exists), so two jobs can never share a key, even if two requests arrive at the same instant.
3. On every `start`, the API first looks the key up:
   - **Found:** return that existing job (**HTTP 200**, `"created": false`). Nothing new is created and nothing is published. This works even after the job has finished, so a client retrying an old request never triggers a second run.
   - **Not found:** create a new job (**HTTP 202**, `"created": true`).

```
POST start {key:"abc"}  ->  202 created:true   job_id=J1     (job J1 runs)
POST start {key:"abc"}  ->  200 created:false  job_id=J1     (same job, nothing new)
POST start {key:"abc"}  ->  200 created:false  job_id=J1     (even after J1 finished)
POST start {key:"xyz"}  ->  202 created:true   job_id=J2     (new key = new run)
```

---

## 5. What if the same request arrives again?

There are **three layers of protection**, each handling a different case. None relies on application memory, which is why this still works with many API replicas.

| Situation | What happens | Which protection |
|---|---|---|
| Same request, **same idempotency key**, any time | **200**, returns the original job, creates nothing | Key lookup + `UNIQUE(idempotency_key)` |
| Same language requested again **while a job is active**, no key | **409** with `{"error":{"code":"job_already_active","job_id":"<running job>"}}`; nothing is created or published | `UNIQUE (language_id, job_type) WHERE status IN (pending, planning, running)`, enforced by the database |
| **10 identical requests at the same millisecond** | Exactly one **202**, nine **409**s that all point at the same `job_id` (tested) | Same database index: only one INSERT can win |
| Same language requested again **after the job finished**, no key | **202**, a new job runs. It is a safe re-run: it writes nothing new because every row is compared first (`IS DISTINCT FROM`). The job history is kept | Re-runs are no-ops by design |
| Translating `fr` while the `en` import is still running | **409** `base_import_running` (translating half-imported data would be wrong) | Check in `create_job` |
| Translating `fr` before `en` was ever imported | **422** `base_import_required` | Check in `create_job` |
| The **message** is delivered twice by Redis | The second copy finds the batch already claimed or done: claim changes 0 rows, the worker skips it ("`batch_skipped`"). No double work | Atomic claim |
| The **plan message** is delivered twice | Planning is idempotent (`ON CONFLICT DO NOTHING`; a job that is already `running` is ignored) | Planning guard |
| Two workers grab the **same batch** at once | One claim wins; the other gets nothing | Atomic claim (tested with 20 simultaneous claimers) |
| A worker is slow, gets declared dead, and **finishes anyway** | Its final "mark succeeded" fails because it no longer owns the batch, so its transaction is **rolled back**. Only the new owner's result is saved | Lock check inside the same transaction |
| Same **idempotency key** but a **different language** | **409** `idempotency_key_reused` with the original `job_id`; nothing created | Check in `create_job` |
| Frontend retries `retry` / `cancel` | Retry on a job that is not failed gives **409**; cancel on a finished job gives **409**. Never a second effect | Status checks in the SQL `WHERE` |

**Bottom line:** the same request can be sent as many times as you like, from as many places as you like, and the result is always one job, one correct set of data, never duplicates. The final data cannot be duplicated even if every protection above were bypassed, because `product_translations` has a composite primary key `(product_id, language_id, key)`.

---

## 6. Errors and failures: what happens and who fixes it

| What goes wrong | What the system does | Recovers by itself? |
|---|---|---|
| **Temporary database problem** (connection drop, deadlock, timeout) | The worker rolls back, puts the batch back to `pending`, and Celery re-delivers it after a growing delay (about 5 s, 10 s, 20 s, with random jitter). `attempts` counts the tries | Yes, up to 5 attempts (`MAX_BATCH_ATTEMPTS`) |
| **Translation provider throttles or times out** (`TransientError`) | Same as above | Yes |
| **Bad data / bug** (a permanent error, e.g. a constraint violation) | The batch is marked `failed` at once with the error text; no pointless retries. **Other batches keep going.** At the end the job is `completed_with_errors` (or `failed` if every batch failed). Nothing from the failed batch is saved | You fix the cause, then `POST /migration/jobs/{id}/retry`: only failed batches re-run |
| **Retries exhausted** | Batch marked `failed`, as above | Via `/retry` after the cause is fixed |
| **Worker killed** (`kill -9`, OOM, server dies) mid-batch | The batch stays `running` but its heartbeat stops. The reaper notices after `BATCH_STALE_AFTER_SECONDS` (default 300 s), sets it back to `pending` and re-sends the message. The dead worker's half-done transaction was never committed, so there is no partial data | Yes |
| **Worker killed while holding unprocessed messages** | Those batches are still `pending` in Postgres. After `BATCH_REDELIVER_AFTER_SECONDS` (default 900 s) without being consumed, the reaper re-publishes them | Yes |
| **Redis down when the API starts a job** | The job row is already saved, so the API still answers 202. The reaper re-sends the plan message when Redis is back | Yes |
| **Redis loses messages** (restart without persistence) | Same: Postgres still knows which batches are `pending`, and the reaper re-publishes them | Yes |
| **API crashes right after saving the job row** | Job stays `pending`; the reaper re-sends the plan message after about 60 s | Yes |
| **Planning crashes half-way** | Re-running planning is safe (no duplicate batches) and the reaper re-triggers it | Yes |
| **The last batch finishes but the worker dies before marking the job complete** | The reaper's sweep finds "running job with nothing left to do" and finishes it | Yes |
| **Someone cancels the job** | Status becomes `cancelled`; no further batch can be claimed; batches already in flight finish; the language is free for a new job | n/a |
| **Whole system stopped and restarted** | On restart everything continues from the database state | Yes |

**What we actually saw in a real drill** (two workers, 400 batches, `docker kill` of one worker one second after start): the job froze at 47.5 %, the reaper repaired it, and the job finished at **400 / 400 batches, 100 %** in about 47 seconds (with the staleness windows temporarily shortened to 20/30 s). The database had exactly **39,983 rows per language with no duplicates**, and only **2 batches** needed a second attempt. This drill also exposed a logging bug that is now fixed and covered by a test.

### The three guarantees behind all of this
1. **Atomic batches.** Data and progress are saved in one transaction, so there are no half-batches and the progress number is always true.
2. **Idempotent writes.** `ON CONFLICT DO UPDATE ... WHERE ... IS DISTINCT FROM` means that repeating any batch changes nothing and creates no extra row versions.
3. **Source precedence.** Each translation row has a `source`: `fallback < import < machine < human`. A job never overwrites a stronger source, so a person's edit is never lost to a re-run.

---

## 7. Questions and edge cases (FAQ)

### Q1. Planning and execution are separate: the job is planned, batches are created and enqueued. What if one batch fails?

Think of **planning as writing a to-do list** and **batches as the independent items on it**. Once the list is written (rows in `migration_batches`), each item lives or dies on its own. A failing batch never cancels the plan, never stops the other batches, and never forces a re-plan.

What happens, in order:

1. **The failure is classified.**
   - *Temporary* (database hiccup, deadlock, timeout, provider throttling): the batch goes back to `pending`, and Celery re-delivers it after a growing delay (about 5 s, 10 s, 20 s, with jitter). Up to **5 attempts** (`MAX_BATCH_ATTEMPTS`).
   - *Permanent* (bad data, a bug, a provider returning the wrong number of values): the batch is marked `failed` **immediately**, with the error text. Retrying a deterministic error would be pointless.
2. **Nothing from the failed batch is saved.** Its data and its progress mark are one transaction, so it rolls back completely. No half-batches.
3. **All other batches carry on**, on any worker, unaffected.
4. **When no batch is `pending` or `running` any more**, the job is closed exactly once:

   | Batches | Job final status |
   |---|---|
   | all `succeeded` | `completed` |
   | some `failed`, some `succeeded` | `completed_with_errors` |
   | all `failed` | `failed` |

5. **What the frontend sees** (example: 3 batches of 10, the middle one failed):
   ```json
   { "status": "completed_with_errors", "processed_records": 20, "failed_records": 10,
     "total_records": 30, "progress_percentage": 66.67,
     "batches": { "total": 3, "succeeded": 2, "failed": 1, "pending": 0, "running": 0 } }
   ```
   Progress stops below 100 on purpose: **100 % means every record is really done**.
6. **Find out why:** `GET /migration/jobs/{job_id}/batches?status=failed` lists each failed batch with its `error`, `attempts` and id range.
7. **Fix the cause, then `POST /migration/jobs/{job_id}/retry`.** Only the *failed* batches are reset to `pending` (attempts back to 0) and re-published, and the job goes back to `running`. Succeeded batches are never touched and nothing is re-planned. When they finish, the job becomes `completed`.
   (Starting a brand-new job for the same language also works, and is safe, but it re-checks every batch instead of just the failed ones.)

### Q2. Where exactly does the translation happen?

**Inside the worker, inside each batch**, in the *processor* that `process_batch` calls. Not in the API, and not in planning (planning only makes the to-do list).

| Language | Processor | What it does inside the batch |
|---|---|---|
| `en` (the default language) | [`ImportBaseProcessor`](../app/workers/processors/import_base.py) | **No translation.** Copies the legacy table into `products` and into English translation rows with plain SQL. |
| `fr`, `bg`, `es`, ... | [`TranslateProcessor`](../app/workers/processors/translate.py) | 1. Reads the **English** rows for this batch's product-id range. 2. Hands them, in chunks of 500, to `provider.translate_batch(items, "en", "fr")`. 3. Saves the returned values as the target language, in the same transaction as the progress mark. |

> **Important, and easy to miss:** the provider that ships as the default (`copy_english`, in [`providers/copy_english.py`](../app/workers/providers/copy_english.py)) does **not** translate. It copies the English text and marks the rows `source = 'fallback'`, so right now `fr` and `bg` contain **English placeholders**. This was a deliberate choice (the assessment is about architecture and migration, not about a translation service), and it means the pipeline is complete and testable without an API key.

**To get real translations, add one class** (no other code changes, no DB change), then set `TRANSLATION_PROVIDER=my_mt` in `.env` and restart the workers:

```python
from app.models.enums import TranslationSource
from app.workers.errors import TransientError
from app.workers.providers import TranslationProvider, register_provider

class MyMachineTranslator(TranslationProvider):
    name = "my_mt"
    source = TranslationSource.MACHINE        # stronger than 'fallback', weaker than 'human'

    def translate_batch(self, items, source_lang, target_lang):
        # items: list of (product_id, key, value). Return ONE translated value per item, in the same order.
        try:
            return call_my_translation_api([i.value for i in items], source_lang, target_lang)
        except RateLimited as e:
            raise TransientError(str(e))      # the batch is retried with backoff

register_provider(MyMachineTranslator)
```
Because it runs inside the batch, you automatically get: parallel workers, retries with backoff for `TransientError`, heartbeats while the API call is slow, rollback if anything fails, and re-runs that skip unchanged rows. `machine` rows replace `fallback` placeholders, and **never** replace a human's edit.

### Q3. What if the planner itself fails?
`plan_job` is retried up to 5 times with backoff. If it still fails, the job becomes `failed` with the error text, and `POST /retry` plans it again from scratch. Planning is idempotent, so a half-finished plan is never duplicated.

### Q4. What if all workers are down?
Nothing is lost. The job just sits at its current progress; the messages wait in Redis, and the batch rows wait in Postgres. As soon as a worker starts, work resumes. The frontend simply sees no progress.

### Q5. What if Postgres goes down in the middle of a run?
- The API answers `503` on `/ready` and errors on other calls until it is back.
- Workers hit "temporary" errors and retry with backoff. Batches whose message was consumed while the database was unreachable are fixed by the reaper once it is back: stuck `running` batches after `BATCH_STALE_AFTER_SECONDS` (default 5 min), unprocessed `pending` ones after `BATCH_REDELIVER_AFTER_SECONDS` (default 15 min). So full recovery can take up to about 15 minutes by default. Lower these two values if you want faster healing (the kill-a-worker drill used 20 s and 30 s).

### Q6. What if Redis restarts or loses its messages?
Postgres still knows exactly which batches are `pending`. The reaper re-publishes them (same 15-minute default window). Duplicates are harmless because of the atomic claim.

### Q7. What if I send the same idempotency key for a *different* language?
You get **409** `idempotency_key_reused` with the `job_id` of the original job, and nothing is created. (A key identifies one request; reusing it for another request is a client bug, so the server says so instead of quietly returning the wrong job.) Reusing the same key with the same language but a different `batch_size` or `fields` returns the **original** job unchanged.

### Q8. What if the legacy data changes while a job is running or after it ran?
- Batches are id ranges fixed at planning time but read the data when each batch **runs**. Edits to rows inside those ranges are picked up; brand-new products with ids outside the planned ranges are not.
- To pick up new or changed data, just run the job again. Unchanged rows are skipped (no writes), changed rows are updated, new rows are added. Human edits are never overwritten.

### Q9. The English import had a failed batch. Can I still translate French?
Yes, the API allows it, because the import job is finished (not active) and some products exist. But **the products from the failed batch do not exist yet**, so they are not translated. Fix and retry the English job, then run `fr` again; only the missing rows get written.

### Q10. A batch is slow (for example a slow external translator). Will it be stolen by another worker?
Not while it is alive. A worker sends a heartbeat after every chunk, so the batch looks alive. If one single provider call takes longer than the stale window (default 5 minutes) with no heartbeat, the reaper may give the batch to another worker. The first worker then fails its "do I still own this?" check, and its transaction is rolled back, so nothing is corrupted, only time is wasted. Also note the hard time limit of 10 minutes per batch; if the translator is slow, use a smaller `batch_size`.

### Q11. How big can a batch be?
`batch_size` defaults to 2000 and must be between 1 and 20,000 (`MAX_BATCH_SIZE`); larger values get **422**. Smaller batches give smoother progress and cheaper retries; larger batches mean fewer round trips.

### Q12. What does the status look like in each phase, and when should the frontend stop polling?

| `status` | Meaning | Keep polling? |
|---|---|---|
| `pending` | Saved, waiting for a worker to plan it (0 %, `total_records` 0) | Yes |
| `planning` | Being cut into batches | Yes |
| `running` | Batches executing (progress and ETA move) | Yes |
| `completed` | All batches succeeded (100 %) | **Stop** |
| `completed_with_errors` | Finished, some batches failed (see Q1) | **Stop**, then show errors / offer retry |
| `failed` | Every batch failed, or planning failed | **Stop** |
| `cancelled` | Cancelled by `POST .../cancel` | **Stop** |

### Q13. What happens to the data when I cancel?
The status becomes `cancelled`; pending batches are never claimed; batches already running finish and keep their data. Everything already written is valid and can be completed later by running the language again. The language is immediately free for a new job.

### Q14. Does a failed or cancelled job block the language forever?
No. Only **active** jobs (`pending`, `planning`, `running`) block a new one. After any final status you can start again or use `/retry`.

### Q15. Can two different languages migrate at the same time?
Yes. They are independent jobs with separate batches. Only the same language is limited to one active job at a time.

### Q16. What if a worker's result arrives after the batch was given to someone else?
Its save fails the ownership check inside its own transaction, so that transaction is rolled back and logged as `batch_lost_lock`. The new owner's result is the one that counts. No duplicate, no overwrite.

### Q17. What if a product ends up with the same business key (`shop_id`, `product_id`) under a different id?
That is a data error: the batch fails permanently with an integrity error, and nothing from that batch is written. The error text tells you which key clashed.

### Q18. Is there authentication or rate limiting?
No. They are out of scope for this assessment. Put the API behind your gateway (auth, rate limits) before exposing it. `job_id` is a random UUID, but that is not a security control.

### Q19. How fast is it? (measured numbers)
Measured on this laptop (Docker Desktop, 1 worker container with 4 processes, the real 10,000-product dataset, batch size 2000 = 5 batches). Times come from the job and batch tables and the worker logs:

| Job | Records | Batches | Whole job (planning to completed) | Per batch (avg / max) |
|---|---:|---:|---:|---|
| `en` import (legacy to products + translations) | 10,000 | 5 | **0.74 s** | 0.25 s / 0.26 s |
| `fr` translate (default provider) | 10,000 | 5 | **1.05 s** | 0.51 s / 0.54 s |
| `bg` translate (default provider) | 10,000 | 5 | **0.98 s** | 0.47 s / 0.51 s |
| All three languages | 30,000 | 15 | about **3 s** in total | |

Other measured points:
- **Planning** 10,000 rows into batches took about **0.4 s**.
- **Many tiny batches:** `fr` with `batch_size=25` (400 batches) took **46.7 s**, but that run included killing a worker and waiting for the reaper (see the drill). Without the kill, `batch_size=500` (20 batches) took 1.7 s.
- One batch of **5,000** rows took about 0.9 to 1.1 s.
- Batch times are recorded with the real clock, so `finished_at - started_at` is the true duration. (This was fixed during testing: Postgres `now()` is the transaction *start*, which under-reported them.)

These are small-data numbers. Extrapolating, 10 million products at roughly 0.25 s per 2,000-row import batch is about 5,000 batches, which is about 21 minutes of single-worker time, so about 5 minutes with 4 workers. **That is an estimate from the small run, not a benchmark.** With a real, slow translator the speed is set by the provider; add workers with `docker compose up -d --scale worker=N`.

### Q20. How is memory managed?
Memory stays bounded by design, whatever the data size. What was done, and what was measured:

| Where | What keeps memory low |
|---|---|
| **English import** | Pure set-based SQL (`INSERT ... SELECT`). Rows never travel through Python, so memory does not grow with batch size. |
| **Translation** | A batch loads only its own id range (at most `batch_size` products, about 5 values each, so about 10,000 small items for 2000 products), and sends them to the provider in **chunks of 500** (`TRANSLATE_CHUNK_SIZE`). |
| **Planning** | One ordered pass over the primary-key index with `GROUP BY`; only `(batch_no, first_id, last_id, count)` comes back: about **5,000 tiny rows for 10 million products**. No `OFFSET`, no loading the product list. |
| **One batch at a time per process** | `worker_prefetch_multiplier=1`: a worker never hoards messages. Each process holds one batch in memory at a time. |
| **Messages are tiny** | A message is only a batch id (an integer). Data never goes through Redis. |
| **No result backend** | `task_ignore_result=True`: Redis does not accumulate task results; state lives in Postgres. |
| **Connection pools are capped** | Worker: 2 connections per process, no overflow. API: 10 plus 5 overflow. Hence no connection pile-up. |
| **API responses are bounded** | Lists have hard limits (jobs at most 200 per page, batches at most 1000), batch size is capped at 20,000, error text is cut at 2,000 characters, and progress is a small aggregate query, never a full scan. |
| **Request-scoped DB sessions** | Opened per request and closed after it (`async with`), so nothing accumulates between requests. |
| **Per-process engine after fork** | Each worker process builds its own small engine after the fork instead of sharing the parent's pool, so memory and connections are not duplicated or corrupted. |
| **No local state** | Nothing is cached or written to disk in the containers. |

**Measured (live containers, after migrating all three languages):**

| Container | Memory |
|---|---|
| `worker` (main process plus 4 worker processes, concurrency 4) | **about 249 MiB in total** (each worker process about 85 MB, but they share most pages, so do not add them up) |
| `api` | about **74 MiB** |
| `beat` | about **62 MiB** |
| Postgres (`si-db`) | about **148 MiB** |
| DB connections open from the app | `si-worker`: **8** (4 processes times 2), `si-api`: **1** |

The worker was about 183 MiB when idle and about 249 MiB after running jobs with 5,000-row batches, so the working set of a batch is small. This is a snapshot, **not a controlled peak-memory test**.

Not configured (honest gaps): `worker_max_tasks_per_child` is not set. If a real translation provider leaks memory over days, set it (for example 200) so processes are recycled. There is no per-container memory limit in `docker-compose.yml` either; add one in production.

---

## 8. How to test the migration

Everything runs in Docker. All commands are run from the project folder `C:\Users\LENOVO\Desktop\SI`.

### A. Run the automated tests (the quickest check)

```bash
docker compose run --rm --no-deps tools pytest -q
```
Expected: **`328 passed, 6 skipped`**. The 6 skipped are the live-stack tests (see D). The tests use their own database `translations_test`, so your real data is never touched. Useful subsets:

```bash
docker compose run --rm --no-deps tools pytest tests/unit -q                       # fast, no database
docker compose run --rm --no-deps tools pytest tests/integration/test_api.py -q    # API + duplicate requests
docker compose run --rm --no-deps tools pytest tests/integration/test_tasks.py -q  # retries, failures, races
docker compose run --rm --no-deps tools pytest tests/integration/test_reaper.py -q # self-healing
docker compose run --rm --no-deps tools pytest tests/integration/test_scale_end_to_end.py -q -s  # 10,000 rows
```

### B. Manual end-to-end test with the real 10,000 products

**1. Start everything and load the data** (skip what is already done):
```bash
cp .env.example .env
docker compose up -d si-db
python scripts/extract_sql_from_mhtml.py shop_products_1001_test.sql.mhtml data/shop_products_1001_test.sql
docker compose run --rm --no-deps tools python scripts/load_legacy.py        # expect: loaded 10000 rows
docker compose up -d api worker beat                                          # also applies the Alembic migrations
```
API docs and a click-to-try UI: **http://localhost:8010/docs** (port 8010, because 8000 is used by another project on this machine). Alternative read-only docs: http://localhost:8010/redoc. In `/docs`, open an endpoint, click **Try it out**, edit the JSON body, click **Execute**.
You can also import `postman/translations-migration.postman_collection.json` into Postman and run the requests in order.

**2. Check the services are healthy**
```bash
curl http://localhost:8010/ready
# {"status":"ready","checks":{"postgres":"ok","redis":"ok"}}
```

**3. Import English** (this creates `products` + English translations)
```bash
curl -X POST http://localhost:8010/migration/start -H "content-type: application/json" \
     -d '{"language_code":"en","batch_size":2000}'
# 202 {"job_id":"<JOB>", "status":"pending", ... "created":true}
```

**4. Watch progress** (repeat until `completed`)
```bash
curl http://localhost:8010/migration/status/<JOB>
# processed_records climbs 2000, 4000, ... 10000; progress_percentage 20 ... 100; batches.succeeded 5
```

**5. Check the data in Postgres**
```bash
docker exec si-translations-si-db-1 psql -U si -d translations -c "select (select count(*) from products) as products, (select count(*) from product_translations) as translations"
# products = 10000, translations = 39983
```

**6. Migrate French and Bulgarian** (same call, different `language_code`), then check each language has exactly **39,983** rows:
```bash
docker exec si-translations-si-db-1 psql -U si -d translations -c "select l.language_code, t.source, count(*) from product_translations t join languages l on l.id=t.language_id group by 1,2 order by 1"
# bg | fallback | 39983     en | import | 39983     fr | fallback | 39983
```

**7. Read a product in a language**
```bash
curl "http://localhost:8010/products/392?lang=fr"
```

### C. Test the "same request again" behaviour

| Test | Do this | You should see |
|---|---|---|
| Duplicate while running | Send `start` for `fr` twice quickly (use `"batch_size":25` so it runs longer) | 1st: **202**; 2nd: **409** `job_already_active` with the same `job_id` |
| Idempotency key | Send `start` with `"idempotency_key":"demo-1"` twice | 1st: **202** `created:true`; 2nd: **200** `created:false`, same `job_id`. Send it again after the job finished: still **200**, no new run |
| Re-run after completion | `start` `fr` again once finished | **202**, new job, finishes with the same row counts; check `select count(*) from product_translations` is unchanged |
| Order check | Truncate and try `fr` before `en` | **422** `base_import_required` |
| New language, no DB change | `curl -X POST http://localhost:8010/languages -H "content-type: application/json" -d '{"name":"Spanish","language_code":"es"}'`, then `start` `es` | Works; no migration or deploy needed |
| New field, no DB change | `curl -X PUT http://localhost:8010/products/392/translations/fr -H "content-type: application/json" -d '{"translations":{"features":"Hand blown glass"}}'` | `200`; the field is readable in `GET /products/392?lang=fr` |
| Human edit survives | Set a `title` via the PUT above, then re-run the `fr` job | Your edited title is still there |

### D. Test failure handling (the important ones)

**Kill a worker in the middle of a job** (recommended; the same drill that was run during development).

1. For a short demo, add these two lines at the end of `.env` (remove them afterwards; the production defaults are 300 and 900):
   ```
   BATCH_STALE_AFTER_SECONDS=20
   BATCH_REDELIVER_AFTER_SECONDS=30
   ```
2. Run two workers:
   ```bash
   docker compose up -d --force-recreate --scale worker=2 worker beat api
   ```
3. Start a job with many small batches:
   ```bash
   curl -X POST http://localhost:8010/migration/start -H "content-type: application/json" \
        -d '{"language_code":"fr","batch_size":25}'
   ```
4. About one second later, kill one worker:
   ```bash
   docker kill si-translations-worker-2
   ```
5. Poll the status. It may stall at some percentage for up to about a minute, then finish at **100 %**:
   ```bash
   curl http://localhost:8010/migration/status/<JOB>
   ```
6. Verify no damage: `fr` still has exactly **39,983** rows, and a few batches show `attempts = 2`:
   ```bash
   docker exec si-translations-si-db-1 psql -U si -d translations -c "select attempts, count(*) from migration_batches where job_id='<JOB>' group by 1"
   ```

**Other failure checks**
- **Redis down at start:** `docker stop redis-1` is **not** recommended because it belongs to another project. The automated test `test_start_still_succeeds_when_publishing_fails_and_the_reaper_recovers_the_job` covers this case.
- **Bad data / retry flow:** covered by `test_failed_batches_are_reported_then_retried_to_completion` (a poisoned batch fails, the rest finish, `completed_with_errors`, then `/retry` completes the job).
- **Cancel:** `curl -X POST http://localhost:8010/migration/jobs/<JOB>/cancel`. The status becomes `cancelled`, and the language can be started again.
- **See what failed:** `curl "http://localhost:8010/migration/jobs/<JOB>/batches?status=failed"` shows each failed batch with its error text and attempt count.

**Run the black-box test against the live stack** (real Redis and real Celery, the 6 "skipped" tests):
```bash
docker compose run --rm --no-deps -e E2E_BASE_URL=http://api:8000 tools pytest tests/e2e -q
```

### E. Where to look when something seems wrong
- **Logs** (structured JSON, every line carries `job_id`, `batch_id`, `worker`, `attempt`, `language`):
  ```bash
  docker compose logs worker --since 10m | grep batch_failed
  docker compose logs api --since 10m
  ```
- **Job and batch state:** `GET /migration/jobs`, `GET /migration/jobs/{id}/batches`, or query `migration_jobs` / `migration_batches` directly.
- **Key log events:** `job_created`, `job_planned`, `batch_started`, `batch_succeeded`, `batch_retry`, `batch_failed`, `batch_skipped`, `batch_lost_lock`, `reaper_repaired`, `job_finished`.

---

## 9. Where each step lives in the code

| Step | File | Function |
|---|---|---|
| HTTP endpoints | [`app/api/routes/migration.py`](../app/api/routes/migration.py) | `start_migration`, `migration_status`, ... |
| Validation, idempotency, duplicate handling, job creation | [`app/services/job_api.py`](../app/services/job_api.py) | `create_job` |
| Progress and ETA | same file | `get_job_status` |
| Cancel and retry | same file | `cancel_job`, `retry_failed` |
| Planning into batches | [`app/services/job_runtime.py`](../app/services/job_runtime.py) | `plan_ranges`, `plan_job` |
| Claim, heartbeat, success, failure, finalise, reaper | same file | `claim_batch`, `heartbeat`, `mark_succeeded`, `release_or_fail`, `finalize_job`, `reap` |
| Celery tasks (the glue and the retry logic) | [`app/workers/tasks.py`](../app/workers/tasks.py) | `plan_job`, `process_batch`, `reap` |
| English import SQL | [`app/workers/processors/import_base.py`](../app/workers/processors/import_base.py) | `ImportBaseProcessor` |
| Translation (provider plug-in) | [`app/workers/processors/translate.py`](../app/workers/processors/translate.py), [`app/workers/providers/`](../app/workers/providers/) | `TranslateProcessor` |
| Database tables and indexes | [`alembic/versions/`](../alembic/versions/) | `0001_core_schema.py`, `0002_job_tables.py` |
| Connection pools | [`app/core/db.py`](../app/core/db.py) | `make_async_engine`, `make_sync_engine` |

## 10. Quick glossary
- **Job**: one request to migrate one language. **Batch**: one slice of a job (for example 2000 products).
- **Claim**: a worker atomically saying "this batch is mine".
- **Heartbeat**: a worker periodically saying "I'm still alive".
- **Reaper**: the once-a-minute sweep that repairs stuck or lost work.
- **Idempotent**: doing it twice has the same effect as doing it once.
- **Transaction**: all-or-nothing database change.
