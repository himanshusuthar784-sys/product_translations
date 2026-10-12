"""Shared test helpers: legacy-row factory and tiny SQL conveniences."""
import uuid
from datetime import datetime

from sqlalchemy import text

LEGACY_COLS = (
    "id, shop_id, product_id, title, variants_count, meta_title, handle, brand, gender, age_group, tags, "
    "google_product_category, google_status, is_available_for_google, is_available_for_google_display_ads, "
    "is_available_for_google_shopping_actions, is_available_for_bing, is_available_for_facebook, "
    "is_available_for_xml, is_deleted, created_at, updated_at"
)


def legacy_row(i: int, **over) -> dict:
    row = dict(
        id=i,
        shop_id=3,
        product_id=1_000_000 + i,
        title=f"Product {i}",
        variants_count=1,
        meta_title="",
        handle=f"product-{i}",
        brand="ACME",
        gender="female",
        age_group="adult",
        tags="red, blue",
        google_product_category="Home & Garden > Decor",
        google_status=0,
        is_available_for_google=1,
        is_available_for_google_display_ads=1,
        is_available_for_google_shopping_actions=1,
        is_available_for_bing=1,
        is_available_for_facebook=1,
        is_available_for_xml=1,
        is_deleted=0,
        created_at=datetime(2019, 1, 24, 19, 35, 43),
        updated_at=datetime(2019, 10, 25, 1, 12, 41),
    )
    row.update(over)
    return row


def seed_legacy(engine, n: int = 0, ids: list[int] | None = None, rows: list[dict] | None = None, **over) -> list[int]:
    """Insert legacy rows. `ids` allows sparse ids (like the real dump: 392..23101)."""
    rows = rows or [legacy_row(i, **over) for i in (ids if ids is not None else range(1, n + 1))]
    placeholders = ", ".join(f":{c.strip()}" for c in LEGACY_COLS.split(","))
    with engine.begin() as c:
        for r in rows:
            c.execute(text(f"INSERT INTO legacy.shop_products_1001_test ({LEGACY_COLS}) VALUES ({placeholders})"), r)
    return [r["id"] for r in rows]


def seed_legacy_bulk(engine, n: int) -> dict:
    """Generate n legacy rows server-side (fast) with realistic gaps. Returns the expected per-key counts
    for the importer: empty/blank values are NOT translations."""
    with engine.begin() as c:
        c.execute(
            text(
                f"""
                INSERT INTO legacy.shop_products_1001_test ({LEGACY_COLS})
                SELECT g, CASE WHEN g % 3000 = 0 THEN 19 ELSE 3 END, 1000000 + g, 'Product ' || g, 1,
                       CASE WHEN g % 1000 = 0 THEN 'Meta ' || g ELSE '' END, 'product-' || g,
                       'Brand' || (g % 40), 'female', 'adult',
                       CASE WHEN g % 500 = 0 THEN '' ELSE 'tag' || g || ', other' END,
                       CASE WHEN g % 1250 = 0 THEN '' ELSE 'Home & Garden > Decor' END,
                       0, 1, 1, 1, 1, 1, 1, CASE WHEN g % 200 = 0 THEN 1 ELSE 0 END,
                       timestamp '2019-01-24 19:35:43' + (g || ' seconds')::interval, timestamp '2019-10-25 01:12:41'
                FROM generate_series(1, :n) g
                """
            ),
            {"n": n},
        )
    return {
        "title": n,
        "brand": n,
        "meta_title": n // 1000,
        "tags": n - n // 500,
        "google_product_category": n - n // 1250,
    }


def schema_fingerprint(engine) -> str:
    """Hash of every column, index and constraint in `public`: proves 'no DB change' claims."""
    import hashlib

    with engine.connect() as c:
        parts = [
            str(c.execute(text(
                "SELECT table_name, column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                "WHERE table_schema = 'public' ORDER BY 1, 2")).all()),
            str(c.execute(text("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY 1, 2")).all()),
            str(c.execute(text(
                "SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE connamespace = 'public'::regnamespace ORDER BY 1, 2")).all()),
        ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def make_ctx(engine, language_code: str, start: int, end: int, *, fields=None, provider=None, heartbeat=None, job_id="job"):
    from app.services.fields import TRANSLATABLE_FIELDS
    from app.workers.processors import BatchContext

    with engine.connect() as c:
        lid = c.execute(text("SELECT id FROM languages WHERE language_code=:c"), {"c": language_code}).scalar_one()
        d = c.execute(text("SELECT id, language_code FROM languages WHERE is_default")).one()
    return BatchContext(
        batch_id=0, job_id=job_id, range_start=start, range_end=end, language_id=lid, language_code=language_code,
        source_language_id=None if lid == d.id else d.id, source_language_code=None if lid == d.id else d.language_code,
        fields=fields or list(TRANSLATABLE_FIELDS), provider=provider, heartbeat=heartbeat or (lambda: None),
    )


def run_processor(engine, ctx, job_type: str | None = None) -> int:
    from app.models.enums import JobType
    from app.workers.processors import get_processor

    jt = JobType(job_type) if job_type else (JobType.IMPORT_BASE if ctx.source_language_id is None else JobType.TRANSLATE)
    with engine.begin() as conn:
        return get_processor(jt).process(conn, ctx)


def snapshot(engine, table: str, key_cols: str) -> list:
    """Row identity + xmin: if a re-run rewrites any tuple, its xmin changes."""
    return rows(engine, f"SELECT {key_cols}, xmin::text FROM {table} ORDER BY {key_cols}")


def scalar(engine, sql: str, **params):
    with engine.connect() as c:
        return c.execute(text(sql), params).scalar_one()


def rows(engine, sql: str, **params):
    with engine.connect() as c:
        return c.execute(text(sql), params).all()


def make_job(
    engine,
    language_code: str = "en",
    *,
    status: str = "running",
    batch_size: int = 100,
    fields: list[str] | None = None,
    job_type: str | None = None,
    provider: str | None = None,
) -> str:
    """Insert a job row directly (bypassing the API) for engine-level tests."""
    from app.services.fields import TRANSLATABLE_FIELDS

    jid = str(uuid.uuid4())
    with engine.begin() as c:
        lid = c.execute(text("SELECT id FROM languages WHERE language_code=:c"), {"c": language_code}).scalar_one()
        default_id = c.execute(text("SELECT id FROM languages WHERE is_default")).scalar_one()
        jt = job_type or ("import_base" if lid == default_id else "translate")
        c.execute(
            text(
                "INSERT INTO migration_jobs (id, language_id, source_language_id, job_type, fields, batch_size, status, provider) "
                "VALUES (CAST(:id AS uuid), :l, :s, CAST(:jt AS migration_job_type), CAST(:f AS text[]), :bs, "
                "CAST(:st AS migration_job_status), :p)"
            ),
            {
                "id": jid,
                "l": lid,
                "s": None if jt == "import_base" else default_id,
                "jt": jt,
                "f": fields or list(TRANSLATABLE_FIELDS),
                "bs": batch_size,
                "st": status,
                "p": provider if provider is not None else (None if jt == "import_base" else "copy_english"),
            },
        )
    return jid


def make_batch(engine, job_id: str, batch_no: int, start: int, end: int, count: int, status: str = "pending", **cols) -> int:
    extra = {"attempts": 0, "locked_by": None, **cols}
    with engine.begin() as c:
        return c.execute(
            text(
                "INSERT INTO migration_batches (job_id, batch_no, range_start, range_end, expected_count, status, "
                "attempts, locked_by, heartbeat_at, enqueued_at) VALUES (CAST(:j AS uuid), :n, :s, :e, :c, "
                "CAST(:st AS migration_batch_status), :attempts, :locked_by, "
                "now() - make_interval(secs => CAST(:hb_age AS double precision)), "
                "now() - make_interval(secs => CAST(:enq_age AS double precision))) RETURNING id"
            ),
            {
                "j": job_id, "n": batch_no, "s": start, "e": end, "c": count, "st": status,
                "attempts": extra["attempts"], "locked_by": extra["locked_by"],
                "hb_age": extra.get("hb_age"), "enq_age": extra.get("enq_age"),
            },
        ).scalar_one()


def job_status(engine, job_id: str) -> str:
    return scalar(engine, "SELECT CAST(status AS text) FROM migration_jobs WHERE id = CAST(:j AS uuid)", j=job_id)


def batch_status(engine, batch_id: int) -> str:
    return scalar(engine, "SELECT CAST(status AS text) FROM migration_batches WHERE id = :b", b=batch_id)
