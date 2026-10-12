"""The schema as built by Alembic: structure, index hygiene, and every integrity rule the design relies on."""
import os
import re
import subprocess
import sys

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import Session

from app.models import Language, MigrationBatch, MigrationJob, Product, ProductTranslation
from app.models.enums import BatchStatus, JobStatus, JobType, TranslationSource
from tests.helpers import job_status, make_batch, make_job, rows, scalar


def _product(engine, pid=1, shop=3, **kw):
    with engine.begin() as c:
        c.execute(
            text("INSERT INTO products (id, shop_id, product_id, handle, gender, age_group) VALUES (:i, :s, :p, 'h', 'f', 'a')"),
            {"i": pid, "s": shop, "p": kw.get("product_id", 1000 + pid)},
        )


# ----------------------------------------------------------------------------------------- structure
def test_languages_seeded_with_en_fr_bg_and_en_default(db):
    got = rows(db, "SELECT language_code, is_default, is_active FROM languages ORDER BY id")
    assert [(r.language_code, r.is_default, r.is_active) for r in got] == [
        ("en", True, True), ("fr", False, True), ("bg", False, True)]


EXPECTED_INDEXES = {
    "languages": {"pk_languages", "uq_languages_language_code", "uq_languages_single_default"},
    "products": {"pk_products", "uq_products_shop_id_product_id", "ix_products_shop_created"},
    # ONE index only: PK == ON CONFLICT target == lookup path (prefix scans cover product / product+language)
    "product_translations": {"pk_product_translations"},
    "migration_jobs": {"pk_migration_jobs", "uq_migration_jobs_active", "uq_migration_jobs_idempotency_key",
                       "ix_migration_jobs_created_at"},
    "migration_batches": {"pk_migration_batches", "uq_migration_batches_job_id_batch_no",
                          "ix_migration_batches_job_status", "ix_migration_batches_running_heartbeat"},
}


@pytest.mark.parametrize("table", sorted(EXPECTED_INDEXES))
def test_exact_index_inventory_no_redundant_indexes(db, table):
    got = {r.indexname for r in rows(db, "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename=:t", t=table)}
    assert got == EXPECTED_INDEXES[table]


def test_index_definitions_follow_the_superset_design(db):
    defs = {r.indexname: r.indexdef for r in rows(db, "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public'")}
    assert "(shop_id, created_at DESC, id DESC)" in defs["ix_products_shop_created"]
    assert "(product_id, language_id, key)" in defs["pk_product_translations"]
    assert "(job_id, status) INCLUDE (processed_count)" in defs["ix_migration_batches_job_status"]
    assert "WHERE (status = 'running'" in defs["ix_migration_batches_running_heartbeat"]
    assert "WHERE (status = ANY" in defs["uq_migration_jobs_active"] or "status = ANY" in defs["uq_migration_jobs_active"]
    assert "WHERE is_default" in defs["uq_languages_single_default"]


def test_translations_pk_prefix_lookups_use_the_pk_index(db):
    """(product_id) and (product_id, language_id) must be served by the PK: no seq scan, no extra index."""
    _product(db, 1)
    with db.begin() as c:
        c.execute(text("SET LOCAL enable_seqscan = off"))
        for where in ("product_id = 1", "product_id = 1 AND language_id = 2", "product_id = 1 AND language_id = 2 AND key = 'title'"):
            plan = "\n".join(r[0] for r in c.execute(text(f"EXPLAIN SELECT * FROM product_translations WHERE {where}")))
            assert "pk_product_translations" in plan, plan


def test_products_unique_key_serves_shop_lookups(db):
    with db.begin() as c:    # the planner needs real rows + stats to make a meaningful choice
        c.execute(text(
            "INSERT INTO products (id, shop_id, product_id, handle, gender, age_group, created_at) "
            "SELECT g, 3 + g % 2, 1000 + g, 'h', 'f', 'a', now() - (g || ' minutes')::interval FROM generate_series(1, 2000) g"))
    with db.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        c.execute(text("ANALYZE products"))
    with db.begin() as c:
        c.execute(text("SET LOCAL enable_seqscan = off"))
        c.execute(text("SET LOCAL enable_bitmapscan = off"))
        plan = "\n".join(r[0] for r in c.execute(text("EXPLAIN SELECT * FROM products WHERE shop_id = 3 AND product_id = 5")))
        assert "uq_products_shop_id_product_id" in plan
        plan = "\n".join(r[0] for r in c.execute(text(
            "EXPLAIN SELECT * FROM products WHERE shop_id = 3 ORDER BY created_at DESC, id DESC LIMIT 20")))
        assert "ix_products_shop_created" in plan and "Sort" not in plan   # index delivers the order


def test_translation_value_is_jsonb_and_key_is_free_text(db):
    cols = {r.column_name: r.data_type for r in rows(
        db, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='product_translations'")}
    assert cols["value"] == "jsonb" and cols["key"] == "text"
    assert "id" not in cols       # composite PK, no surrogate id


# ------------------------------------------------------------------------------------ integrity rules
def test_products_unique_shop_product(db):
    _product(db, 1, shop=3, product_id=555)
    with pytest.raises(IntegrityError):
        _product(db, 2, shop=3, product_id=555)
    _product(db, 3, shop=4, product_id=555)       # same product_id, other shop: allowed


def test_translation_pk_rejects_duplicates_and_fk_rejects_orphans(db, lang_id):
    _product(db, 1)
    ins = text("INSERT INTO product_translations (product_id, language_id, key, value) VALUES (:p, :l, :k, '{\"value\": \"x\"}')")
    with db.begin() as c:
        c.execute(ins, {"p": 1, "l": lang_id("fr"), "k": "title"})
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(ins, {"p": 1, "l": lang_id("fr"), "k": "title"})
    with pytest.raises(IntegrityError):    # product does not exist
        with db.begin() as c:
            c.execute(ins, {"p": 999, "l": lang_id("fr"), "k": "title"})
    with pytest.raises(IntegrityError):    # language does not exist
        with db.begin() as c:
            c.execute(ins, {"p": 1, "l": 999, "k": "title"})


def test_translation_value_must_have_value_key(db, lang_id):
    _product(db, 1)
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(text("INSERT INTO product_translations (product_id, language_id, key, value) VALUES (1, :l, 't', '{\"other\": 1}')"),
                      {"l": lang_id("fr")})
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(text("INSERT INTO product_translations (product_id, language_id, key, value) VALUES (1, :l, 't', 'null')"),
                      {"l": lang_id("fr")})


def test_value_can_hold_any_json_type(db, lang_id):
    _product(db, 1)
    import json
    for i, v in enumerate(["text", 5, 2.5, True, None, ["a", "b"], {"nested": {"x": 1}}]):
        with db.begin() as c:
            c.execute(text("INSERT INTO product_translations (product_id, language_id, key, value) VALUES (1, :l, :k, CAST(:v AS jsonb))"),
                      {"l": lang_id("fr"), "k": f"k{i}", "v": json.dumps({"value": v})})
    assert scalar(db, "SELECT count(*) FROM product_translations") == 7


def test_deleting_product_cascades_but_language_delete_is_restricted(db, lang_id):
    _product(db, 1)
    with db.begin() as c:
        c.execute(text("INSERT INTO product_translations (product_id, language_id, key, value) VALUES (1, :l, 'title', '{\"value\":\"x\"}')"),
                  {"l": lang_id("fr")})
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(text("DELETE FROM languages WHERE language_code = 'fr'"))
    with db.begin() as c:
        c.execute(text("DELETE FROM products WHERE id = 1"))
    assert scalar(db, "SELECT count(*) FROM product_translations") == 0


def test_only_one_default_language_and_unique_codes(db):
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(text("INSERT INTO languages (name, language_code, is_default) VALUES ('Spanish', 'es', true)"))
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(text("INSERT INTO languages (name, language_code) VALUES ('English again', 'en')"))
    with db.begin() as c:   # many non-default languages are fine
        c.execute(text("INSERT INTO languages (name, language_code) VALUES ('Spanish', 'es'), ('German', 'de')"))


def test_translation_source_enum_order_encodes_precedence(db):
    labels = [r[0] for r in rows(db, "SELECT unnest(enum_range(NULL::translation_source))::text")]
    assert labels == ["fallback", "import", "machine", "human"]
    assert scalar(db, "SELECT 'fallback'::translation_source < 'import'::translation_source AND "
                      "'machine'::translation_source < 'human'::translation_source")


def test_one_active_job_per_language_and_type_enforced_by_db(db):
    first = make_job(db, "fr", status="pending")
    with pytest.raises(IntegrityError):
        make_job(db, "fr", status="running")
    make_job(db, "bg", status="running")                       # other language: fine
    with db.begin() as c:                                      # once terminal, a new one is allowed
        c.execute(text("UPDATE migration_jobs SET status='completed' WHERE id = CAST(:j AS uuid)"), {"j": first})
    make_job(db, "fr", status="pending")
    make_job(db, "fr", status="failed")                        # many terminal jobs per language are fine
    make_job(db, "fr", status="cancelled")


def test_idempotency_key_unique_when_present_only(db):
    with db.begin() as c:
        for key in ("k1", None, None):
            c.execute(text(
                "INSERT INTO migration_jobs (id, language_id, job_type, fields, batch_size, status, idempotency_key) "
                "VALUES (gen_random_uuid(), 2, 'translate', '{title}', 10, 'completed', :k)"), {"k": key})
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(text(
                "INSERT INTO migration_jobs (id, language_id, job_type, fields, batch_size, status, idempotency_key) "
                "VALUES (gen_random_uuid(), 3, 'translate', '{title}', 10, 'completed', 'k1')"))


def test_job_and_batch_check_constraints(db):
    with pytest.raises(IntegrityError):
        with db.begin() as c:
            c.execute(text("INSERT INTO migration_jobs (id, language_id, job_type, fields, batch_size) "
                           "VALUES (gen_random_uuid(), 2, 'translate', '{title}', 0)"))
    jid = make_job(db, "fr")
    with pytest.raises(IntegrityError):
        make_batch(db, jid, 0, start=10, end=5, count=1)       # range_start > range_end
    make_batch(db, jid, 0, start=5, end=10, count=3)
    with pytest.raises(IntegrityError):
        make_batch(db, jid, 0, start=11, end=12, count=1)      # (job_id, batch_no) unique -> idempotent planning


def test_deleting_a_job_cascades_to_its_batches(db):
    jid = make_job(db, "fr")
    make_batch(db, jid, 0, 1, 10, 10)
    make_batch(db, jid, 1, 11, 20, 10)
    with db.begin() as c:
        c.execute(text("DELETE FROM migration_jobs WHERE id = CAST(:j AS uuid)"), {"j": jid})
    assert scalar(db, "SELECT count(*) FROM migration_batches") == 0


def test_invalid_enum_values_are_rejected(db):
    jid = make_job(db, "fr")
    with pytest.raises((DataError, IntegrityError)):
        with db.begin() as c:
            c.execute(text("UPDATE migration_jobs SET status = 'exploded' WHERE id = CAST(:j AS uuid)"), {"j": jid})


# --------------------------------------------------------------------------- ORM <-> schema agreement
def test_orm_models_roundtrip_against_the_migrated_schema(db):
    with Session(db) as s:
        s.add(Product(id=1, shop_id=3, product_id=10, handle="h", gender="f", age_group="a"))
        s.flush()
        fr = s.query(Language).filter_by(language_code="fr").one()
        s.add(ProductTranslation(product_id=1, language_id=fr.id, key="title", value={"value": "Bonjour"},
                                 source=TranslationSource.HUMAN))
        job = MigrationJob(language_id=fr.id, job_type=JobType.TRANSLATE, fields=["title"], batch_size=10,
                           status=JobStatus.RUNNING, provider="copy_english")
        s.add(job)
        s.flush()
        s.add(MigrationBatch(job_id=job.id, batch_no=0, range_start=1, range_end=1, expected_count=1,
                             status=BatchStatus.PENDING))
        s.commit()
    with Session(db) as s:
        t = s.query(ProductTranslation).one()
        assert t.value == {"value": "Bonjour"} and t.source is TranslationSource.HUMAN
        j = s.query(MigrationJob).one()
        assert j.status is JobStatus.RUNNING and j.fields == ["title"] and j.job_type is JobType.TRANSLATE
        assert s.query(MigrationBatch).one().status is BatchStatus.PENDING
        p = s.get(Product, 1)
        assert p.is_deleted is False and p.is_available_for_google is True and p.created_at.tzinfo is not None


# ------------------------------------------------------------------------------- migrations themselves
def _alembic(env_db: str, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ}
    for key in ("DATABASE_URL_SYNC", "DATABASE_URL_ASYNC"):
        env[key] = re.sub(r"/[^/?]+$", f"/{env_db}", os.environ[key])
    return subprocess.run([sys.executable, "-m", "alembic", *args], env=env, capture_output=True, text=True)


@pytest.fixture
def scratch_db():
    admin = re.sub(r"/[^/?]+$", "/postgres", os.environ["DATABASE_URL_SYNC"]).replace("postgresql+psycopg://", "postgresql://")
    name = "translations_test_migrations"
    with psycopg.connect(admin, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
        c.execute(f"CREATE DATABASE {name}")
    yield name
    with psycopg.connect(admin, autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def test_alembic_upgrade_downgrade_upgrade_roundtrip(scratch_db):
    up = _alembic(scratch_db, "upgrade", "head")
    assert up.returncode == 0, up.stderr
    dsn = re.sub(r"/[^/?]+$", f"/{scratch_db}", os.environ["DATABASE_URL_SYNC"]).replace("postgresql+psycopg://", "postgresql://")

    def tables():
        with psycopg.connect(dsn) as c:
            return {r[0] for r in c.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")}

    assert {"languages", "products", "product_translations", "migration_jobs", "migration_batches"} <= tables()
    assert _alembic(scratch_db, "upgrade", "head").returncode == 0           # re-running head is a no-op
    down = _alembic(scratch_db, "downgrade", "base")
    assert down.returncode == 0, down.stderr
    assert tables() == {"alembic_version"}
    with psycopg.connect(dsn) as c:                                           # enums cleaned up as well
        assert c.execute("SELECT count(*) FROM pg_type WHERE typname IN ('translation_source','migration_job_type',"
                         "'migration_job_status','migration_batch_status')").fetchone()[0] == 0
    again = _alembic(scratch_db, "upgrade", "head")
    assert again.returncode == 0, again.stderr
    assert {"languages", "products"} <= tables()


def test_alembic_has_a_single_head_and_linear_history(scratch_db):
    heads = _alembic(scratch_db, "heads")
    assert heads.returncode == 0 and heads.stdout.count("(head)") == 1, heads.stdout
    assert "0002" in heads.stdout
