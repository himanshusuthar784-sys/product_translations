"""legacy table -> products + default-language translations (ImportBaseProcessor)."""
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from tests.helpers import legacy_row, make_ctx, rows, run_processor, scalar, seed_legacy, snapshot

TR_KEY = "product_id, language_id, key"


def run_import(db, start=1, end=10**9, **kw):
    return run_processor(db, make_ctx(db, "en", start, end, **kw))


def tr(db, product_id, key, lang="en"):
    r = rows(db, "SELECT value, CAST(source AS text) AS source FROM product_translations t JOIN languages l ON l.id=t.language_id "
                 "WHERE product_id=:p AND key=:k AND l.language_code=:l", p=product_id, k=key, l=lang)
    return r[0] if r else None


def test_maps_every_non_translatable_column_and_preserves_ids(db):
    seed_legacy(db, rows=[legacy_row(
        392, shop_id=19, product_id=212112277530, variants_count=4, handle="imax-vase", gender="unisex",
        age_group="kids", google_status=4, is_available_for_google=0, is_available_for_bing=0, is_deleted=1)])
    assert run_import(db) == 1
    p = rows(db, "SELECT * FROM products")[0]
    assert (p.id, p.shop_id, p.product_id, p.variants_count, p.handle, p.gender, p.age_group, p.google_status) == (
        392, 19, 212112277530, 4, "imax-vase", "unisex", "kids", 4)
    assert p.is_available_for_google is False and p.is_available_for_bing is False
    assert p.is_available_for_google_display_ads is True and p.is_available_for_xml is True
    assert p.is_deleted is True


def test_legacy_timestamps_are_interpreted_as_utc(db):
    seed_legacy(db, 1, created_at=datetime(2019, 1, 24, 19, 35, 43), updated_at=datetime(2019, 10, 25, 1, 12, 41))
    run_import(db)
    p = rows(db, "SELECT created_at, updated_at FROM products")[0]
    assert p.created_at == datetime(2019, 1, 24, 19, 35, 43, tzinfo=timezone.utc)
    assert p.updated_at == datetime(2019, 10, 25, 1, 12, 41, tzinfo=timezone.utc)


def test_null_legacy_timestamps_fall_back_to_now(db):
    seed_legacy(db, 1, created_at=None, updated_at=None)
    run_import(db)
    assert scalar(db, "SELECT created_at > now() - interval '1 minute' FROM products")


def test_translation_rows_for_each_field_with_jsonb_value_wrapper(db):
    seed_legacy(db, rows=[legacy_row(1, title="Vase", brand="IMAX", tags="a, b", meta_title="SEO Vase",
                                     google_product_category="Home > Vases")])
    run_import(db)
    got = {r.key: (r.value, r.source) for r in rows(db, "SELECT key, value, CAST(source AS text) AS source FROM product_translations")}
    assert got == {
        "title": ({"value": "Vase"}, "import"),
        "brand": ({"value": "IMAX"}, "import"),
        "tags": ({"value": "a, b"}, "import"),
        "meta_title": ({"value": "SEO Vase"}, "import"),
        "google_product_category": ({"value": "Home > Vases"}, "import"),
    }
    assert scalar(db, "SELECT language_id FROM product_translations LIMIT 1") == scalar(db, "SELECT id FROM languages WHERE is_default")


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_empty_and_blank_source_values_are_not_materialised(db, blank):
    seed_legacy(db, rows=[legacy_row(1, meta_title=blank, tags=blank, google_product_category=blank)])
    run_import(db)
    keys = {r.key for r in rows(db, "SELECT key FROM product_translations")}
    assert keys == {"title", "brand"}


def test_null_source_values_are_skipped(db):
    seed_legacy(db, rows=[legacy_row(1, meta_title=None, tags=None)])
    run_import(db)
    assert {r.key for r in rows(db, "SELECT key FROM product_translations")} == {"title", "brand", "google_product_category"}


def test_values_are_stored_verbatim_including_whitespace_unicode_quotes_and_corruption(db):
    title = "  Möbel 'Set' \"2\" — 日本 ��  "
    seed_legacy(db, rows=[legacy_row(1, title=title)])
    run_import(db)
    assert tr(db, 1, "title").value == {"value": title}     # not trimmed, not re-encoded


def test_field_subset_only_imports_selected_keys(db):
    seed_legacy(db, 3, meta_title="m")
    run_import(db, fields=["title", "tags"])
    assert {r.key for r in rows(db, "SELECT DISTINCT key FROM product_translations")} == {"title", "tags"}
    assert scalar(db, "SELECT count(*) FROM products") == 3     # products are always upserted


def test_only_the_requested_id_range_is_processed(db):
    seed_legacy(db, ids=[1, 5, 10, 20, 30])
    assert run_import(db, 5, 20) == 3
    assert [r.id for r in rows(db, "SELECT id FROM products ORDER BY id")] == [5, 10, 20]
    assert {r.product_id for r in rows(db, "SELECT DISTINCT product_id FROM product_translations")} == {5, 10, 20}


def test_sparse_ids_like_the_real_dump(db):
    ids = [392, 393, 394, 23100, 23101]
    seed_legacy(db, ids=ids)
    assert run_import(db) == 5
    assert [r.id for r in rows(db, "SELECT id FROM products ORDER BY id")] == ids


def test_empty_range_is_a_noop(db):
    seed_legacy(db, 3)
    assert run_import(db, 100, 200) == 0
    assert scalar(db, "SELECT count(*) FROM products") == 0


# ----------------------------------------------------------------------------------------- idempotency
def test_rerun_is_a_true_noop_no_tuple_is_rewritten(db):
    seed_legacy(db, 50, meta_title="m")
    run_import(db)
    p1, t1 = snapshot(db, "products", "id"), snapshot(db, TR_KEY and "product_translations", TR_KEY)
    n_before = scalar(db, "SELECT count(*) FROM product_translations")
    for _ in range(3):
        assert run_import(db) == 50
    assert snapshot(db, "products", "id") == p1                      # xmin identical => no new tuple versions
    assert snapshot(db, "product_translations", TR_KEY) == t1
    assert scalar(db, "SELECT count(*) FROM product_translations") == n_before   # and no duplicates


def test_rerun_after_source_change_updates_only_what_changed(db):
    seed_legacy(db, 3)
    run_import(db)
    before = {r[:3]: r.xmin for r in snapshot(db, "product_translations", TR_KEY)}
    with db.begin() as c:
        c.execute(text("UPDATE legacy.shop_products_1001_test SET title = 'NEW TITLE' WHERE id = 2"))
    run_import(db)
    after = {r[:3]: r.xmin for r in snapshot(db, "product_translations", TR_KEY)}
    changed = {k for k in after if after[k] != before[k]}
    en = scalar(db, "SELECT id FROM languages WHERE language_code='en'")
    assert changed == {(2, en, "title")}
    assert tr(db, 2, "title").value == {"value": "NEW TITLE"}


def test_new_source_value_appears_on_rerun(db):
    seed_legacy(db, 1, meta_title="")
    run_import(db)
    assert tr(db, 1, "meta_title") is None
    with db.begin() as c:
        c.execute(text("UPDATE legacy.shop_products_1001_test SET meta_title = 'Now set'"))
    run_import(db)
    assert tr(db, 1, "meta_title").value == {"value": "Now set"}


def test_updated_at_of_translation_only_moves_when_value_changes(db):
    seed_legacy(db, 1)
    run_import(db)
    t0 = scalar(db, "SELECT updated_at FROM product_translations WHERE key='title'")
    run_import(db)
    assert scalar(db, "SELECT updated_at FROM product_translations WHERE key='title'") == t0
    with db.begin() as c:
        c.execute(text("UPDATE legacy.shop_products_1001_test SET title = 'changed'"))
    run_import(db)
    assert scalar(db, "SELECT updated_at FROM product_translations WHERE key='title'") > t0


# ---------------------------------------------------------------------------------- source precedence
def _set_source(db, product_id, key, source, value="manual"):
    with db.begin() as c:
        c.execute(text("UPDATE product_translations SET source = CAST(:s AS translation_source), value = CAST(:v AS jsonb) "
                       "WHERE product_id = :p AND key = :k"),
                  {"s": source, "v": f'{{"value": "{value}"}}', "p": product_id, "k": key})


def test_human_edits_are_never_overwritten_by_an_import(db):
    seed_legacy(db, 1)
    run_import(db)
    _set_source(db, 1, "title", "human", "Edited by a person")
    with db.begin() as c:
        c.execute(text("UPDATE legacy.shop_products_1001_test SET title = 'legacy changed'"))
    run_import(db)
    assert tr(db, 1, "title").value == {"value": "Edited by a person"} and tr(db, 1, "title").source == "human"


def test_machine_translation_is_not_downgraded_by_an_import(db):
    seed_legacy(db, 1)
    run_import(db)
    _set_source(db, 1, "title", "machine", "machine text")
    with db.begin() as c:
        c.execute(text("UPDATE legacy.shop_products_1001_test SET title = 'legacy changed'"))
    run_import(db)
    assert tr(db, 1, "title").source == "machine" and tr(db, 1, "title").value == {"value": "machine text"}


def test_fallback_placeholder_is_upgraded_by_an_import(db):
    seed_legacy(db, 1)
    run_import(db)
    _set_source(db, 1, "title", "fallback", "placeholder")
    run_import(db)
    assert tr(db, 1, "title").source == "import" and tr(db, 1, "title").value == {"value": "Product 1"}


def test_other_languages_rows_are_untouched_by_the_default_language_import(db, lang_id):
    seed_legacy(db, 1)
    run_import(db)
    with db.begin() as c:
        c.execute(text("INSERT INTO product_translations (product_id, language_id, key, value, source) "
                       "VALUES (1, :l, 'title', '{\"value\": \"Titre\"}', 'human')"), {"l": lang_id("fr")})
    run_import(db)
    assert tr(db, 1, "title", "fr").value == {"value": "Titre"}


# ------------------------------------------------------------------------------------------- failures
def test_conflicting_business_key_with_a_different_id_fails_loudly(db):
    """(shop_id, product_id) is unique in `products`: a clash is a data error, not something to swallow."""
    seed_legacy(db, rows=[legacy_row(1, shop_id=3, product_id=777)])
    run_import(db)
    with db.begin() as c:
        c.execute(text("UPDATE legacy.shop_products_1001_test SET id = 2"))      # same business key, new id
    with pytest.raises(IntegrityError):
        run_import(db)
    assert scalar(db, "SELECT count(*) FROM products") == 1                       # rolled back cleanly


def test_a_failing_statement_rolls_back_the_whole_batch(db):
    seed_legacy(db, 2)                                # ids 1, 2 -> product_ids 1000001, 1000002
    with db.begin() as c:                             # a *different* product already owns row 2's business key
        c.execute(text("INSERT INTO products (id, shop_id, product_id, handle, gender, age_group) "
                       "VALUES (900, 3, 1000002, 'x', 'f', 'a')"))
    with pytest.raises(IntegrityError):
        run_import(db)
    # row 1 was fine on its own, but the batch is all-or-nothing
    assert scalar(db, "SELECT count(*) FROM products WHERE id IN (1, 2)") == 0
    assert scalar(db, "SELECT count(*) FROM product_translations") == 0


def test_heartbeat_is_invoked_during_processing(db):
    seed_legacy(db, 2)
    calls = []
    run_import(db, heartbeat=lambda: calls.append(1))
    assert calls   # lock stays alive between the two statements
