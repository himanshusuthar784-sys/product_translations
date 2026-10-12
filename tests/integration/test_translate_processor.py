"""default-language translations -> target language through a pluggable TranslationProvider."""
import json

import pytest
from sqlalchemy import text

from app.core.config import get_settings
from app.models.enums import TranslationSource
from app.workers import providers
from app.workers.errors import TransientError
from app.workers.providers import TranslationProvider, register_provider
from tests.helpers import legacy_row, make_ctx, rows, run_processor, scalar, seed_legacy, snapshot

KEY = "product_id, language_id, key"


@pytest.fixture
def base(db):
    """10 English products imported (ids 1..10); meta_title only on product 1."""
    seed_legacy(db, rows=[legacy_row(i, meta_title="Meta" if i == 1 else "") for i in range(1, 11)])
    run_processor(db, make_ctx(db, "en", 1, 100))
    return db


def translate(db, lang="fr", start=1, end=100, **kw):
    return run_processor(db, make_ctx(db, lang, start, end, **kw))


def tr(db, product_id, key, lang):
    r = rows(db, "SELECT value, CAST(source AS text) AS source FROM product_translations t JOIN languages l ON l.id=t.language_id "
                 "WHERE product_id=:p AND key=:k AND l.language_code=:l", p=product_id, k=key, l=lang)
    return r[0] if r else None


@pytest.fixture
def custom_provider():
    created: list[type] = []

    def _make(name, source, fn=None, calls=None):
        class P(TranslationProvider):
            def translate_batch(self, items, source_lang, target_lang):
                if calls is not None:
                    calls.append((len(items), source_lang, target_lang))
                return fn(items) if fn else [f"[{target_lang}] {i.value}" for i in items]

        P.name, P.source = name, source
        register_provider(P)
        created.append(name)
        return name

    yield _make
    for n in created:
        providers._REGISTRY.pop(n, None)


# ------------------------------------------------------------------------------------ default provider
def test_default_provider_copies_english_as_fallback(base):
    n = translate(base)
    assert n == 10
    en = {(r.product_id, r.key): r.value for r in rows(base, "SELECT product_id, key, value FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='en'")}
    fr = {(r.product_id, r.key): (r.value, r.source) for r in rows(
        base, "SELECT product_id, key, value, CAST(source AS text) AS source FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='fr'")}
    assert set(fr) == set(en)                                # same keys, nothing invented, nothing dropped
    assert all(fr[k][0] == en[k] and fr[k][1] == "fallback" for k in en)


def test_empty_english_values_are_not_translated(base):
    translate(base)
    assert tr(base, 1, "meta_title", "fr") is not None       # present in English
    assert tr(base, 2, "meta_title", "fr") is None           # absent in English


def test_rerun_is_a_true_noop(base):
    translate(base)
    snap = snapshot(base, "product_translations", KEY)
    for _ in range(3):
        assert translate(base) == 10
    assert snapshot(base, "product_translations", KEY) == snap


def test_two_languages_are_independent(base):
    translate(base, "fr")
    translate(base, "bg")
    counts = dict(rows(base, "SELECT l.language_code, count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id GROUP BY 1"))
    assert counts["fr"] == counts["bg"] == counts["en"]


def test_field_subset(base):
    translate(base, fields=["title"])
    assert {r.key for r in rows(base, "SELECT DISTINCT key FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='fr'")} == {"title"}


def test_range_is_respected_and_returns_product_count(base):
    assert translate(base, start=3, end=5) == 3
    assert {r.product_id for r in rows(base, "SELECT DISTINCT product_id FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='fr'")} == {3, 4, 5}


def test_source_is_the_default_language_not_any_other_language(base, lang_id):
    with base.begin() as c:   # a French row with different text must not leak into Bulgarian
        c.execute(text("INSERT INTO product_translations (product_id, language_id, key, value, source) VALUES (1, :l, 'title', '{\"value\": \"FRENCH\"}', 'human')"), {"l": lang_id("fr")})
    translate(base, "bg")
    assert tr(base, 1, "title", "bg").value == {"value": "Product 1"}


def test_no_source_rows_means_nothing_written(db):
    seed_legacy(db, 3)
    run_processor(db, make_ctx(db, "en", 1, 100))
    with db.begin() as c:
        c.execute(text("DELETE FROM product_translations"))
    assert translate(db) == 3                                  # products exist, but no English values to translate
    assert scalar(db, "SELECT count(*) FROM product_translations") == 0


# ------------------------------------------------------------------------------------------ providers
def test_machine_provider_values_are_stored_with_machine_source(base, custom_provider):
    name = custom_provider("mt_test", TranslationSource.MACHINE)
    translate(base, provider=name)
    assert tr(base, 1, "title", "fr").value == {"value": "[fr] Product 1"}
    assert tr(base, 1, "title", "fr").source == "machine"


def test_machine_upgrades_fallback_but_fallback_never_downgrades_machine(base, custom_provider):
    translate(base)                                            # fallback rows
    mt = custom_provider("mt_test", TranslationSource.MACHINE)
    translate(base, provider=mt)
    assert tr(base, 1, "title", "fr").source == "machine"      # upgraded
    translate(base)                                            # default (fallback) provider runs again
    assert tr(base, 1, "title", "fr").source == "machine"      # NOT downgraded
    assert tr(base, 1, "title", "fr").value == {"value": "[fr] Product 1"}


def test_human_translation_survives_every_provider(base, custom_provider):
    translate(base)
    with base.begin() as c:
        c.execute(text("UPDATE product_translations SET value='{\"value\":\"Mon titre\"}', source='human' "
                       "WHERE product_id=1 AND key='title' AND language_id=(SELECT id FROM languages WHERE language_code='fr')"))
    translate(base, provider=custom_provider("mt_test", TranslationSource.MACHINE))
    translate(base)
    assert tr(base, 1, "title", "fr").value == {"value": "Mon titre"} and tr(base, 1, "title", "fr").source == "human"


def test_provider_receives_languages_and_chunks_by_setting(base, custom_provider, monkeypatch):
    monkeypatch.setattr(get_settings(), "translate_chunk_size", 7)
    calls: list = []
    name = custom_provider("mt_test", TranslationSource.MACHINE, calls=calls)
    translate(base, provider=name)
    total_items = scalar(base, "SELECT count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='en'")
    assert sum(c[0] for c in calls) == total_items
    assert all(c[0] <= 7 for c in calls) and len(calls) == -(-total_items // 7)
    assert all(c[1:] == ("en", "fr") for c in calls)


def test_heartbeat_called_once_per_chunk(base, custom_provider, monkeypatch):
    monkeypatch.setattr(get_settings(), "translate_chunk_size", 10)
    beats: list = []
    name = custom_provider("mt_test", TranslationSource.MACHINE)
    translate(base, provider=name, heartbeat=lambda: beats.append(1))
    total = scalar(base, "SELECT count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='en'")
    assert len(beats) == -(-total // 10)


def test_non_string_values_survive_the_roundtrip(base, custom_provider):
    name = custom_provider("mt_test", TranslationSource.MACHINE, fn=lambda items: [{"n": i.product_id, "list": ["é", 1, None]} for i in items])
    translate(base, fields=["title"], provider=name)
    assert tr(base, 2, "title", "fr").value == {"value": {"n": 2, "list": ["é", 1, None]}}


def test_provider_returning_wrong_number_of_values_fails_the_batch_atomically(base, custom_provider):
    name = custom_provider("bad_test", TranslationSource.MACHINE, fn=lambda items: ["x"])
    with pytest.raises(ValueError, match="returned 1 values for"):
        translate(base, provider=name)
    assert scalar(base, "SELECT count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='fr'") == 0


def test_transient_provider_error_propagates_and_leaves_no_partial_writes(base, custom_provider, monkeypatch):
    monkeypatch.setattr(get_settings(), "translate_chunk_size", 5)
    state = {"n": 0}

    def flaky(items):
        state["n"] += 1
        if state["n"] == 3:
            raise TransientError("429 rate limited")
        return [str(i.value) for i in items]

    name = custom_provider("flaky_test", TranslationSource.MACHINE, fn=flaky)
    with pytest.raises(TransientError):
        translate(base, provider=name)
    # chunks 1-2 were upserted inside the same transaction: they must be rolled back too
    assert scalar(base, "SELECT count(*) FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='fr'") == 0


def test_unknown_provider_name_fails(base):
    with pytest.raises(ValueError, match="unknown translation provider"):
        translate(base, provider="does_not_exist")


def test_provider_stored_values_are_valid_json_with_unicode_unescaped(base, custom_provider):
    name = custom_provider("mt_test", TranslationSource.MACHINE, fn=lambda items: ["Crème brûlée 日本 \"q\" \\ back" for _ in items])
    translate(base, "bg", fields=["title"], provider=name)
    v = scalar(base, "SELECT value FROM product_translations t JOIN languages l ON l.id=t.language_id WHERE l.language_code='bg' AND key='title' AND product_id=1")
    assert v == {"value": "Crème brûlée 日本 \"q\" \\ back"}
    assert json.loads(json.dumps(v)) == v
