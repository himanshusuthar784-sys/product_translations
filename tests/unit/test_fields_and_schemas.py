import pytest
from pydantic import ValidationError

from app.schemas import LanguageIn, SetTranslationsRequest, StartMigrationRequest
from app.services.fields import TRANSLATABLE_FIELDS, validate_fields


class TestValidateFields:
    def test_none_means_all_in_registry_order(self):
        assert validate_fields(None) == list(TRANSLATABLE_FIELDS)

    def test_subset_is_normalised_to_registry_order_and_deduplicated(self):
        assert validate_fields(["brand", "title", "brand"]) == ["title", "brand"]

    def test_empty_list_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            validate_fields([])

    def test_unknown_field_rejected_and_named(self):
        with pytest.raises(ValueError, match="description.*zzz|unknown"):
            validate_fields(["title", "zzz", "description"])

    def test_sql_injection_attempt_is_just_an_unknown_field(self):
        with pytest.raises(ValueError, match="unknown"):
            validate_fields(["title; DROP TABLE products"])

    def test_registry_covers_the_five_legacy_translatable_columns(self):
        assert set(TRANSLATABLE_FIELDS) == {"title", "meta_title", "brand", "tags", "google_product_category"}
        # keys equal the legacy column names (1:1 mapping today)
        assert all(k == v for k, v in TRANSLATABLE_FIELDS.items())


class TestLanguageIn:
    @pytest.mark.parametrize("raw, expected", [("es", "es"), ("DE", "de"), ("pt-BR", "pt-BR"), ("zh-Hant", "zh-Hant")])
    def test_valid_codes_are_normalised(self, raw, expected):
        assert LanguageIn(name="X", language_code=raw).language_code == expected

    @pytest.mark.parametrize("bad", ["e", "en_US", "english!", "123", "en--US", "fr-"])
    def test_invalid_codes_rejected(self, bad):
        with pytest.raises(ValidationError):
            LanguageIn(name="X", language_code=bad)

    def test_name_required(self):
        with pytest.raises(ValidationError):
            LanguageIn(name="", language_code="es")


class TestSetTranslationsRequest:
    def test_accepts_new_arbitrary_keys(self):
        r = SetTranslationsRequest(translations={"features": "x", "seo_keywords": ["a", "b"], "title": 1})
        assert set(r.translations) == {"features", "seo_keywords", "title"}

    @pytest.mark.parametrize("bad_key", ["Title", "1abc", "with space", "a-b", "", "x" * 65, "ключ"])
    def test_rejects_bad_keys(self, bad_key):
        with pytest.raises(ValidationError):
            SetTranslationsRequest(translations={bad_key: "v"})

    def test_rejects_empty_payload(self):
        with pytest.raises(ValidationError):
            SetTranslationsRequest(translations={})


class TestStartMigrationRequest:
    def test_defaults(self):
        r = StartMigrationRequest(language_code="fr")
        assert r.fields is None and r.batch_size is None and r.idempotency_key is None

    @pytest.mark.parametrize("bs", [0, -5])
    def test_batch_size_must_be_positive(self, bs):
        with pytest.raises(ValidationError):
            StartMigrationRequest(language_code="fr", batch_size=bs)

    def test_language_code_length_bounds(self):
        with pytest.raises(ValidationError):
            StartMigrationRequest(language_code="x")
