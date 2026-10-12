import pytest

from app.models.enums import ACTIVE_JOB_STATUSES, TERMINAL_JOB_STATUSES, JobStatus, TranslationSource
from app.workers import providers
from app.workers.providers import TranslationItem, TranslationProvider, get_provider, provider_names, register_provider
from app.workers.providers.copy_english import CopySourceProvider


def test_source_precedence_order_matches_postgres_enum():
    """The upsert relies on the PG enum's declaration order for precedence."""
    assert [s.value for s in TranslationSource] == ["fallback", "import", "machine", "human"]


def test_job_status_groups_are_disjoint_and_complete():
    assert set(ACTIVE_JOB_STATUSES).isdisjoint(TERMINAL_JOB_STATUSES)
    assert set(ACTIVE_JOB_STATUSES) | set(TERMINAL_JOB_STATUSES) == set(JobStatus)


class TestCopyProvider:
    def test_returns_same_values_in_order(self):
        items = [TranslationItem(1, "title", "Hello"), TranslationItem(2, "tags", ["a", "b"]), TranslationItem(3, "x", None)]
        assert CopySourceProvider().translate_batch(items, "en", "fr") == ["Hello", ["a", "b"], None]

    def test_empty_input(self):
        assert CopySourceProvider().translate_batch([], "en", "fr") == []

    def test_marks_rows_as_lowest_precedence_fallback(self):
        assert CopySourceProvider.source is TranslationSource.FALLBACK


class TestRegistry:
    def test_default_provider_registered(self):
        assert "copy_english" in provider_names()
        assert isinstance(get_provider("copy_english"), CopySourceProvider)

    def test_unknown_provider_raises_with_known_names(self):
        with pytest.raises(ValueError, match="unknown translation provider 'nope'.*copy_english"):
            get_provider("nope")

    def test_register_custom_provider(self):
        @register_provider
        class Upper(TranslationProvider):
            name = "upper_test"
            source = TranslationSource.MACHINE

            def translate_batch(self, items, source_lang, target_lang):
                return [str(i.value).upper() for i in items]

        try:
            assert get_provider("upper_test").translate_batch([TranslationItem(1, "t", "abc")], "en", "fr") == ["ABC"]
        finally:
            providers._REGISTRY.pop("upper_test", None)
