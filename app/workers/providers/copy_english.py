from collections.abc import Sequence
from typing import Any

from app.models.enums import TranslationSource
from app.workers.providers.base import TranslationItem, TranslationProvider


class CopySourceProvider(TranslationProvider):
    """Default provider: copies the source-language value as a placeholder.

    Rows are flagged `source='fallback'`, the lowest precedence, so any later machine or human
    translation replaces them and a re-run of this provider never overwrites a better one.
    """

    name = "copy_english"
    source = TranslationSource.FALLBACK

    def translate_batch(
        self, items: Sequence[TranslationItem], source_lang: str, target_lang: str
    ) -> list[Any]:
        return [i.value for i in items]
