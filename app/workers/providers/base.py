from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from app.models.enums import TranslationSource


@dataclass(frozen=True)
class TranslationItem:
    product_id: int
    key: str
    value: Any  # unwrapped from {"value": ...}


class TranslationProvider(ABC):
    """Produces target-language values for source-language items.

    Contract:
      * `translate_batch` returns exactly one value per input item, in order.
      * Raise `TransientError` for retry-able problems (rate limit, timeout);
        any other exception is treated as permanent for the batch.
      * It must be safe to call repeatedly with the same input (jobs are re-run).
    """

    name: str
    source: TranslationSource

    @abstractmethod
    def translate_batch(
        self, items: Sequence[TranslationItem], source_lang: str, target_lang: str
    ) -> list[Any]: ...
