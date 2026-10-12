from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy import Connection, text

from app.models.enums import TranslationSource

# Shared, idempotent, dead-tuple-free upsert.
#  * unchanged rows are not rewritten (IS DISTINCT FROM)  -> re-runs create no new tuples
#  * precedence by enum order: never replace a higher-precedence source (human > machine > import > fallback)
UPSERT_FROM_ARRAYS = text(
    """
    INSERT INTO product_translations (product_id, language_id, key, value, source)
    SELECT t.p, :language_id, t.k, CAST(t.v AS jsonb), CAST(:source AS translation_source)
    FROM unnest(CAST(:product_ids AS integer[]), CAST(:keys AS text[]), CAST(:vals AS text[])) AS t(p, k, v)
    ON CONFLICT (product_id, language_id, key) DO UPDATE
       SET value = EXCLUDED.value, source = EXCLUDED.source, updated_at = now()
     WHERE product_translations.source <= EXCLUDED.source
       AND product_translations.source <> 'human'
       AND (product_translations.value IS DISTINCT FROM EXCLUDED.value
            OR product_translations.source IS DISTINCT FROM EXCLUDED.source)
    """
)


@dataclass(frozen=True)
class BatchContext:
    batch_id: int
    job_id: str
    range_start: int
    range_end: int
    language_id: int
    language_code: str
    source_language_id: int | None
    source_language_code: str | None
    fields: list[str]
    provider: str | None
    heartbeat: Callable[[], None]  # raises LostLockError if the batch was taken over


class BatchProcessor(ABC):
    @abstractmethod
    def process(self, conn: Connection, ctx: BatchContext) -> int:
        """Process ctx's id range inside the caller's transaction. Returns processed record count."""


def upsert_translations(
    conn: Connection,
    *,
    language_id: int,
    source: TranslationSource,
    product_ids: list[int],
    keys: list[str],
    values_json: list[str],
) -> None:
    if not product_ids:
        return
    conn.execute(
        UPSERT_FROM_ARRAYS,
        {
            "language_id": language_id,
            "source": source.value,
            "product_ids": product_ids,
            "keys": keys,
            "vals": values_json,
        },
    )
