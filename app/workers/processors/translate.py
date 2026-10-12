"""default-language translations -> target-language translations through a TranslationProvider."""
import json

from sqlalchemy import Connection, text

from app.core.config import get_settings
from app.workers.processors.base import BatchContext, BatchProcessor, upsert_translations
from app.workers.providers import TranslationItem, get_provider

_FETCH_SOURCE = text(
    """
    SELECT product_id, key, value -> 'value' AS raw
    FROM product_translations
    WHERE product_id BETWEEN :s AND :e
      AND language_id = :source_language_id
      AND key = ANY(CAST(:fields AS text[]))
    ORDER BY product_id, key
    """
)
_COUNT_PRODUCTS = text("SELECT count(*) FROM products WHERE id BETWEEN :s AND :e")


class TranslateProcessor(BatchProcessor):
    def process(self, conn: Connection, ctx: BatchContext) -> int:
        settings = get_settings()
        provider = get_provider(ctx.provider or settings.translation_provider)
        rows = conn.execute(
            _FETCH_SOURCE,
            {
                "s": ctx.range_start,
                "e": ctx.range_end,
                "source_language_id": ctx.source_language_id,
                "fields": ctx.fields,
            },
        ).all()

        items = [TranslationItem(r.product_id, r.key, r.raw) for r in rows]
        chunk = settings.translate_chunk_size
        for i in range(0, len(items), chunk):
            part = items[i : i + chunk]
            values = provider.translate_batch(part, ctx.source_language_code, ctx.language_code)
            if len(values) != len(part):
                raise ValueError(
                    f"provider '{provider.name}' returned {len(values)} values for {len(part)} items"
                )
            upsert_translations(
                conn,
                language_id=ctx.language_id,
                source=provider.source,
                product_ids=[p.product_id for p in part],
                keys=[p.key for p in part],
                values_json=[json.dumps({"value": v}, ensure_ascii=False) for v in values],
            )
            ctx.heartbeat()  # keeps the lock alive during slow providers
        return conn.execute(_COUNT_PRODUCTS, {"s": ctx.range_start, "e": ctx.range_end}).scalar_one()
