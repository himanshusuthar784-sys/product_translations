"""legacy table -> products + default-language translations, as set-based SQL.

No Python row loop: constant memory regardless of batch size, one round trip per statement.
"""
from sqlalchemy import Connection, text

from app.services.fields import LEGACY_TABLE, TRANSLATABLE_FIELDS
from app.workers.processors.base import BatchContext, BatchProcessor

_PRODUCTS_UPSERT = text(
    f"""
    INSERT INTO products (
        id, shop_id, product_id, variants_count, handle, gender, age_group, google_status,
        is_available_for_google, is_available_for_google_display_ads,
        is_available_for_google_shopping_actions, is_available_for_bing,
        is_available_for_facebook, is_available_for_xml, is_deleted, created_at, updated_at)
    SELECT
        l.id, l.shop_id, l.product_id, l.variants_count, l.handle, l.gender, l.age_group, l.google_status,
        l.is_available_for_google <> 0, l.is_available_for_google_display_ads <> 0,
        l.is_available_for_google_shopping_actions <> 0, l.is_available_for_bing <> 0,
        l.is_available_for_facebook <> 0, l.is_available_for_xml <> 0, l.is_deleted <> 0,
        COALESCE(l.created_at AT TIME ZONE 'UTC', now()),
        COALESCE(l.updated_at AT TIME ZONE 'UTC', now())
    FROM {LEGACY_TABLE} l
    WHERE l.id BETWEEN :s AND :e
    ON CONFLICT (id) DO UPDATE SET
        shop_id = EXCLUDED.shop_id, product_id = EXCLUDED.product_id,
        variants_count = EXCLUDED.variants_count, handle = EXCLUDED.handle,
        gender = EXCLUDED.gender, age_group = EXCLUDED.age_group, google_status = EXCLUDED.google_status,
        is_available_for_google = EXCLUDED.is_available_for_google,
        is_available_for_google_display_ads = EXCLUDED.is_available_for_google_display_ads,
        is_available_for_google_shopping_actions = EXCLUDED.is_available_for_google_shopping_actions,
        is_available_for_bing = EXCLUDED.is_available_for_bing,
        is_available_for_facebook = EXCLUDED.is_available_for_facebook,
        is_available_for_xml = EXCLUDED.is_available_for_xml,
        is_deleted = EXCLUDED.is_deleted, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at
    WHERE (products.shop_id, products.product_id, products.variants_count, products.handle,
           products.gender, products.age_group, products.google_status,
           products.is_available_for_google, products.is_available_for_google_display_ads,
           products.is_available_for_google_shopping_actions, products.is_available_for_bing,
           products.is_available_for_facebook, products.is_available_for_xml, products.is_deleted,
           products.created_at, products.updated_at)
          IS DISTINCT FROM
          (EXCLUDED.shop_id, EXCLUDED.product_id, EXCLUDED.variants_count, EXCLUDED.handle,
           EXCLUDED.gender, EXCLUDED.age_group, EXCLUDED.google_status,
           EXCLUDED.is_available_for_google, EXCLUDED.is_available_for_google_display_ads,
           EXCLUDED.is_available_for_google_shopping_actions, EXCLUDED.is_available_for_bing,
           EXCLUDED.is_available_for_facebook, EXCLUDED.is_available_for_xml, EXCLUDED.is_deleted,
           EXCLUDED.created_at, EXCLUDED.updated_at)
    """
)

_COUNT_SOURCE = text(f"SELECT count(*) FROM {LEGACY_TABLE} WHERE id BETWEEN :s AND :e")


def _translations_upsert(fields: list[str]):
    # `fields` were validated against TRANSLATABLE_FIELDS (whitelist) when the job was created;
    # re-check here so this f-string can never carry anything else.
    cols = [TRANSLATABLE_FIELDS[f] for f in fields]  # KeyError => refuse
    values = ", ".join(f"('{f}', l.{c})" for f, c in zip(fields, cols))
    return text(
        f"""
        INSERT INTO product_translations (product_id, language_id, key, value, source)
        SELECT l.id, :language_id, v.key, jsonb_build_object('value', v.value), CAST('import' AS translation_source)
        FROM {LEGACY_TABLE} l
        CROSS JOIN LATERAL (VALUES {values}) AS v(key, value)
        WHERE l.id BETWEEN :s AND :e
          AND v.value ~ '\\S'   -- NULL, '' and whitespace-only values (incl. tabs/newlines) are not translations
        ON CONFLICT (product_id, language_id, key) DO UPDATE
           SET value = EXCLUDED.value, source = EXCLUDED.source, updated_at = now()
         WHERE product_translations.source <= EXCLUDED.source
           AND product_translations.source <> 'human'
           AND (product_translations.value IS DISTINCT FROM EXCLUDED.value
                OR product_translations.source IS DISTINCT FROM EXCLUDED.source)
        """
    )


class ImportBaseProcessor(BatchProcessor):
    def process(self, conn: Connection, ctx: BatchContext) -> int:
        rng = {"s": ctx.range_start, "e": ctx.range_end}
        conn.execute(_PRODUCTS_UPSERT, rng)  # products first: translations FK -> products
        ctx.heartbeat()
        conn.execute(_translations_upsert(ctx.fields), {**rng, "language_id": ctx.language_id})
        return conn.execute(_COUNT_SOURCE, rng).scalar_one()
