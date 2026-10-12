import json

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.models import Language, Product, ProductTranslation
from app.schemas import ProductOut, SetTranslationsRequest
from app.services.errors import NotFound, Unprocessable

router = APIRouter(prefix="/products", tags=["products"])


async def _language(session: AsyncSession, code: str) -> Language:
    lang = await session.scalar(select(Language).where(Language.language_code == code))
    if lang is None:
        raise NotFound("language", code)
    return lang


@router.get("/{product_id}", response_model=ProductOut)
async def get_product(
    product_id: int,
    lang: str = Query(default="en", description="language code"),
    fallback: bool = Query(default=True, description="fill missing keys from the default language"),
    session: AsyncSession = Depends(get_session),
):
    """Read path. One indexed query on the translations PK: (product_id, language_id IN (lang, default))."""
    product = await session.get(Product, product_id)
    if product is None:
        raise NotFound("product", str(product_id))
    requested = await _language(session, lang)
    default = await session.scalar(select(Language).where(Language.is_default.is_(True)))

    wanted = {requested.id} | ({default.id} if (fallback and default) else set())
    rows = (
        await session.execute(
            select(ProductTranslation.language_id, ProductTranslation.key, ProductTranslation.value).where(
                ProductTranslation.product_id == product_id, ProductTranslation.language_id.in_(wanted)
            )
        )
    ).all()

    translations: dict = {k: v["value"] for lid, k, v in rows if lid == requested.id}
    fallback_keys: list[str] = []
    if fallback and default and default.id != requested.id:
        for lid, k, v in rows:
            if lid == default.id and k not in translations:
                translations[k] = v["value"]
                fallback_keys.append(k)

    return ProductOut(
        id=product.id,
        shop_id=product.shop_id,
        product_id=product.product_id,
        handle=product.handle,
        gender=product.gender,
        age_group=product.age_group,
        variants_count=product.variants_count,
        is_deleted=product.is_deleted,
        language=requested.language_code,
        translations=translations,
        fallback_keys=sorted(fallback_keys),
    )


@router.put("/{product_id}/translations/{language_code}", response_model=dict)
async def set_translations(
    product_id: int,
    language_code: str,
    body: SetTranslationsRequest,
    session: AsyncSession = Depends(get_session),
):
    """Human edit. Any key is accepted (new translatable fields need NO schema change) and rows are
    stored with source='human', which migration jobs never overwrite."""
    if await session.get(Product, product_id) is None:
        raise NotFound("product", str(product_id))
    lang = await _language(session, language_code)
    if not lang.is_active:
        raise Unprocessable("language_inactive", f"language '{language_code}' is not active")

    await session.execute(
        text(
            """
            INSERT INTO product_translations (product_id, language_id, key, value, source)
            SELECT :pid, :lid, t.k, CAST(t.v AS jsonb), 'human'
            FROM unnest(CAST(:keys AS text[]), CAST(:vals AS text[])) AS t(k, v)
            ON CONFLICT (product_id, language_id, key) DO UPDATE
               SET value = EXCLUDED.value, source = 'human', updated_at = now()
            """
        ),
        {
            "pid": product_id,
            "lid": lang.id,
            "keys": list(body.translations),
            "vals": [json.dumps({"value": v}, ensure_ascii=False) for v in body.translations.values()],
        },
    )
    await session.commit()
    return {"product_id": product_id, "language": language_code, "updated_keys": sorted(body.translations)}
