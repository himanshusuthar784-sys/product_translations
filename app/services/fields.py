"""Registry of translatable fields: translation `key` -> column in the legacy source table.

This is the ONLY place to touch to make another legacy column translatable. No DDL is needed:
`product_translations.key` is free text. Keys without a legacy column (e.g. future `features`,
`specifications`) are written directly through the translation provider / API.

Column names are interpolated into SQL, so they come exclusively from this whitelist - never
from request data.
"""

TRANSLATABLE_FIELDS: dict[str, str] = {
    "title": "title",
    "meta_title": "meta_title",
    "brand": "brand",
    "tags": "tags",
    "google_product_category": "google_product_category",
}

LEGACY_TABLE = "legacy.shop_products_1001_test"


def validate_fields(requested: list[str] | None) -> list[str]:
    """Return the effective, de-duplicated, ordered field list or raise ValueError."""
    if requested is None:
        return list(TRANSLATABLE_FIELDS)
    if not requested:
        raise ValueError("fields must not be empty")
    unknown = sorted(set(requested) - set(TRANSLATABLE_FIELDS))
    if unknown:
        raise ValueError(f"unknown translatable field(s): {', '.join(unknown)}")
    return [f for f in TRANSLATABLE_FIELDS if f in set(requested)]
