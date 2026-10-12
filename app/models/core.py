"""languages, products, product_translations.

Schema is created by Alembic (hand-written DDL in alembic/versions); these models mirror it.
"""
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base
from app.models.enums import TranslationSource

TS = DateTime(timezone=True)


class Language(Base):
    __tablename__ = "languages"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    language_code: Mapped[str] = mapped_column(String(16), nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("language_code", name="uq_languages_language_code"),
        # exactly one default language
        Index("uq_languages_single_default", "is_default", unique=True, postgresql_where=text("is_default")),
    )


class Product(Base):
    """Non-translatable product attributes only. Translatable fields live in product_translations."""

    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # legacy ids are preserved
    shop_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    product_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    variants_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    handle: Mapped[str] = mapped_column(Text, nullable=False)
    gender: Mapped[str] = mapped_column(String(30), nullable=False)
    age_group: Mapped[str] = mapped_column(String(30), nullable=False)
    google_status: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("0"))
    is_available_for_google: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    is_available_for_google_display_ads: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    is_available_for_google_shopping_actions: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    is_available_for_bing: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    is_available_for_facebook: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    is_available_for_xml: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    created_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        # business key; also serves (shop_id) and (shop_id, product_id) lookups
        UniqueConstraint("shop_id", "product_id", name="uq_products_shop_id_product_id"),
        # "latest products of a shop" + keyset pagination
        Index("ix_products_shop_created", "shop_id", text("created_at DESC"), text("id DESC")),
    )


class ProductTranslation(Base):
    """One row per (product, language, field). Adding a language/field is data, not DDL."""

    __tablename__ = "product_translations"

    product_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("products.id", ondelete="CASCADE"), primary_key=True
    )
    language_id: Mapped[int] = mapped_column(
        SmallInteger, ForeignKey("languages.id", ondelete="RESTRICT"), primary_key=True
    )
    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[dict] = mapped_column(JSONB, nullable=False)  # {"value": <any>}
    source: Mapped[TranslationSource] = mapped_column(
        SAEnum(
            TranslationSource,
            name="translation_source",
            values_callable=lambda e: [m.value for m in e],
            create_type=False,
        ),
        nullable=False,
        server_default=TranslationSource.IMPORT.value,
    )
    created_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now(), onupdate=func.now())
