"""core schema: languages, products, product_translations (+ seed en/fr/bg)

Revision ID: 0001
Revises:
Create Date: 2026-10-12
"""
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Declaration order IS the precedence order (PG compares enums by position):
    # a job may overwrite a row only if existing <= incoming, and never a 'human' row.
    op.execute("CREATE TYPE translation_source AS ENUM ('fallback', 'import', 'machine', 'human')")

    # ------------------------------------------------------------------ languages
    op.execute(
        """
        CREATE TABLE languages (
            id            smallint GENERATED ALWAYS AS IDENTITY,
            name          varchar(100) NOT NULL,
            language_code varchar(16)  NOT NULL,
            is_default    boolean      NOT NULL DEFAULT false,
            is_active     boolean      NOT NULL DEFAULT true,
            created_at    timestamptz  NOT NULL DEFAULT now(),
            updated_at    timestamptz  NOT NULL DEFAULT now(),
            CONSTRAINT pk_languages PRIMARY KEY (id),
            CONSTRAINT uq_languages_language_code UNIQUE (language_code)
        )
        """
    )
    # exactly one default language
    op.execute("CREATE UNIQUE INDEX uq_languages_single_default ON languages (is_default) WHERE is_default")

    # ------------------------------------------------------------------- products
    op.execute(
        """
        CREATE TABLE products (
            id                                       integer     NOT NULL,
            shop_id                                  bigint      NOT NULL,
            product_id                               bigint      NOT NULL,
            variants_count                           integer     NOT NULL DEFAULT 0,
            handle                                   text        NOT NULL,
            gender                                   varchar(30) NOT NULL,
            age_group                                varchar(30) NOT NULL,
            google_status                            smallint    NOT NULL DEFAULT 0,
            is_available_for_google                  boolean     NOT NULL DEFAULT true,
            is_available_for_google_display_ads      boolean     NOT NULL DEFAULT true,
            is_available_for_google_shopping_actions boolean     NOT NULL DEFAULT true,
            is_available_for_bing                    boolean     NOT NULL DEFAULT true,
            is_available_for_facebook                boolean     NOT NULL DEFAULT true,
            is_available_for_xml                     boolean     NOT NULL DEFAULT true,
            is_deleted                               boolean     NOT NULL DEFAULT false,
            created_at                               timestamptz NOT NULL DEFAULT now(),
            updated_at                               timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT pk_products PRIMARY KEY (id),
            CONSTRAINT uq_products_shop_id_product_id UNIQUE (shop_id, product_id)
        )
        """
    )
    op.execute("CREATE INDEX ix_products_shop_created ON products (shop_id, created_at DESC, id DESC)")

    # ------------------------------------------------------- product_translations
    # Composite PK = the only index: PK, ON CONFLICT target and lookup path in one.
    # Prefix (product_id) and (product_id, language_id) lookups are covered by it.
    op.execute(
        """
        CREATE TABLE product_translations (
            product_id  integer            NOT NULL,
            language_id smallint           NOT NULL,
            key         text               NOT NULL,
            value       jsonb              NOT NULL,
            source      translation_source NOT NULL DEFAULT 'import',
            created_at  timestamptz        NOT NULL DEFAULT now(),
            updated_at  timestamptz        NOT NULL DEFAULT now(),
            CONSTRAINT pk_product_translations PRIMARY KEY (product_id, language_id, key),
            CONSTRAINT fk_product_translations_product_id_products
                FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE,
            CONSTRAINT fk_product_translations_language_id_languages
                FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT,
            CONSTRAINT ck_product_translations_value_has_value CHECK (value ? 'value')
        )
        """
    )

    # ----------------------------------------------------------------------- seed
    op.execute(
        """
        INSERT INTO languages (name, language_code, is_default) VALUES
            ('English',   'en', true),
            ('French',    'fr', false),
            ('Bulgarian', 'bg', false)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE product_translations")
    op.execute("DROP TABLE products")
    op.execute("DROP TABLE languages")
    op.execute("DROP TYPE translation_source")
