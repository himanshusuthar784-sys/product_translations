"""Load the MariaDB dump of `shop_products_1001_test` into Postgres (schema `legacy`).

* Parses the dump's multi-row INSERTs (MySQL string escapes included) - no MySQL needed.
* Creates `legacy.shop_products_1001_test` with Postgres types and bulk-loads via COPY.
* Idempotent: the table is replaced inside one transaction (TRUNCATE + COPY).
* Fails loudly on any malformed row or row-count mismatch; nothing is committed in that case.

The legacy table is an audit/source copy. It is NOT managed by Alembic.

Usage:
    python scripts/load_legacy.py [dump.sql]
Env: DATABASE_URL_SYNC (postgresql+psycopg://... is accepted).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

DEFAULT_SQL = "data/shop_products_1001_test.sql"
N_COLUMNS = 22
EXPECTED_MIN_ROWS = 1

DDL = """
CREATE SCHEMA IF NOT EXISTS legacy;
CREATE TABLE IF NOT EXISTS legacy.shop_products_1001_test (
    id                                       integer      PRIMARY KEY,
    shop_id                                  bigint       NOT NULL,
    product_id                               bigint       NOT NULL UNIQUE,
    title                                    text         NOT NULL,
    variants_count                           integer      NOT NULL DEFAULT 0,
    meta_title                               text,
    handle                                   text         NOT NULL,
    brand                                    varchar(255) NOT NULL,
    gender                                   varchar(30)  NOT NULL,
    age_group                                varchar(30)  NOT NULL,
    tags                                     text,
    google_product_category                  text         NOT NULL,
    google_status                            smallint     NOT NULL DEFAULT 0,
    is_available_for_google                  smallint     NOT NULL DEFAULT 1,
    is_available_for_google_display_ads      smallint     NOT NULL DEFAULT 1,
    is_available_for_google_shopping_actions smallint     NOT NULL DEFAULT 1,
    is_available_for_bing                    smallint     NOT NULL DEFAULT 1,
    is_available_for_facebook                smallint     NOT NULL DEFAULT 1,
    is_available_for_xml                     smallint     NOT NULL DEFAULT 1,
    is_deleted                               smallint     NOT NULL DEFAULT 0,
    created_at                               timestamp,
    updated_at                               timestamp
);
"""

COPY_SQL = "COPY legacy.shop_products_1001_test FROM STDIN"

_ESCAPES = {"n": "\n", "r": "\r", "t": "\t", "0": "\0", "Z": "\x1a", "b": "\b"}


def parse_row(line: str) -> list[str | None]:
    """Parse one `(v1, 'v2', NULL, ...)` tuple line from a mysqldump INSERT."""
    s = line.strip().rstrip(",;").strip()
    if not (s.startswith("(") and s.endswith(")")):
        raise ValueError("not a tuple line")
    s = s[1:-1]
    vals: list[str | None] = []
    i, n = 0, len(s)
    while i < n:
        if s[i] == "'":
            j, buf = i + 1, []
            while True:
                if j >= n:
                    raise ValueError("unterminated string")
                c = s[j]
                if c == "\\":
                    nxt = s[j + 1]
                    buf.append(_ESCAPES.get(nxt, nxt))
                    j += 2
                elif c == "'":
                    if j + 1 < n and s[j + 1] == "'":
                        buf.append("'")
                        j += 2
                    else:
                        break
                else:
                    buf.append(c)
                    j += 1
            vals.append("".join(buf))
            i = j + 1
        else:
            j = s.find(",", i)
            j = n if j < 0 else j
            tok = s[i:j].strip()
            vals.append(None if tok.upper() == "NULL" else tok)
            i = j
        if i < n and s[i] == ",":
            i += 1
        while i < n and s[i] == " ":
            i += 1
    return vals


def read_rows(path: Path) -> list[list[str | None]]:
    rows: list[list[str | None]] = []
    in_values = False
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            if line.startswith("INSERT INTO"):
                in_values = True
                continue
            if in_values and line.startswith("("):
                try:
                    row = parse_row(line)
                except Exception as exc:  # noqa: BLE001 - re-raise with location
                    raise SystemExit(f"line {lineno}: cannot parse row: {exc}") from exc
                if len(row) != N_COLUMNS:
                    raise SystemExit(f"line {lineno}: expected {N_COLUMNS} columns, got {len(row)}")
                rows.append(row)
            elif in_values and line.strip() and not line.startswith("("):
                in_values = False
    return rows


def main() -> None:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SQL)
    dsn = os.environ["DATABASE_URL_SYNC"].replace("postgresql+psycopg://", "postgresql://")
    rows = read_rows(path)
    if len(rows) < EXPECTED_MIN_ROWS:
        raise SystemExit("no rows parsed - refusing to truncate the legacy table")
    ids = {r[0] for r in rows}
    if len(ids) != len(rows):
        raise SystemExit("duplicate ids in dump")

    with psycopg.connect(dsn) as conn:  # one transaction: all-or-nothing
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE legacy.shop_products_1001_test")
            with cur.copy(COPY_SQL) as copy:
                for r in rows:
                    copy.write_row(r)
            cur.execute("SELECT count(*) FROM legacy.shop_products_1001_test")
            loaded = cur.fetchone()[0]
            if loaded != len(rows):
                raise SystemExit(f"row-count mismatch: parsed {len(rows)}, loaded {loaded}")
            cur.execute(
                "SELECT count(*) FROM legacy.shop_products_1001_test "
                "WHERE title LIKE '%' || chr(65533) || '%' OR tags LIKE '%' || chr(65533) || '%' "
                "OR brand LIKE '%' || chr(65533) || '%' OR google_product_category LIKE '%' || chr(65533) || '%'"
            )
            corrupted = cur.fetchone()[0]
    print(f"loaded {loaded} rows into legacy.shop_products_1001_test")
    print(f"WARNING: {corrupted} rows contain U+FFFD (corrupted source text; loaded as-is)" if corrupted else "no U+FFFD characters found")


if __name__ == "__main__":
    main()
