"""Rebuild the original SQL dump from a browser-saved Dropbox *preview page* (.mhtml).

The assessment's SQL file was saved as an MHTML snapshot of Dropbox's text previewer, where
each SQL line is one `<li data-slate-node="element">`. This extracts those lines back into a
plain .sql file. If you have the raw .sql from Dropbox, skip this step and pass it straight to
`load_legacy.py`.

Usage:
    python scripts/extract_sql_from_mhtml.py [src.mhtml] [dst.sql]
"""
from __future__ import annotations

import email
import html
import re
import sys
from email import policy
from pathlib import Path

DEFAULT_SRC = "shop_products_1001_test.sql.mhtml"
DEFAULT_DST = "data/shop_products_1001_test.sql"

_LINE_RE = re.compile(r'<li data-slate-node="element"[^>]*>(.*?)</li>', re.S)
_TAG_RE = re.compile(r"<[^>]+>")


def extract(src: Path) -> list[str]:
    msg = email.message_from_binary_file(src.open("rb"), policy=policy.default)
    pages = [p.get_content() for p in msg.walk() if p.get_content_type() == "text/html"]
    if not pages:
        raise SystemExit("no text/html part found in the MHTML file")
    lines = _LINE_RE.findall(pages[0])
    # Empty lines are rendered as a zero-width BOM span.
    return [html.unescape(_TAG_RE.sub("", ln)).replace("﻿", "") for ln in lines]


def main() -> None:
    src = Path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SRC)
    dst = Path(sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DST)
    lines = extract(src)
    inserts = sum(1 for ln in lines if ln.startswith("INSERT INTO"))
    rows = sum(1 for ln in lines if ln.startswith("("))
    if not inserts or not rows:
        raise SystemExit(f"extraction failed: {inserts} INSERT statements, {rows} row lines")
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {dst} : {len(lines)} lines, {inserts} INSERT statements, {rows} row lines")


if __name__ == "__main__":
    main()
