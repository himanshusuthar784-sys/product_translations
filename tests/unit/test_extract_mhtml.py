"""scripts/extract_sql_from_mhtml.py on a miniature Dropbox-preview MHTML."""
import sys

import pytest

from scripts import extract_sql_from_mhtml as ex

LI = '<li data-slate-node="element" class="_lineContainer_x"><div><span data-slate-node="text"><span data-slate-leaf="true"><span data-slate-string="true">{}</span></span></span></div></li>'
EMPTY_LI = '<li data-slate-node="element" class="_lineContainer_x"><div><span><span data-slate-zero-width="n">﻿<br></span></span></div></li>'


def build_mhtml(lines: list[str]) -> bytes:
    body = "<html><body><ul>" + "".join(EMPTY_LI if ln is None else LI.format(ln) for ln in lines) + "</ul></body></html>"
    return (
        "From: <Saved by Blink>\r\nMIME-Version: 1.0\r\n"
        'Content-Type: multipart/related; type="text/html"; boundary="BOUND"\r\n\r\n'
        "--BOUND\r\nContent-Type: text/html; charset=utf-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n"
        + body + "\r\n--BOUND--\r\n"
    ).encode("utf-8")


def test_extracts_lines_unescapes_html_and_keeps_blank_lines(tmp_path):
    src = tmp_path / "in.mhtml"
    src.write_bytes(
        build_mhtml(
            [
                "CREATE TABLE `t` (",
                None,
                "INSERT INTO `t` VALUES",
                "(1, &#39;Home &amp; Garden &gt; Decor&#39;),",
            ]
        )
    )
    lines = ex.extract(src)
    assert lines == ["CREATE TABLE `t` (", "", "INSERT INTO `t` VALUES", "(1, 'Home & Garden > Decor'),"]


def test_main_writes_sql_file_and_reports_counts(tmp_path, monkeypatch, capsys):
    src, dst = tmp_path / "in.mhtml", tmp_path / "out" / "dump.sql"
    src.write_bytes(build_mhtml(["INSERT INTO `t` VALUES", "(1),", "(2);"]))
    monkeypatch.setattr(sys, "argv", ["x", str(src), str(dst)])
    ex.main()
    assert dst.read_text(encoding="utf-8") == "INSERT INTO `t` VALUES\n(1),\n(2);\n"
    assert "1 INSERT statements, 2 row lines" in capsys.readouterr().out


def test_main_fails_loudly_when_page_has_no_sql(tmp_path, monkeypatch):
    src = tmp_path / "in.mhtml"
    src.write_bytes(build_mhtml(["just some page text"]))
    monkeypatch.setattr(sys, "argv", ["x", str(src), str(tmp_path / "o.sql")])
    with pytest.raises(SystemExit, match="extraction failed"):
        ex.main()
    assert not (tmp_path / "o.sql").exists()


def test_main_fails_when_no_html_part(tmp_path, monkeypatch):
    src = tmp_path / "in.mhtml"
    src.write_bytes(b"MIME-Version: 1.0\r\nContent-Type: text/plain\r\n\r\nhello")
    monkeypatch.setattr(sys, "argv", ["x", str(src), str(tmp_path / "o.sql")])
    with pytest.raises(SystemExit):
        ex.main()
