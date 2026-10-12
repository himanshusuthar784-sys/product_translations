"""scripts/load_legacy.py against a real Postgres: COPY load, idempotent reload, all-or-nothing behaviour."""
import sys

import pytest

from scripts import load_legacy
from tests.helpers import rows, scalar
from tests.unit.test_load_legacy_parser import make_tuple


def write_dump(path, tuples):
    path.write_text("INSERT INTO `t` VALUES\n" + ",\n".join(tuples) + ";\n", encoding="utf-8")


def load(monkeypatch, path):
    monkeypatch.setattr(sys, "argv", ["load_legacy", str(path)])
    load_legacy.main()


def test_loads_rows_with_converted_types(db, tmp_path, monkeypatch):
    f = tmp_path / "d.sql"
    write_dump(f, [make_tuple(), make_tuple(**{"0": "2", "2": "212112277531", "3": r"'It\'s a \"vase\"'", "10": "'a, b'", "19": "1"})])
    load(monkeypatch, f)
    got = rows(db, "SELECT id, shop_id, product_id, title, meta_title, tags, is_deleted, created_at FROM legacy.shop_products_1001_test ORDER BY id")
    assert [g.id for g in got] == [1, 2] and got[1].product_id == 212112277531 and got[1].title == 'It\'s a "vase"'
    assert got[0].tags is None and got[0].meta_title == "" and got[1].is_deleted == 1
    assert str(got[0].created_at) == "2019-01-24 19:35:43"


def test_reload_replaces_the_snapshot_and_is_idempotent(db, tmp_path, monkeypatch):
    f = tmp_path / "d.sql"
    write_dump(f, [make_tuple(**{"0": str(i), "2": str(1000 + i)}) for i in range(1, 6)])
    load(monkeypatch, f)
    load(monkeypatch, f)
    assert scalar(db, "SELECT count(*) FROM legacy.shop_products_1001_test") == 5
    write_dump(f, [make_tuple(**{"0": "9", "2": "9999"})])
    load(monkeypatch, f)
    assert [r.id for r in rows(db, "SELECT id FROM legacy.shop_products_1001_test")] == [9]


@pytest.mark.parametrize(
    "content, message",
    [
        ("INSERT INTO `t` VALUES\n(1, 2, 3);\n", "expected 22 columns"),
        ("-- nothing useful here\n", "no rows parsed"),
    ],
)
def test_bad_input_aborts_and_leaves_the_existing_snapshot_untouched(db, tmp_path, monkeypatch, content, message):
    good = tmp_path / "good.sql"
    write_dump(good, [make_tuple(), make_tuple(**{"0": "2", "2": "5"})])
    load(monkeypatch, good)
    bad = tmp_path / "bad.sql"
    bad.write_text(content, encoding="utf-8")
    with pytest.raises(SystemExit, match=message):
        load(monkeypatch, bad)
    assert scalar(db, "SELECT count(*) FROM legacy.shop_products_1001_test") == 2


def test_duplicate_ids_are_rejected_before_touching_the_table(db, tmp_path, monkeypatch):
    good = tmp_path / "good.sql"
    write_dump(good, [make_tuple()])
    load(monkeypatch, good)
    dup = tmp_path / "dup.sql"
    write_dump(dup, [make_tuple(), make_tuple()])
    with pytest.raises(SystemExit, match="duplicate ids"):
        load(monkeypatch, dup)
    assert scalar(db, "SELECT count(*) FROM legacy.shop_products_1001_test") == 1


def test_reports_corrupted_characters(db, tmp_path, monkeypatch, capsys):
    f = tmp_path / "d.sql"
    write_dump(f, [make_tuple(**{"3": "'M��bel'"}), make_tuple(**{"0": "2", "2": "5"})])
    load(monkeypatch, f)
    out = capsys.readouterr().out
    assert "loaded 2 rows" in out and "1 rows contain U+FFFD" in out
    assert scalar(db, "SELECT title FROM legacy.shop_products_1001_test WHERE id = 1") == "M��bel"      # stored verbatim
