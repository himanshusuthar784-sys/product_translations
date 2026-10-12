"""The MariaDB dump parser (scripts/load_legacy.py): string escapes, NULLs, malformed input."""
import pytest

from scripts.load_legacy import N_COLUMNS, parse_row, read_rows


def make_tuple(**override) -> str:
    vals = ["1", "3", "212112277530", "'Title'", "1", "''", "'handle'", "'ACME'", "'male'", "'kids'", "NULL",
            "'Cat'", "0", "1", "1", "1", "1", "1", "1", "0", "'2019-01-24 19:35:43'", "'2019-10-25 01:12:41'"]
    for idx, v in override.items():
        vals[int(idx)] = v
    assert len(vals) == N_COLUMNS
    return "(" + ", ".join(vals) + ")"


def test_parses_numbers_strings_and_null():
    row = parse_row(make_tuple() + ",")
    assert row[0] == "1"
    assert row[3] == "Title"
    assert row[5] == ""          # empty string stays empty string
    assert row[10] is None       # NULL -> None
    assert row[20] == "2019-01-24 19:35:43"
    assert len(row) == N_COLUMNS


@pytest.mark.parametrize(
    "sql_literal, expected",
    [
        (r"'It\'s fine'", "It's fine"),                 # backslash-escaped quote
        ("'It''s fine'", "It's fine"),                  # doubled quote
        (r"'back\\slash'", "back\\slash"),              # escaped backslash
        (r"'line\nbreak\ttab'", "line\nbreak\ttab"),    # \n \t
        ("'a, b (c), d'", "a, b (c), d"),               # commas / parens inside strings
        ("'Home & Garden > Decor'", "Home & Garden > Decor"),
        ("'M��bel'", "M��bel"),     # corrupted source text is kept verbatim
        ("'Crème Brûlée 日本'", "Crème Brûlée 日本"),
        ("''", ""),
    ],
)
def test_string_escapes(sql_literal, expected):
    assert parse_row(make_tuple(**{"3": sql_literal}))[3] == expected


def test_trailing_separators_are_ignored():
    assert parse_row(make_tuple() + ";") == parse_row(make_tuple() + ",") == parse_row(make_tuple())


@pytest.mark.parametrize("bad", ["not a tuple", "'unterminated", "(1, 'unterminated)", ""])
def test_malformed_rows_raise(bad):
    with pytest.raises(Exception):
        parse_row(bad)


def test_read_rows_collects_rows_across_insert_statements(tmp_path):
    f = tmp_path / "dump.sql"
    f.write_text(
        "-- header\n"
        "INSERT INTO `t` (`a`) VALUES\n"
        + make_tuple() + ",\n" + make_tuple(**{"0": "2"}) + ";\n"
        "\n"
        "INSERT INTO `t` (`a`) VALUES\n"
        + make_tuple(**{"0": "3"}) + ";\n"
        "ALTER TABLE `t` ADD PRIMARY KEY (`id`);\n",
        encoding="utf-8",
    )
    rows = read_rows(f)
    assert [r[0] for r in rows] == ["1", "2", "3"]


def test_read_rows_rejects_wrong_column_count(tmp_path):
    f = tmp_path / "dump.sql"
    f.write_text("INSERT INTO `t` VALUES\n(1, 2, 3);\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="expected 22 columns"):
        read_rows(f)


def test_read_rows_ignores_parenthesised_lines_outside_inserts(tmp_path):
    f = tmp_path / "dump.sql"
    f.write_text("(this is a comment-like line)\nINSERT INTO `t` VALUES\n" + make_tuple() + ";\n", encoding="utf-8")
    assert len(read_rows(f)) == 1
