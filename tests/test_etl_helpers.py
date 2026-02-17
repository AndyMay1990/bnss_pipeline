"""Tests for ETL helper functions."""

import pytest

from bnss_pipeline.etl_bnss import (
    _clean_cell_text,
    _plain_url,
    _roman_to_int,
    _split_section_cell,
    _validate_as_of,
    canonical_id_bnss,
)


class TestRomanToInt:
    @pytest.mark.parametrize(
        "roman, expected",
        [
            ("I", 1),
            ("IV", 4),
            ("IX", 9),
            ("XIV", 14),
            ("XL", 40),
            ("XLII", 42),
            ("XC", 90),
            ("C", 100),
            ("XXXIX", 39),
        ],
    )
    def test_conversion(self, roman: str, expected: int):
        assert _roman_to_int(roman) == expected

    def test_lowercase(self):
        assert _roman_to_int("xiv") == 14


class TestCanonicalId:
    def test_padding(self):
        assert canonical_id_bnss(1, 1) == "BNSS:CH01:S001"

    def test_large_numbers(self):
        assert canonical_id_bnss(39, 532) == "BNSS:CH39:S532"


class TestValidateAsOf:
    def test_valid_date(self):
        assert _validate_as_of("2026-01-10") == "2026-01-10"

    def test_invalid_date_raises(self):
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            _validate_as_of("not-a-date")

    def test_partial_date_raises(self):
        with pytest.raises(ValueError):
            _validate_as_of("2026-13-01")


class TestCleanCellText:
    def test_collapses_whitespace(self):
        assert _clean_cell_text("  hello   world  ") == "hello world"

    def test_strips_change_tag(self):
        assert _clean_cell_text("Title (Change) here") == "Title here"

    def test_strips_trailing_dot(self):
        assert _clean_cell_text("Some title.") == "Some title"


class TestPlainUrl:
    def test_plain_url_passthrough(self):
        url = "https://example.com/page.html"
        assert _plain_url(url) == url

    def test_unwraps_markdown(self):
        md = "[Page](https://example.com/page.html)"
        assert _plain_url(md) == "https://example.com/page.html"

    def test_empty_string(self):
        assert _plain_url("") == ""

    def test_none_becomes_empty(self):
        assert _plain_url(None) == ""


class TestSplitSectionCell:
    def test_basic_split(self):
        no, title = _split_section_cell("1. Short title")
        assert no == "1"
        assert title == "Short title"

    def test_empty_returns_none(self):
        no, title = _split_section_cell("")
        assert no is None
        assert title is None

    def test_subsection(self):
        no, title = _split_section_cell("497(2) Bail conditions")
        assert no == "497(2)"
        assert title == "Bail conditions"
