"""Unit tests for ETL helper functions."""

import pytest

from bnss_pipeline.etl_bnss import (
    _clean_cell_text,
    _roman_to_int,
    _validate_as_of,
    canonical_id_bnss,
)


class TestRomanToInt:
    """Tests for _roman_to_int."""

    @pytest.mark.parametrize(
        "roman, expected",
        [
            ("I", 1),
            ("II", 2),
            ("III", 3),
            ("IV", 4),
            ("V", 5),
            ("IX", 9),
            ("X", 10),
            ("XIV", 14),
            ("XL", 40),
            ("L", 50),
            ("XC", 90),
            ("C", 100),
            ("XXXVII", 37),
        ],
    )
    def test_valid_numerals(self, roman: str, expected: int) -> None:
        assert _roman_to_int(roman) == expected

    def test_case_insensitive(self) -> None:
        assert _roman_to_int("iv") == 4
        assert _roman_to_int("Xiv") == 14

    def test_whitespace_stripped(self) -> None:
        assert _roman_to_int("  III  ") == 3

    def test_invalid_character_raises(self) -> None:
        with pytest.raises(KeyError):
            _roman_to_int("ABC")


class TestCanonicalId:
    """Tests for canonical_id_bnss."""

    def test_basic(self) -> None:
        assert canonical_id_bnss(1, 1) == "BNSS:CH01:S001"
        assert canonical_id_bnss(10, 100) == "BNSS:CH10:S100"
        assert canonical_id_bnss(37, 532) == "BNSS:CH37:S532"

    def test_zero_padding(self) -> None:
        assert canonical_id_bnss(1, 1) == "BNSS:CH01:S001"
        assert canonical_id_bnss(9, 9) == "BNSS:CH09:S009"


class TestCleanCellText:
    """Tests for _clean_cell_text."""

    def test_collapses_whitespace(self) -> None:
        assert _clean_cell_text("  hello   world  ") == "hello world"

    def test_strips_trailing_dot(self) -> None:
        assert _clean_cell_text("Some title.") == "Some title"

    def test_removes_change_annotation(self) -> None:
        assert _clean_cell_text("Title (Change) here") == "Title here"

    def test_empty_string(self) -> None:
        assert _clean_cell_text("") == ""
        assert _clean_cell_text("   ") == ""


class TestValidateAsOf:
    """Tests for _validate_as_of."""

    def test_valid_date(self) -> None:
        assert _validate_as_of("2026-01-15") == "2026-01-15"

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            _validate_as_of("15-01-2026")

    def test_invalid_date_raises(self) -> None:
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            _validate_as_of("not-a-date")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _validate_as_of("")
