"""Unit tests for ETL parsers: parse_index_bnss and parse_crosswalk_bnss_crpc."""

import pytest

from bnss_pipeline.etl_bnss import (
    BnssSectionIndexRow,
    CrosswalkRow,
    parse_crosswalk_bnss_crpc,
    parse_index_bnss,
)

COMMON_KWARGS = {
    "source_url": "https://example.com/test",
    "content_hash": "abc123",
    "version": "bnss@2026-01-01",
}


class TestParseIndexBnss:
    """Tests for parse_index_bnss."""

    def test_happy_path(self, sample_index_html: str) -> None:
        rows = parse_index_bnss(sample_index_html, **COMMON_KWARGS)

        assert len(rows) >= 3
        assert all(isinstance(r, BnssSectionIndexRow) for r in rows)

        first = rows[0]
        assert first.chapter_no == 1
        assert first.section_no == 1
        assert first.law == "BNSS"
        assert first.canonical_id == "BNSS:CH01:S001"
        assert first.source_url == COMMON_KWARGS["source_url"]
        assert first.content_hash == COMMON_KWARGS["content_hash"]
        assert first.version == COMMON_KWARGS["version"]

    def test_chapter_2_section(self, sample_index_html: str) -> None:
        rows = parse_index_bnss(sample_index_html, **COMMON_KWARGS)

        ch2_rows = [r for r in rows if r.chapter_no == 2]
        assert len(ch2_rows) >= 1
        assert ch2_rows[0].section_no == 3
        assert ch2_rows[0].canonical_id == "BNSS:CH02:S003"

    def test_no_chapters_raises(self, empty_html: str) -> None:
        with pytest.raises(ValueError, match="No CHAPTER headings found"):
            parse_index_bnss(empty_html, **COMMON_KWARGS)

    def test_chapters_but_no_sections_raises(self) -> None:
        html = "<html><body>CHAPTER I PRELIMINARY</body></html>"
        with pytest.raises(ValueError, match="produced 0 rows"):
            parse_index_bnss(html, **COMMON_KWARGS)

    def test_section_titles_are_cleaned(self, sample_index_html: str) -> None:
        rows = parse_index_bnss(sample_index_html, **COMMON_KWARGS)
        for r in rows:
            assert not r.section_title.startswith(" ")
            assert not r.section_title.endswith(".")


class TestParseCrosswalkBnssCrpc:
    """Tests for parse_crosswalk_bnss_crpc."""

    def test_happy_path(self, sample_crosswalk_html: str) -> None:
        rows = parse_crosswalk_bnss_crpc(sample_crosswalk_html, **COMMON_KWARGS)

        assert len(rows) == 3
        assert all(isinstance(r, CrosswalkRow) for r in rows)

        first = rows[0]
        assert first.bnss_section_no == "1"
        assert first.crpc_section_no == "1"
        assert first.source_url == COMMON_KWARGS["source_url"]

    def test_remarks_captured(self, sample_crosswalk_html: str) -> None:
        rows = parse_crosswalk_bnss_crpc(sample_crosswalk_html, **COMMON_KWARGS)
        assert rows[0].remarks == "No change"
        assert rows[1].remarks == "Modified"
        assert rows[2].remarks == "Renumbered"

    def test_no_table_raises(self, no_table_html: str) -> None:
        with pytest.raises(ValueError, match="No <table> found"):
            parse_crosswalk_bnss_crpc(no_table_html, **COMMON_KWARGS)

    def test_empty_rows_raises(self, crosswalk_empty_rows_html: str) -> None:
        with pytest.raises(ValueError, match="produced 0 rows"):
            parse_crosswalk_bnss_crpc(crosswalk_empty_rows_html, **COMMON_KWARGS)

    def test_version_propagated(self, sample_crosswalk_html: str) -> None:
        rows = parse_crosswalk_bnss_crpc(sample_crosswalk_html, **COMMON_KWARGS)
        for r in rows:
            assert r.version == "bnss@2026-01-01"
