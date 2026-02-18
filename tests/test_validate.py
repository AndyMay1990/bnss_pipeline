"""Unit tests for dataset validation."""

import json
from pathlib import Path

import pytest

from bnss_pipeline.validate import (
    ValidationReport,
    ValidationResult,
    check_crosswalk_no_duplicates,
    check_crosswalk_schema,
    check_file_exists,
    check_sections_gaps,
    check_sections_no_duplicates,
    check_sections_schema,
    check_version_consistency,
)


class TestValidationResult:
    """Tests for ValidationResult."""

    def test_pass_summary(self) -> None:
        r = ValidationResult(check_name="test", passed=True, message="All good")
        assert "[PASS]" in r.summary()
        assert "test" in r.summary()

    def test_fail_summary(self) -> None:
        r = ValidationResult(check_name="test", passed=False, message="Bad")
        assert "[FAIL]" in r.summary()


class TestValidationReport:
    """Tests for ValidationReport."""

    def test_all_pass(self) -> None:
        report = ValidationReport(
            results=[
                ValidationResult(check_name="a", passed=True, message="ok"),
                ValidationResult(check_name="b", passed=True, message="ok"),
            ]
        )
        assert report.passed is True
        assert report.passed_count == 2
        assert report.failed_count == 0

    def test_some_fail(self) -> None:
        report = ValidationReport(
            results=[
                ValidationResult(check_name="a", passed=True, message="ok"),
                ValidationResult(check_name="b", passed=False, message="bad"),
            ]
        )
        assert report.passed is False
        assert report.failed_count == 1

    def test_empty_report_passes(self) -> None:
        report = ValidationReport()
        assert report.passed is True


class TestCheckFileExists:
    """Tests for check_file_exists."""

    def test_existing_file(self, tmp_path: Path) -> None:
        f = tmp_path / "test.jsonl"
        f.write_text('{"a": 1}\n')
        result = check_file_exists(f, "test")
        assert result.passed is True

    def test_missing_file(self, tmp_path: Path) -> None:
        f = tmp_path / "missing.jsonl"
        result = check_file_exists(f, "test")
        assert result.passed is False
        assert "does not exist" in result.message

    def test_empty_file(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.jsonl"
        f.write_text("")
        result = check_file_exists(f, "test")
        assert result.passed is False
        assert "empty" in result.message


class TestCheckSectionsNoDuplicates:
    """Tests for check_sections_no_duplicates."""

    def test_no_duplicates(self) -> None:
        rows = [{"section_no": 1}, {"section_no": 2}, {"section_no": 3}]
        result = check_sections_no_duplicates(rows)
        assert result.passed is True

    def test_with_duplicates(self) -> None:
        rows = [{"section_no": 1}, {"section_no": 1}, {"section_no": 2}]
        result = check_sections_no_duplicates(rows)
        assert result.passed is False
        assert "duplicate" in result.message.lower()


class TestCheckSectionsGaps:
    """Tests for check_sections_gaps."""

    def test_contiguous(self) -> None:
        rows = [{"section_no": 1}, {"section_no": 2}, {"section_no": 3}]
        result = check_sections_gaps(rows)
        assert result.passed is True

    def test_with_gaps(self) -> None:
        rows = [{"section_no": 1}, {"section_no": 3}, {"section_no": 5}]
        result = check_sections_gaps(rows)
        assert result.passed is False
        assert "gap" in result.message.lower()

    def test_empty_rows(self) -> None:
        result = check_sections_gaps([])
        assert result.passed is False


class TestCheckSectionsSchema:
    """Tests for check_sections_schema."""

    def test_valid_rows(self) -> None:
        rows = [{
            "canonical_id": "BNSS:CH01:S001",
            "law": "BNSS",
            "chapter_no": 1,
            "chapter_title": "PRELIMINARY",
            "section_no": 1,
            "section_title": "Short title",
            "source_url": "https://example.com",
            "content_hash": "abc",
            "version": "bnss@2026-01-01",
        }]
        result = check_sections_schema(rows)
        assert result.passed is True

    def test_missing_fields(self) -> None:
        rows = [{"canonical_id": "BNSS:CH01:S001", "law": "BNSS"}]
        result = check_sections_schema(rows)
        assert result.passed is False
        assert "missing fields" in result.details[0]

    def test_wrong_law(self) -> None:
        rows = [{
            "canonical_id": "X", "law": "BNS", "chapter_no": 1,
            "chapter_title": "X", "section_no": 1, "section_title": "X",
            "source_url": "X", "content_hash": "X", "version": "X",
        }]
        result = check_sections_schema(rows)
        assert result.passed is False
        assert "BNSS" in result.details[0]


class TestCheckCrosswalkNoDuplicates:
    """Tests for check_crosswalk_no_duplicates."""

    def test_no_duplicates(self) -> None:
        rows = [{"bnss_section_no": "1"}, {"bnss_section_no": "2"}]
        result = check_crosswalk_no_duplicates(rows)
        assert result.passed is True

    def test_with_duplicates(self) -> None:
        rows = [{"bnss_section_no": "1"}, {"bnss_section_no": "1"}]
        result = check_crosswalk_no_duplicates(rows)
        assert result.passed is False


class TestCheckCrosswalkSchema:
    """Tests for check_crosswalk_schema."""

    def test_valid(self) -> None:
        rows = [{
            "bnss_section_no": "1",
            "source_url": "https://example.com",
            "content_hash": "abc",
            "version": "bnss@2026-01-01",
        }]
        result = check_crosswalk_schema(rows)
        assert result.passed is True

    def test_empty_section_no(self) -> None:
        rows = [{
            "bnss_section_no": "",
            "source_url": "X",
            "content_hash": "X",
            "version": "X",
        }]
        result = check_crosswalk_schema(rows)
        assert result.passed is False


class TestCheckVersionConsistency:
    """Tests for check_version_consistency."""

    def test_consistent(self) -> None:
        sections = [{"version": "bnss@2026-01-01"}]
        crosswalk = [{"version": "bnss@2026-01-01"}]
        result = check_version_consistency(sections, crosswalk)
        assert result.passed is True

    def test_inconsistent(self) -> None:
        sections = [{"version": "bnss@2026-01-01"}]
        crosswalk = [{"version": "bnss@2026-02-01"}]
        result = check_version_consistency(sections, crosswalk)
        assert result.passed is False
        assert "Multiple versions" in result.message


class TestCLIValidateCommand:
    """Tests for validate CLI command parsing."""

    def test_validate_command_exists(self) -> None:
        from bnss_pipeline.cli import build_parser
        args = build_parser().parse_args(["validate"])
        assert args.cmd == "validate"

    def test_validate_with_as_of(self) -> None:
        from bnss_pipeline.cli import build_parser
        args = build_parser().parse_args(["validate", "--as-of", "2026-02-17"])
        assert args.as_of == "2026-02-17"
