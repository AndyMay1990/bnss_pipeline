"""Dataset validation for BNSS Pipeline.

Checks data integrity after ETL: gaps, duplicates, schema conformance,
and cross-dataset consistency.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import get_settings
from .etl_bnss import BnssSectionIndexRow, CrosswalkRow, _validate_as_of

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check."""

    check_name: str
    passed: bool
    message: str
    details: List[str] = field(default_factory=list)

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.check_name}: {self.message}"


@dataclass
class ValidationReport:
    """Aggregated validation report."""

    results: List[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    def summary(self) -> str:
        lines = [r.summary() for r in self.results]
        lines.append(f"\n{self.passed_count} passed, {self.failed_count} failed")
        return "\n".join(lines)


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Read a JSONL file into a list of dicts."""
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {i} in {path}: {exc}") from exc
    return rows


def check_file_exists(path: Path, name: str) -> ValidationResult:
    """Check that a dataset file exists and is non-empty."""
    if not path.exists():
        return ValidationResult(
            check_name=f"{name}_exists",
            passed=False,
            message=f"{path} does not exist",
        )
    if path.stat().st_size == 0:
        return ValidationResult(
            check_name=f"{name}_exists",
            passed=False,
            message=f"{path} is empty (0 bytes)",
        )
    return ValidationResult(
        check_name=f"{name}_exists",
        passed=True,
        message=f"{path} exists ({path.stat().st_size} bytes)",
    )


def check_sections_no_duplicates(rows: List[Dict[str, Any]]) -> ValidationResult:
    """Check for duplicate section numbers in the index."""
    seen: Dict[int, int] = {}
    duplicates: List[str] = []
    for row in rows:
        sec = row.get("section_no")
        if sec in seen:
            duplicates.append(f"Section {sec} appears at rows {seen[sec]} and {len(seen) + 1}")
        seen[sec] = len(seen) + 1

    if duplicates:
        return ValidationResult(
            check_name="sections_no_duplicates",
            passed=False,
            message=f"{len(duplicates)} duplicate section(s) found",
            details=duplicates[:10],
        )
    return ValidationResult(
        check_name="sections_no_duplicates",
        passed=True,
        message=f"{len(seen)} unique sections, no duplicates",
    )


def check_sections_gaps(rows: List[Dict[str, Any]]) -> ValidationResult:
    """Check for gaps in section numbering."""
    section_nos = sorted({row["section_no"] for row in rows})
    if not section_nos:
        return ValidationResult(
            check_name="sections_gaps",
            passed=False,
            message="No sections found",
        )

    expected = set(range(section_nos[0], section_nos[-1] + 1))
    actual = set(section_nos)
    missing = sorted(expected - actual)

    if missing:
        return ValidationResult(
            check_name="sections_gaps",
            passed=False,
            message=f"{len(missing)} gap(s) in section numbering",
            details=[f"Missing section(s): {missing[:20]}"],
        )
    return ValidationResult(
        check_name="sections_gaps",
        passed=True,
        message=f"Sections {section_nos[0]}-{section_nos[-1]} contiguous, no gaps",
    )


def check_sections_schema(rows: List[Dict[str, Any]]) -> ValidationResult:
    """Validate that all section rows conform to the expected schema."""
    required_fields = {
        "canonical_id", "law", "chapter_no", "chapter_title",
        "section_no", "section_title", "source_url", "content_hash", "version",
    }
    errors: List[str] = []
    for i, row in enumerate(rows, 1):
        missing = required_fields - set(row.keys())
        if missing:
            errors.append(f"Row {i}: missing fields {missing}")
        if row.get("law") != "BNSS":
            errors.append(f"Row {i}: law='{row.get('law')}', expected 'BNSS'")
        if not isinstance(row.get("section_no"), int):
            errors.append(f"Row {i}: section_no is not int")
        if not isinstance(row.get("chapter_no"), int):
            errors.append(f"Row {i}: chapter_no is not int")

    if errors:
        return ValidationResult(
            check_name="sections_schema",
            passed=False,
            message=f"{len(errors)} schema violation(s)",
            details=errors[:10],
        )
    return ValidationResult(
        check_name="sections_schema",
        passed=True,
        message=f"All {len(rows)} rows conform to schema",
    )


def check_crosswalk_no_duplicates(rows: List[Dict[str, Any]]) -> ValidationResult:
    """Check for duplicate BNSS section numbers in the crosswalk."""
    seen: Dict[str, int] = {}
    duplicates: List[str] = []
    for i, row in enumerate(rows, 1):
        key = row.get("bnss_section_no", "")
        if key in seen:
            duplicates.append(f"BNSS section {key} at rows {seen[key]} and {i}")
        seen[key] = i

    if duplicates:
        return ValidationResult(
            check_name="crosswalk_no_duplicates",
            passed=False,
            message=f"{len(duplicates)} duplicate BNSS section(s)",
            details=duplicates[:10],
        )
    return ValidationResult(
        check_name="crosswalk_no_duplicates",
        passed=True,
        message=f"{len(seen)} unique crosswalk entries, no duplicates",
    )


def check_crosswalk_schema(rows: List[Dict[str, Any]]) -> ValidationResult:
    """Validate crosswalk rows conform to expected schema."""
    required_fields = {
        "bnss_section_no", "source_url", "content_hash", "version",
    }
    errors: List[str] = []
    for i, row in enumerate(rows, 1):
        missing = required_fields - set(row.keys())
        if missing:
            errors.append(f"Row {i}: missing fields {missing}")
        if not row.get("bnss_section_no"):
            errors.append(f"Row {i}: bnss_section_no is empty")

    if errors:
        return ValidationResult(
            check_name="crosswalk_schema",
            passed=False,
            message=f"{len(errors)} schema violation(s)",
            details=errors[:10],
        )
    return ValidationResult(
        check_name="crosswalk_schema",
        passed=True,
        message=f"All {len(rows)} rows conform to schema",
    )


def check_version_consistency(
    sections: List[Dict[str, Any]], crosswalk: List[Dict[str, Any]]
) -> ValidationResult:
    """Check that all rows across both datasets share the same version."""
    versions = set()
    for row in sections:
        versions.add(row.get("version"))
    for row in crosswalk:
        versions.add(row.get("version"))

    if len(versions) == 1:
        return ValidationResult(
            check_name="version_consistency",
            passed=True,
            message=f"All rows have version: {versions.pop()}",
        )
    return ValidationResult(
        check_name="version_consistency",
        passed=False,
        message=f"Multiple versions found: {versions}",
        details=[f"Expected 1 version, found {len(versions)}"],
    )


def run_validation(*, as_of: str) -> ValidationReport:
    """Run all validation checks on the latest datasets.

    Args:
        as_of: Dataset version date (YYYY-MM-DD).

    Returns:
        ValidationReport with all check results.
    """
    s = get_settings()
    as_of = _validate_as_of(as_of)

    ds_dir = s.project_root / s.datasets_dir
    sections_path = ds_dir / "bnss_sections_index.jsonl"
    crosswalk_path = ds_dir / "bnss_crosswalk.jsonl"

    report = ValidationReport()

    # File existence
    report.results.append(check_file_exists(sections_path, "sections"))
    report.results.append(check_file_exists(crosswalk_path, "crosswalk"))

    if not sections_path.exists() or not crosswalk_path.exists():
        logger.error("Cannot run full validation — dataset files missing")
        return report

    sections = _read_jsonl(sections_path)
    crosswalk = _read_jsonl(crosswalk_path)

    logger.info("Validating %d sections, %d crosswalk rows", len(sections), len(crosswalk))

    # Section checks
    report.results.append(check_sections_schema(sections))
    report.results.append(check_sections_no_duplicates(sections))
    report.results.append(check_sections_gaps(sections))

    # Crosswalk checks
    report.results.append(check_crosswalk_schema(crosswalk))
    report.results.append(check_crosswalk_no_duplicates(crosswalk))

    # Cross-dataset checks
    report.results.append(check_version_consistency(sections, crosswalk))

    return report
