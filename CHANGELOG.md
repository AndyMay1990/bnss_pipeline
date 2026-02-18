# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-17

### Added
- `bnss_pipeline/validate.py` — dataset validation module with 8 checks:
  file existence, schema conformance, duplicate detection, gap detection,
  crosswalk schema, crosswalk duplicates, and version consistency
- `bnss-pipeline validate` CLI command
- `validate` step integrated into `bnss-pipeline all` command
- `ValidationResult` and `ValidationReport` data classes
- README.md — complete rewrite with architecture diagram, quick start,
  CLI reference, output format, configuration, and project structure
- CONTRIBUTING.md — dev setup, testing, code standards, PR workflow
- LICENSE (MIT)
- 20+ unit tests for validation module

### Changed
- `bnss-pipeline all` now runs fetch → ETL → validate (was fetch → ETL)
- `__init__.py` exports `run_validation`, `ValidationReport`, `ValidationResult`

## [0.1.1] - 2026-02-17

### Fixed
- Removed BOM (byte order mark) from all Python source files
- `_write_jsonl()` is now atomic via tmp-file rename
- `fetch_many()` no longer crashes on first failure

### Added
- Structured logging via `logging` module
- `--verbose` / `-v` CLI flag for debug logging
- `.env.example` documenting all configuration variables
- `CHANGELOG.md`
- Docstrings on all public functions and classes
- `[tool.ruff]` and `[tool.pytest]` configuration in `pyproject.toml`
- Unit tests (42 tests across 5 files)
- GitHub Actions CI workflow (lint + test)
- Makefile for common dev tasks

### Changed
- Split dev dependencies into `[project.optional-dependencies.dev]`
- Updated `.gitignore` comprehensively

### Removed
- 8 duplicate/unused root-level files
- BOM characters from all source files

## [0.1.0] - 2026-02-17

### Added
- Initial release
- HTTP fetch with conditional GET, caching, and retry
- ETL parser for BNSS section index and CrPC crosswalk
- CLI with `fetch`, `etl`, and `all` commands
- JSONL output datasets
