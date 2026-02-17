# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-02-17

### Fixed
- Removed BOM (byte order mark) from all Python source files — root cause of
  `utf-8-sig` workarounds throughout the codebase
- `_read_json()` now reads `utf-8` instead of `utf-8-sig` (BOM no longer written)
- `_write_jsonl()` is now atomic via tmp-file rename (matches `_write_json_atomic()`)
- `fetch_many()` no longer crashes on first failure — continues and reports errors

### Added
- Structured logging via `logging` module (replaces all `print()` in library code)
- `--verbose` / `-v` CLI flag for debug logging
- `.env.example` documenting all configuration variables
- `CHANGELOG.md`
- Docstrings on all public functions and classes
- `[tool.ruff]` and `[tool.pytest]` configuration in `pyproject.toml`

### Changed
- Split dev dependencies (`black`, `ruff`, `pytest`, `ipykernel`) into
  `[project.optional-dependencies.dev]`
- Updated `.gitignore` to cover `__pycache__/`, `.vscode/`, `.continue/`,
  `.ipynb_checkpoints/`, and project artifact directories
- `RETRY_STATUS` changed from mutable `set` to `frozenset`
- Extracted `_persist_html()` helper to reduce duplication in `fetch_url()`

### Removed
- BOM characters from source files (root cause fix)
- `_plain_url()` helper — no longer needed since URLs are never written
  with markdown wrapping

## [0.1.0] - 2026-02-17

### Added
- Initial release
- HTTP fetch with conditional GET, caching, and retry
- ETL parser for BNSS section index and CrPC crosswalk
- CLI with `fetch`, `etl`, and `all` commands
- JSONL output datasets
