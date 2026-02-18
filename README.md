# BNSS Pipeline

ETL pipeline for extracting, transforming, and loading structured data from the [Bharatiya Nagarik Suraksha Sanhita (BNSS)](https://cytrain.ncrb.gov.in/staticpage/web_pages/IndexBNSS.html) — India's new criminal procedure code that replaced the Code of Criminal Procedure (CrPC) in 2024.

## What It Does

1. **Fetches** BNSS source HTML from the NCRB CyTrain portal with conditional GET caching
2. **Parses** the HTML into structured datasets (section index + BNSS-to-CrPC crosswalk)
3. **Outputs** versioned JSONL files ready for search, RAG, or analysis

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  CyTrain     │────▶│  ingest_http │────▶│  raw_html/   │
│  (NCRB)      │     │  (fetch)     │     │  (cached)    │
└─────────────┘     └──────────────┘     └──────┬───────┘
                                                │
                                         ┌──────▼───────┐
                                         │  etl_bnss    │
                                         │  (parse)     │
                                         └──────┬───────┘
                                                │
                    ┌───────────────────────────┼───────────────────┐
                    │                           │                   │
             ┌──────▼───────┐           ┌───────▼──────┐   ┌───────▼──────┐
             │  sections    │           │  crosswalk   │   │  validate    │
             │  index.jsonl │           │  .jsonl      │   │  (check)     │
             └──────────────┘           └──────────────┘   └──────────────┘
```

## Quick Start

```bash
# Clone and install
git clone https://github.com/AndyMay1990/bnss_pipeline.git
cd bnss_pipeline
pip install -e '.[dev]'

# Copy and edit configuration
cp .env.example .env

# Run the full pipeline
bnss-pipeline all --as-of 2026-02-17

# Or run steps individually
bnss-pipeline fetch --as-of 2026-02-17
bnss-pipeline etl --as-of 2026-02-17
bnss-pipeline validate --as-of 2026-02-17
```

## CLI Commands

| Command | Description |
|---|---|
| `bnss-pipeline fetch` | Download and cache BNSS source HTML pages |
| `bnss-pipeline etl` | Parse cached HTML into JSONL datasets |
| `bnss-pipeline validate` | Validate dataset integrity (gaps, duplicates, schema) |
| `bnss-pipeline all` | Run fetch → ETL → validate in sequence |

All commands support:
- `--as-of YYYY-MM-DD` — Dataset version date (defaults to today)
- `-v` / `--verbose` — Enable debug logging

## Output Datasets

### `bnss_sections_index.jsonl`

All 532 BNSS sections with chapter structure:

```json
{
  "canonical_id": "BNSS:CH01:S001",
  "law": "BNSS",
  "chapter_no": 1,
  "chapter_title": "PRELIMINARY",
  "section_no": 1,
  "section_title": "Short title, commencement and application",
  "source_url": "https://cytrain.ncrb.gov.in/...",
  "content_hash": "a1b2c3...",
  "version": "bnss@2026-02-17"
}
```

### `bnss_crosswalk.jsonl`

Mapping of BNSS sections to old CrPC sections:

```json
{
  "bnss_section_no": "1",
  "bnss_section_title": "Short title",
  "crpc_section_no": "1",
  "crpc_section_title": "Short title",
  "remarks": "No change",
  "source_url": "https://cytrain.ncrb.gov.in/...",
  "content_hash": "d4e5f6...",
  "version": "bnss@2026-02-17"
}
```

## Configuration

All settings use the `BNSS_` prefix and can be set via environment variables or `.env` file.
See [`.env.example`](.env.example) for the full list.

| Variable | Default | Description |
|---|---|---|
| `BNSS_TIMEOUT_TOTAL` | `30.0` | HTTP request timeout (seconds) |
| `BNSS_MAX_ATTEMPTS` | `5` | Max retry attempts for transient failures |
| `BNSS_MIN_DELAY_SECONDS` | `1.0` | Minimum delay between requests |
| `BNSS_RAW_HTML_DIR` | `raw_html` | Directory for cached HTML files |
| `BNSS_DATASETS_DIR` | `datasets` | Directory for output JSONL files |

## Development

```bash
# Install dev dependencies
make dev

# Run tests
make test

# Lint
make lint

# Auto-format
make format
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full development guidelines.

## Project Structure

```
bnss_pipeline/
├── __init__.py          # Public API exports
├── config.py            # Settings via pydantic-settings
├── models.py            # RawDocument data model
├── ingest_http.py       # HTTP fetch with caching and retry
├── etl_bnss.py          # HTML parsing into structured data
├── validate.py          # Dataset validation checks
└── cli.py               # CLI entry point
tests/
├── conftest.py          # Shared test fixtures
├── test_etl_parsers.py  # Parser tests
├── test_helpers.py      # Helper function tests
├── test_models.py       # Model tests
├── test_config.py       # Config tests
├── test_cli.py          # CLI tests
└── test_validate.py     # Validation tests
```

## License

MIT — see [LICENSE](LICENSE) for details.
