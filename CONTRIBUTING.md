# Contributing to BNSS Pipeline

Thank you for considering contributing to BNSS Pipeline!

## Development Setup

```bash
# 1. Fork and clone
git clone https://github.com/<your-username>/bnss_pipeline.git
cd bnss_pipeline

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# 3. Install with dev dependencies
make dev
# or: pip install -e '.[dev]'

# 4. Copy environment config
cp .env.example .env
```

## Running Tests

```bash
# Run all tests
make test

# Run with verbose output
pytest tests/ -v --tb=long

# Run a specific test file
pytest tests/test_etl_parsers.py -v

# Run a specific test
pytest tests/test_helpers.py::TestRomanToInt::test_valid_numerals -v
```

## Code Standards

### Formatting & Linting

```bash
# Check linting
make lint

# Auto-format
make format
```

We use [Ruff](https://docs.astral.sh/ruff/) for both linting and formatting:
- Line length: 100 characters
- Target: Python 3.13
- Rules: E, F, W, I, N, UP, B, SIM

### Code Principles

1. **Fix root causes, not symptoms** — If you're adding a workaround, dig deeper
2. **Type everything** — All public functions must have type hints
3. **Document the why** — Docstrings explain purpose; comments explain reasoning
4. **Test error paths** — Every `raise` should have a test that triggers it
5. **Fail fast and loud** — Never silently swallow errors

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add support for BNS law parsing
fix: handle empty crosswalk table rows
test: add edge case tests for Roman numeral parser
docs: update README with validate command
chore: remove deprecated config_old.py
refactor: extract _persist_html from fetch_url
```

## Pull Request Process

1. **Create a branch** from `main`: `git checkout -b feature/your-feature`
2. **Write tests first** for new functionality
3. **Run the full check**: `make lint && make test`
4. **Push and create a PR** with a clear description of:
   - What changed
   - Why it changed (root cause if it's a fix)
   - How to test it
5. **One PR per concern** — Don't mix features with refactors

## Project Architecture

```
fetch (ingest_http.py)  →  cache (raw_html/)  →  parse (etl_bnss.py)  →  output (datasets/)
                                                                        →  validate (validate.py)
```

- **`config.py`** — All settings, driven by environment variables
- **`models.py`** — Data models (Pydantic)
- **`ingest_http.py`** — HTTP fetch with conditional GET, caching, retry
- **`etl_bnss.py`** — HTML parsing into structured JSONL
- **`validate.py`** — Dataset integrity checks
- **`cli.py`** — CLI commands wiring everything together

## Reporting Issues

When reporting a bug, include:
1. What you expected to happen
2. What actually happened
3. Steps to reproduce
4. Python version and OS
5. Relevant log output (`bnss-pipeline -v ...`)
