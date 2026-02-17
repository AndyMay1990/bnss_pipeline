.PHONY: install dev lint format test all clean

## Install runtime dependencies only
install:
	pip install -e .

## Install with dev dependencies
dev:
	pip install -e '.[dev]'

## Lint with ruff
lint:
	ruff check bnss_pipeline/ tests/

## Auto-format with ruff
format:
	ruff format bnss_pipeline/ tests/

## Run tests
test:
	pytest tests/ -v --tb=short

## Run the full pipeline
all:
	bnss-pipeline all

## Clean generated artifacts
clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache
	rm -rf build/ dist/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
