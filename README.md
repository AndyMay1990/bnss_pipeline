# BNSS Pipeline

This project fetches BNSS source pages, caches the raw HTML, and builds structured datasets for downstream use.

**What It Does**
1. Fetches BNSS pages with conditional requests and caching.
2. Stores raw HTML and metadata in `raw_html/` and `manifests/`.
3. Parses the BNSS index and crosswalk table into JSONL datasets in `datasets/`.

**Requirements**
1. Python 3.13 or later.
2. Internet access for the fetch step.

**Install**
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -e .
```

If you use `uv`, this also works:
```bash
uv sync
```

**Quickstart**
```bash
python -m bnss_pipeline.cli all --as-of 2026-02-03
```

**CLI**
```bash
python -m bnss_pipeline.cli fetch --as-of 2026-02-03
python -m bnss_pipeline.cli etl --as-of 2026-02-03
python -m bnss_pipeline.cli all --as-of 2026-02-03
```

If installed as a package, you can also run:
```bash
bnss-pipeline all --as-of 2026-02-03
```

**Outputs**
1. `datasets/bnss_sections_index.jsonl`
2. `datasets/bnss_crosswalk.jsonl`

**Data Flow**
1. `fetch` stores raw HTML in `raw_html/` and fetch manifests in `manifests/`.
2. `etl` reads `manifests/url_cache.json` to find the latest cached HTML and writes JSONL datasets.

**Configuration**
Configuration is driven by environment variables with the `BNSS_` prefix. These map to fields in `bnss_pipeline/config.py`.

Common options:
1. `BNSS_USER_AGENT`
2. `BNSS_ACCEPT_LANGUAGE`
3. `BNSS_MIN_DELAY_SECONDS`
4. `BNSS_TIMEOUT_TOTAL`
5. `BNSS_MAX_ATTEMPTS`
6. `BNSS_BACKOFF_MULTIPLIER`
7. `BNSS_BACKOFF_MIN`
8. `BNSS_BACKOFF_MAX`
9. `BNSS_PROJECT_ROOT`
10. `BNSS_RAW_HTML_DIR`
11. `BNSS_MANIFESTS_DIR`
12. `BNSS_DATASETS_DIR`

**Python API**
```python
from bnss_pipeline import fetch_many, run_etl_bnss, get_settings

s = get_settings()
results = fetch_many([s.cytrain_index_bnss, s.cytrain_section_table_bnss], as_of="2026-02-03")
sections_path, crosswalk_path = run_etl_bnss(as_of="2026-02-03")
```

**Project Layout**
1. `bnss_pipeline/` package source.
2. `raw_html/` cached HTML and metadata.
3. `manifests/` fetch manifests and URL cache.
4. `datasets/` generated JSONL datasets.

**Notes**
1. The ETL step requires a successful fetch run beforehand.
2. If the upstream HTML changes, parsing may fail and should be updated in `bnss_pipeline/etl_bnss.py`.
