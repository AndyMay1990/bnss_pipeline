"""Command-line interface for the BNSS pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import List

from .config import get_settings
from .etl_bnss import run_etl_bnss
from .ingest_http import fetch_many

logger = logging.getLogger(__name__)


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _resolve_as_of(value: str | None) -> str:
    return value or _today_utc()


def build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    p = argparse.ArgumentParser(
        prog="bnss-pipeline",
        description="BNSS ETL pipeline: fetch, parse, and build JSONL datasets.",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # --- fetch ---
    fetch = sub.add_parser("fetch", help="Step 1: policy-aware HTTP ingestion")
    fetch.add_argument(
        "--source",
        choices=["cytrain"],
        default="cytrain",
        help="Upstream source preset (default: cytrain)",
    )
    fetch.add_argument(
        "--as-of", default=None, help="Dataset version date, e.g. 2026-01-10"
    )

    # --- etl ---
    etl = sub.add_parser("etl", help="Step 2: parse cached HTML into datasets")
    etl.add_argument(
        "--as-of", default=None, help="Dataset version date, e.g. 2026-01-10"
    )

    # --- all ---
    run_all = sub.add_parser("all", help="Run fetch then ETL")
    run_all.add_argument(
        "--source",
        choices=["cytrain"],
        default="cytrain",
        help="Upstream source preset (default: cytrain)",
    )
    run_all.add_argument(
        "--as-of", default=None, help="Dataset version date, e.g. 2026-01-10"
    )

    return p


def _seed_urls(source: str) -> List[str]:
    """Return the list of URLs for the given source preset."""
    s = get_settings()
    if source == "cytrain":
        return [s.cytrain_index_bnss, s.cytrain_section_table_bnss]
    raise ValueError(f"Unknown source: {source}")


def _configure_logging(verbose: bool) -> None:
    """Set up root logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )


def main(argv: list[str] | None = None) -> int:
    """Entry point for the CLI."""
    args = build_parser().parse_args(argv)
    _configure_logging(args.verbose)

    if args.cmd == "fetch":
        urls = _seed_urls(args.source)
        results = fetch_many(urls, as_of=_resolve_as_of(args.as_of))
        print(json.dumps([r.model_dump(mode="json") for r in results], indent=2))
        return 0

    if args.cmd == "etl":
        sections_path, crosswalk_path = run_etl_bnss(
            as_of=_resolve_as_of(args.as_of)
        )
        print(
            json.dumps(
                {"sections": str(sections_path), "crosswalk": str(crosswalk_path)},
                indent=2,
            )
        )
        return 0

    if args.cmd == "all":
        urls = _seed_urls(args.source)
        results = fetch_many(urls, as_of=_resolve_as_of(args.as_of))

        # Check for fetch errors before proceeding to ETL
        errors = [r for r in results if not r.is_success]
        if errors:
            for r in errors:
                logger.error("Fetch failed for %s: %s", r.source_url, r.error)
            logger.warning(
                "%d/%d URLs failed; attempting ETL with available data",
                len(errors), len(results),
            )

        sections_path, crosswalk_path = run_etl_bnss(
            as_of=_resolve_as_of(args.as_of)
        )
        print(
            json.dumps(
                {"sections": str(sections_path), "crosswalk": str(crosswalk_path)},
                indent=2,
            )
        )
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
