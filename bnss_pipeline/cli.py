"""CLI entry point for BNSS Pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import List, Optional

from .config import get_settings
from .etl_bnss import run_etl_bnss
from .ingest_http import fetch_many

logger = logging.getLogger(__name__)


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _resolve_as_of(value: Optional[str]) -> str:
    return value or _today_utc()


def _setup_logging(verbose: bool = False) -> None:
    """Configure structured logging for the pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    p = argparse.ArgumentParser(
        prog="bnss-pipeline",
        description="ETL pipeline for BNSS legal data",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    sub = p.add_subparsers(dest="cmd", required=True)

    fetch = sub.add_parser("fetch", help="Step 1: fetch BNSS source pages")
    fetch.add_argument(
        "--source", choices=["cytrain"], default="cytrain", help="Upstream source preset"
    )
    fetch.add_argument("--as-of", default=None, help="Dataset version date (YYYY-MM-DD)")

    etl = sub.add_parser("etl", help="Step 2: parse cached HTML into datasets")
    etl.add_argument("--as-of", default=None, help="Dataset version date (YYYY-MM-DD)")

    run_all = sub.add_parser("all", help="Fetch then run ETL")
    run_all.add_argument(
        "--source", choices=["cytrain"], default="cytrain", help="Upstream source preset"
    )
    run_all.add_argument("--as-of", default=None, help="Dataset version date (YYYY-MM-DD)")

    return p


def _seed_urls(source: str) -> List[str]:
    """Resolve source preset to list of URLs."""
    s = get_settings()
    if source == "cytrain":
        return [s.cytrain_index_bnss, s.cytrain_section_table_bnss]
    raise ValueError(f"Unknown source: {source}")


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Returns exit code."""
    args = build_parser().parse_args(argv)
    _setup_logging(verbose=args.verbose)

    if args.cmd == "fetch":
        urls = _seed_urls(args.source)
        results = fetch_many(urls, as_of=_resolve_as_of(args.as_of))
        print(json.dumps([r.model_dump(mode="json") for r in results], indent=2))
        return 0

    if args.cmd == "etl":
        sections_path, crosswalk_path = run_etl_bnss(as_of=_resolve_as_of(args.as_of))
        print(json.dumps({"sections": str(sections_path), "crosswalk": str(crosswalk_path)}, indent=2))
        return 0

    if args.cmd == "all":
        urls = _seed_urls(args.source)
        fetch_many(urls, as_of=_resolve_as_of(args.as_of))
        sections_path, crosswalk_path = run_etl_bnss(as_of=_resolve_as_of(args.as_of))
        print(json.dumps({"sections": str(sections_path), "crosswalk": str(crosswalk_path)}, indent=2))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
