from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import List

from .config import get_settings
from .etl_bnss import run_etl_bnss
from .ingest_http import fetch_many


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _resolve_as_of(value: str | None) -> str:
    return value or _today_utc()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bnss-pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    fetch = sub.add_parser("fetch", help="Step 1: policy-aware ingestion (HTTP fetch)")
    fetch.add_argument(
        "--source",
        choices=["cytrain"],
        default="cytrain",
        help="Upstream source preset",
    )
    fetch.add_argument("--as-of", default=None, help="Dataset version date, e.g. 2026-01-10")

    etl = sub.add_parser("etl", help="Step 2: parse cached HTML into datasets")
    etl.add_argument("--as-of", default=None, help="Dataset version date, e.g. 2026-01-10")

    run_all = sub.add_parser("all", help="Fetch then run ETL")
    run_all.add_argument(
        "--source",
        choices=["cytrain"],
        default="cytrain",
        help="Upstream source preset",
    )
    run_all.add_argument("--as-of", default=None, help="Dataset version date, e.g. 2026-01-10")

    return p


def _seed_urls(source: str) -> List[str]:
    s = get_settings()
    if source == "cytrain":
        return [s.cytrain_index_bnss, s.cytrain_section_table_bnss]
    raise ValueError(f"Unknown source: {source}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

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
